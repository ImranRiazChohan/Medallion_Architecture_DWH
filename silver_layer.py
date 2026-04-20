"""
===============================================================================
ETL Script: Load Silver Layer (Bronze -> Silver)
===============================================================================
Replicates the SQL stored procedure logic using:
  - polars  : transformations / cleansing
  - psycopg2: reading from and writing to PostgreSQL
===============================================================================
"""

import time
import polars as pl
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from datetime import datetime, date


@contextmanager
def get_conn():
    conn = psycopg2.connect(dbname="Datawarehouse",user="postgres",password="imran",host="localhost",port="5432")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_table(conn, table: str) -> pl.DataFrame:
    """Read a full Bronze table into a Polars DataFrame."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pl.DataFrame(rows, schema=cols, orient="row")


def truncate_and_load(conn, table: str, df: pl.DataFrame) -> None:
    """Truncate a Silver table then bulk-insert rows from a DataFrame."""
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table}")
        if df.is_empty():
            return
        cols   = df.columns
        values = [tuple(r) for r in df.rows()]
        sql    = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
        execute_values(cur, sql, values)


def timer(label: str):
    """Simple context manager that prints load duration."""
    class _T:
        def __enter__(self_):
            self_._t = time.time()
            print(f">> Truncating / Inserting: {label}")
            return self_
        def __exit__(self_, *_):
            dur = time.time() - self_._t
            print(f">> Load Duration: {dur:.1f}s")
            print(">> " + "-" * 45)
    return _T()


def safe_int_to_date(s: pl.Series) -> pl.Series:
    """
    Convert integer columns stored as YYYYMMDD → Date.
    Values that are 0 or not exactly 8 digits become null.
    """
    return (
        s.cast(pl.Utf8)
         .map_elements(
             lambda v: (
                 date(int(v[:4]), int(v[4:6]), int(v[6:8]))
                 if v and len(v) == 8 and v != "0"
                 else None
             ),
             return_dtype=pl.Date,
         )
    )


# ── Table loaders ─────────────────────────────────────────────────────────────

def load_crm_cust_info(conn) -> None:
    with timer("silver_layer.crm_cust_info"):
        df = read_table(conn, "bronze_layer.crm_cust_info")

        # Keep most-recent record per customer (replicate ROW_NUMBER logic)
        df = (
            df.filter(pl.col("cst_id").is_not_null())
              .sort("cst_create_date", descending=True)
              .unique(subset=["cst_id"], keep="first")
        )

        df = df.with_columns([
            pl.col("cst_firstname").str.strip_chars(),
            pl.col("cst_lastname").str.strip_chars(),

            pl.col("cst_marital_status")
              .str.strip_chars().str.to_uppercase()
              .map_elements(
                  lambda v: "Single" if v == "S" else "Married" if v == "M" else "n/a",
                  return_dtype=pl.Utf8,
              ),

            pl.col("cst_gndr")
              .str.strip_chars().str.to_uppercase()
              .map_elements(
                  lambda v: "Female" if v == "F" else "Male" if v == "M" else "n/a",
                  return_dtype=pl.Utf8,
              ),
        ])

        cols = ["cst_id","cst_key","cst_firstname","cst_lastname",
                "cst_marital_status","cst_gndr","cst_create_date"]
        truncate_and_load(conn, "silver_layer.crm_cust_info", df.select(cols))


def load_crm_prd_info(conn) -> None:
    with timer("silver_layer.crm_prd_info"):
        df = read_table(conn, "bronze_layer.crm_prd_info")

        df = df.with_columns([
            # cat_id: first 5 chars of prd_key, replace '-' with '_'
            pl.col("prd_key").str.slice(0, 5).str.replace("-", "_").alias("cat_id"),
            # prd_key: remainder after first 6 chars
            pl.col("prd_key").str.slice(6).alias("prd_key"),
            pl.col("prd_cost").fill_null(0),
            pl.col("prd_line")
              .str.strip_chars().str.to_uppercase()
              .map_elements(
                  lambda v: {
                      "M": "Mountain", "R": "Road",
                      "S": "Other Sales", "T": "Touring",
                  }.get(v, "n/a"),
                  return_dtype=pl.Utf8,
              ),
            pl.col("prd_start_dt").cast(pl.Date),
        ])

        # prd_end_dt = LEAD(prd_start_dt) - 1 day, partitioned by prd_key
        df = df.sort(["prd_key", "prd_start_dt"])
        df = df.with_columns(
            pl.col("prd_start_dt")
              .shift(-1)
              .over("prd_key")
              .alias("_next_start")
        ).with_columns(
            (pl.col("_next_start") - pl.duration(days=1)).alias("prd_end_dt")
        ).drop("_next_start")

        cols = ["prd_id","cat_id","prd_key","prd_nm",
                "prd_cost","prd_line","prd_start_dt","prd_end_dt"]
        truncate_and_load(conn, "silver_layer.crm_prd_info", df.select(cols))


def load_crm_sales_details(conn) -> None:
    with timer("silver_layer.crm_sales_details"):
        df = read_table(conn, "bronze_layer.crm_sales_details")

        for col in ("sls_order_dt", "sls_ship_dt", "sls_due_dt"):
            df = df.with_columns(safe_int_to_date(df[col]).alias(col))

        df = df.with_columns([
            # sls_price: derive from sales / qty when invalid
            pl.when(pl.col("sls_price").is_null() | (pl.col("sls_price") <= 0))
              .then(pl.col("sls_sales") / pl.col("sls_quantity").replace(0, None))
              .otherwise(pl.col("sls_price"))
              .alias("sls_price"),
        ]).with_columns([
            # sls_sales: recalculate when missing or inconsistent
            pl.when(
                pl.col("sls_sales").is_null()
                | (pl.col("sls_sales") <= 0)
                | (pl.col("sls_sales") != pl.col("sls_quantity") * pl.col("sls_price").abs())
            )
            .then(pl.col("sls_quantity") * pl.col("sls_price").abs())
            .otherwise(pl.col("sls_sales"))
            .alias("sls_sales"),
        ])

        cols = ["sls_ord_num","sls_prd_key","sls_cust_id","sls_order_dt",
                "sls_ship_dt","sls_due_dt","sls_sales","sls_quantity","sls_price"]
        truncate_and_load(conn, "silver_layer.crm_sales_details", df.select(cols))


def load_erp_cust_az12(conn) -> None:
    with timer("silver_layer.erp_cust_az12"):
        df = read_table(conn, "bronze_layer.erp_cust_az12")

        today = datetime.today().date()
        df = df.with_columns([
            # Strip 'NAS' prefix from cid
            pl.when(pl.col("cid").str.starts_with("NAS"))
              .then(pl.col("cid").str.slice(3))
              .otherwise(pl.col("cid"))
              .alias("cid"),

            # Nullify future birthdates
            pl.when(pl.col("bdate").cast(pl.Date) > today)
              .then(None)
              .otherwise(pl.col("bdate"))
              .alias("bdate"),

            # Normalise gender
            pl.col("gen")
              .str.strip_chars().str.to_uppercase()
              .map_elements(
                  lambda v: "Female" if v in ("F", "FEMALE")
                       else "Male"   if v in ("M", "MALE")
                       else "n/a",
                  return_dtype=pl.Utf8,
              ),
        ])

        truncate_and_load(conn, "silver_layer.erp_cust_az12", df.select(["cid","bdate","gen"]))


def load_erp_loc_a101(conn) -> None:
    with timer("silver_layer.erp_loc_a101"):
        df = read_table(conn, "bronze_layer.erp_loc_a101")

        df = df.with_columns([
            pl.col("cid").str.replace_all("-", "").alias("cid"),
            pl.col("cntry")
              .str.strip_chars()
              .map_elements(
                  lambda v: "Germany"       if v == "DE"
                       else "United States" if v in ("US", "USA")
                       else "n/a"           if not v
                       else v,
                  return_dtype=pl.Utf8,
              ),
        ])

        truncate_and_load(conn, "silver_layer.erp_loc_a101", df.select(["cid","cntry"]))


def load_erp_px_cat_g1v2(conn) -> None:
    with timer("silver_layer.erp_px_cat_g1v2"):
        df = read_table(conn, "bronze_layer.erp_px_cat_g1v2")
        # No transformation needed — direct copy
        truncate_and_load(conn, "silver_layer.erp_px_cat_g1v2",
                          df.select(["id","cat","subcat","maintenance"]))


# ── Orchestrator ──────────────────────────────────────────────────────────────

def load_silver() -> None:
    batch_start = time.time()
    print("=" * 50)
    print("Loading Silver Layer")
    print("=" * 50)

    try:
        with get_conn() as conn:
            print("-" * 50)
            print("Loading CRM Tables")
            print("-" * 50)
            load_crm_cust_info(conn)
            load_crm_prd_info(conn)
            load_crm_sales_details(conn)
            load_erp_cust_az12(conn)

            print("-" * 50)
            print("Loading ERP Tables")
            print("-" * 50)
            load_erp_loc_a101(conn)
            load_erp_px_cat_g1v2(conn)

        total = time.time() - batch_start
        print("=" * 50)
        print("Loading Silver Layer is Completed")
        print(f"   - Total Load Duration: {total:.1f}s")
        print("=" * 50)

    except Exception as e:
        print("=" * 50)
        print("ERROR OCCURRED DURING LOADING SILVER LAYER")
        print(f"Error: {e}")
        print("=" * 50)
        raise


if __name__ == "__main__":
    load_silver()