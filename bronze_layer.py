
import polars as pl
import psycopg2
from psycopg2.extras import execute_values
import os
import sys
from pathlib import Path


sys.path.append('/opt/airflow/medallion_project')


def db_connection():
    conn=psycopg2.connect(dbname="Datawarehouse",user="postgres",password="imran",host="192.168.80.101",port="5432")
    curr=conn.cursor()
    return curr,conn

def main():
    curr,conn=db_connection()

    customer_df=pl.read_csv("/opt/airflow/medallion_project/source_crm/cust_info.csv")
    product_df=pl.read_csv("/opt/airflow/medallion_project/source_crm/prd_info.csv")
    sales_df=pl.read_csv("/opt/airflow/medallion_project/source_crm/sales_details.csv")

    erp_cust_df=pl.read_csv("/opt/airflow/medallion_project/source_erp/CUST_AZ12.csv")
    erp_loc_df=pl.read_csv("/opt/airflow/medallion_project/source_erp/LOC_A101.csv")
    erp_cat_df=pl.read_csv("/opt/airflow/medallion_project/source_erp/PX_CAT_G1V2.csv")

   

    # insert Customer information Into Bronze Layer
    customer_data=customer_df.rows()
    curr.execute("truncate table bronze_layer.crm_cust_info")
    conn.commit()
    
    insert_query = f"""
        INSERT INTO bronze_layer.crm_cust_info  (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
        VALUES %s
    """
    execute_values(curr, insert_query, customer_data, page_size=500)

    conn.commit()
    print(f"✅ {len(customer_data)} rows insert ho gaye 'bronze_layer.crm_cust_info ' mein")


    # insert Product information Into Bronze Layer

    product_data=product_df.rows()
    curr.execute("truncate table bronze_layer.crm_prd_info")
    conn.commit()

    insert_query = f"""
        INSERT INTO bronze_layer.crm_prd_info  (prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
        VALUES %s
    """
    execute_values(curr, insert_query, product_data, page_size=500)

    conn.commit()
    print(f"✅ {len(product_data)} rows insert ho gaye 'bronze_layer.crm_prd_info ' mein")


    # insert Sale Detail Into Bronze Layer
    
    sales_data=sales_df.rows()
    curr.execute("truncate table bronze_layer.crm_sales_details")
    conn.commit()

    insert_query = f"""
        INSERT INTO bronze_layer.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
        VALUES %s
    """
    execute_values(curr, insert_query, sales_data, page_size=500)

    conn.commit()
    print(f"✅ {len(sales_data)} rows insert ho gaye 'bronze_layer.crm_sales_details ' mein")


    # Insert ERP Based Customer Details in Bronze Layer
    
    erp_cust_data=erp_cust_df.rows()
    curr.execute("truncate table bronze_layer.erp_cust_az12")
    conn.commit()

    insert_query = f"""
        INSERT INTO bronze_layer.erp_cust_az12 (cid,bdate,gen)
        VALUES %s
    """
    execute_values(curr, insert_query, erp_cust_data, page_size=500)

    conn.commit()
    print(f"✅ {len(erp_cust_data)} rows insert ho gaye 'bronze_layer.erp_cust_az12 ' mein")



    # Insert ERP Based Location Details in Bronze Layer
    
    erp_loc_data=erp_loc_df.rows()
    curr.execute("truncate table bronze_layer.erp_loc_a101")
    conn.commit()

    insert_query = f"""
        INSERT INTO bronze_layer.erp_loc_a101 (cid,cntry)
        VALUES %s
    """
    execute_values(curr, insert_query,erp_loc_data, page_size=500)

    conn.commit()
    print(f"✅ {len(erp_loc_data)} rows insert ho gaye 'bronze_layer.erp_loc_a101 ' mein")


    # Insert ERP Based Category Details in Bronze Layer
    
    erp_cat_data=erp_cat_df.rows()
    curr.execute("truncate table bronze_layer.erp_px_cat_g1v2")
    conn.commit()

    insert_query = f"""
        INSERT INTO bronze_layer.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
        VALUES %s
    """
    execute_values(curr, insert_query,erp_cat_data , page_size=500)

    conn.commit()
    print(f"✅ {len(erp_cat_data)} rows insert ho gaye 'bronze_layer.erp_px_cat_g1v2 ' mein")
    # ── 5. Connection band karo ───────────────────────────────────────────────────
    curr.close()
    conn.close()

    return 0

if __name__=='__main__':

    main()
