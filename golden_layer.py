import polars as pl
import psycopg2
from psycopg2.extras import execute_values
import sys

sys.path.append('/opt/airflow/medallion_project')

def db_connection():
    conn=psycopg2.connect(dbname="Datawarehouse",user="postgres",password="imran",host="192.168.80.121",port="5432")
    curr=conn.cursor()
    return curr,conn

def get_data_from_silver_layer(table_name:str):
    
    uri = "postgresql://postgres:imran@localhost:5432/Datawarehouse"
    query = f"""SELECT * FROM silver_layer.{table_name}"""

    df = pl.read_database_uri(query=query, uri=uri, engine="connectorx")
    return df

def get_data_from_golden_layer(table_name:str):
    
    uri = "postgresql://postgres:imran@localhost:5432/Datawarehouse"
    query = f"""SELECT * FROM gold_layer.{table_name}"""

    df = pl.read_database_uri(query=query, uri=uri, engine="connectorx")
    return df


def dim_customer():
    crm_cust_df=get_data_from_silver_layer('crm_cust_info')
    erp_cust_df=get_data_from_silver_layer('erp_cust_az12')
    erp_loc_df=get_data_from_silver_layer('erp_loc_a101')

    dim_customer_df=crm_cust_df.join(erp_cust_df,left_on='cst_key',right_on='cid',how='left',suffix='_erp_cust').join(erp_loc_df,left_on='cst_key',right_on='cid',how='left',suffix='_erp_loc')
    dim_customer_df=dim_customer_df.select(['cst_id', 'cst_key', 'cst_firstname', 'cst_lastname', 'cntry','cst_marital_status', 'cst_gndr','bdate','cst_create_date'])

    dim_customer_df=dim_customer_df.fill_null("n/a")
    return dim_customer_df


def dim_product():
    crm_prd_df=get_data_from_silver_layer('crm_prd_info')
    erp_prd_df=get_data_from_silver_layer('erp_px_cat_g1v2')
    
    dim_product_df=crm_prd_df.join(erp_prd_df,left_on='cat_id',right_on='id',how='left')
    dim_product_df=dim_product_df.filter(pl.col('prd_end_dt').is_null())
    dim_product_df=dim_product_df.select(['prd_id','prd_key','prd_nm','cat_id','cat','subcat','maintenance','prd_cost', 'prd_line', 'prd_start_dt'])
    

    return dim_product_df


def fact_sales():

    dim_prd=get_data_from_golden_layer('dim_products')
    dim_cust=get_data_from_golden_layer('dim_customer')
    crm_sales_df=get_data_from_silver_layer('crm_sales_details')
    

    fact_sales=crm_sales_df\
                    .join(dim_prd,left_on='sls_prd_key',right_on='product_no',how='left')\
                    .join(dim_cust,left_on='sls_cust_id',right_on='customer_id',how='left')
    
    fact_sales=fact_sales.select(['sls_ord_num','product_id', 'sls_cust_id', 'sls_order_dt','sls_ship_dt', 'sls_due_dt', 'sls_sales', 'sls_quantity', 'sls_price'])

    return fact_sales

def main():

    # insert Dimension Tables

    cust_df=dim_customer()
    prd_df=dim_product()

    curr,conn=db_connection()

    
    customer_dim=cust_df.rows()    
    insert_query = f"""
                INSERT INTO gold_layer.dim_customer  
                (customer_id,customer_no,first_name,last_name,country,marital_status,gender,birth_date,created_date)
        VALUES %s
    """
    execute_values(curr, insert_query, customer_dim, page_size=500)
    print(f"✅ {len(customer_dim)} rows insert ho gaye 'gold_layer.dim_customer' mein")


    product_dim=prd_df.rows()    
    insert_query = f"""
                INSERT INTO gold_layer.dim_products
                (product_id,product_no,product_name,category_id,category,sub_category,maintenance,product_cost,product_line,start_date)
        VALUES %s
    """
    execute_values(curr, insert_query, product_dim, page_size=500)
    print(f"✅ {len(product_dim)} rows insert ho gaye 'gold_layer.dim_products' mein")

    conn.commit()

    # INSERT FACT SALES 

    curr,conn=db_connection()
    
    sales_df=fact_sales()
    sales_data=sales_df.rows()    
    insert_query = f"""
                INSERT INTO gold_layer.fact_sales
                (order_no ,product_key ,customer_key ,order_date ,shipping_date ,due_date ,sales_amount,quantity,price)
        VALUES %s
    """
    execute_values(curr, insert_query,sales_data, page_size=500)
    print(f"✅ {len(sales_data)} rows insert ho gaye 'gold_layer.fact_sales' mein")

    conn.commit()

    return 0

if __name__=='__main__':
    main()