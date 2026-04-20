create Database Datawarehouse;

create schema bronze_layer;
create schema silver_layer;
create schema gold_layer;


-- Working on Bronze Layer

CREATE TABLE bronze_layer.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);



CREATE TABLE bronze_layer.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);


CREATE TABLE bronze_layer.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);


CREATE TABLE bronze_layer.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);


CREATE TABLE bronze_layer.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);


CREATE TABLE bronze_layer.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);



-- Working on Silver Layer 

CREATE TABLE silver_layer.crm_cust_info (
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver_layer.crm_prd_info (
    prd_id          INT,
    cat_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(50),
    prd_cost        INT,
    prd_line        VARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver_layer.crm_sales_details (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver_layer.erp_loc_a101 (
    cid             VARCHAR(50),
    cntry           VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver_layer.erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver_layer.erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Working Golden Layer

-- =============================================
-- DIMENSION TABLE: dim_products
-- =============================================
CREATE TABLE gold_layer.dim_products (
    product_key     INT          GENERATED ALWAYS AS IDENTITY,
    product_id      INT,
    product_no      INT,
    product_name    VARCHAR(50),
    category_id     VARCHAR(50),
    category        VARCHAR(50),
    sub_category    VARCHAR(50),
    maintenance     VARCHAR(50),
    product_cost    INT,
    product_line    VARCHAR(50),
    start_date      DATE,

    CONSTRAINT pk_dim_products PRIMARY KEY (product_key)
);

-- =============================================
-- DIMENSION TABLE: dim_customer
-- =============================================
CREATE TABLE gold_layer.dim_customer (
    customer_key    INT          GENERATED ALWAYS AS IDENTITY,
    customer_id     INT,
    customer_no     VARCHAR(50),
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    country         VARCHAR(50),
    marital_status  VARCHAR(50),
    gender          VARCHAR(50),
    birth_date      DATE,
    created_date    TIMESTAMP,

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
);

-- =============================================
-- FACT TABLE: fact_sales
-- =============================================
CREATE TABLE gold_layer.fact_sales (
    order_no        VARCHAR(50)  NOT NULL,
    product_key     INT,
    customer_key    INT,
    order_date      DATE,
    shipping_date   DATE,
    due_date        DATE,
    sales_amount    INT,
    quantity        INT,
    price           INT,

    CONSTRAINT pk_fact_sales  PRIMARY KEY (order_no),
    CONSTRAINT fk_product     FOREIGN KEY (product_key)  REFERENCES gold_layer.dim_products (product_key),
    CONSTRAINT fk_customer    FOREIGN KEY (customer_key) REFERENCES gold_layer.dim_customer (customer_key)
);