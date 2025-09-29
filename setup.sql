/***************************************************************************************************       
Asset:        Zero to Snowflake - Setup
Version:      v1     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************/

USE ROLE sysadmin;

-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"sql", "vignette": "setup"}}';

/*--
 • database, schema and warehouse creation
--*/

-- create tb_101 database
CREATE OR REPLACE DATABASE tb_101;

-- create raw_pos schema
CREATE OR REPLACE SCHEMA tb_101.raw_pos;

-- create raw_customer schema
CREATE OR REPLACE SCHEMA tb_101.raw_customer;

-- create harmonized schema
CREATE OR REPLACE SCHEMA tb_101.harmonized;

-- create analytics schema
CREATE OR REPLACE SCHEMA tb_101.analytics;

-- create governance schema
CREATE OR REPLACE SCHEMA tb_101.governance;

-- create raw_support
CREATE OR REPLACE SCHEMA tb_101.raw_support;

-- Create schema for the Semantic Layer
CREATE OR REPLACE SCHEMA tb_101.semantic_layer
COMMENT = 'Schema for the business-friendly semantic layer, optimized for analytical consumption.';

-- create warehouses
CREATE OR REPLACE WAREHOUSE tb_de_wh
    WAREHOUSE_SIZE = 'large' -- Large for initial data load - scaled down to XSmall at end of this scripts
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data engineering warehouse for tasty bytes';

CREATE OR REPLACE WAREHOUSE tb_dev_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'developer warehouse for tasty bytes';

-- create analyst warehouse
CREATE OR REPLACE WAREHOUSE tb_analyst_wh
    COMMENT = 'TastyBytes Analyst Warehouse'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'large'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = true,
    AUTO_RESUME = true;

-- Create a dedicated large warehouse for analytical workloads
CREATE OR REPLACE WAREHOUSE tb_cortex_wh
    WAREHOUSE_SIZE = 'LARGE'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Dedicated large warehouse for Cortex Analyst and other analytical tools.';

-- create roles
USE ROLE securityadmin;

-- functional roles
CREATE ROLE IF NOT EXISTS tb_admin
    COMMENT = 'admin for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tb_data_engineer
    COMMENT = 'data engineer for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tb_dev
    COMMENT = 'developer for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tb_analyst
    COMMENT = 'analyst for tasty bytes';
    
-- role hierarchy
GRANT ROLE tb_admin TO ROLE sysadmin;
GRANT ROLE tb_data_engineer TO ROLE tb_admin;
GRANT ROLE tb_dev TO ROLE tb_data_engineer;
GRANT ROLE tb_analyst TO ROLE tb_data_engineer;

-- privilege grants
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE tb_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE tb_admin;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE tb_101 TO ROLE tb_admin;
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_engineer;
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.raw_support TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.raw_support TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.raw_support TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.governance TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.governance TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.governance TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.semantic_layer TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.semantic_layer TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.semantic_layer TO ROLE tb_dev;

-- warehouse grants
GRANT OWNERSHIP ON WAREHOUSE tb_de_wh TO ROLE tb_admin COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE tb_de_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_de_wh TO ROLE tb_data_engineer;

GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_data_engineer;
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_dev;

GRANT ALL ON WAREHOUSE tb_analyst_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_analyst_wh TO ROLE tb_data_engineer;
GRANT ALL ON WAREHOUSE tb_analyst_wh TO ROLE tb_dev;

GRANT ALL ON WAREHOUSE tb_cortex_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_cortex_wh TO ROLE tb_data_engineer;
GRANT ALL ON WAREHOUSE tb_cortex_wh TO ROLE tb_dev;

-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.governance TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.governance TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.governance TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.semantic_layer TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.semantic_layer TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.semantic_layer TO ROLE tb_dev;

-- Apply Masking Policy Grants
USE ROLE accountadmin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_data_engineer;
  
-- Grants for tb_admin
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE tb_admin;

-- Grants for tb_analyst
GRANT ALL ON SCHEMA harmonized TO ROLE tb_analyst;
GRANT ALL ON SCHEMA analytics TO ROLE tb_analyst;
GRANT OPERATE, USAGE ON WAREHOUSE tb_analyst_wh TO ROLE tb_analyst;

-- Grants for cortex search service
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE TB_DEV;
GRANT USAGE ON SCHEMA TB_101.HARMONIZED TO ROLE TB_DEV;
GRANT USAGE ON WAREHOUSE TB_DE_WH TO ROLE TB_DEV;


-- raw_pos table build
USE ROLE sysadmin;
USE WAREHOUSE tb_de_wh;

/*--
 • file format and stage creation
--*/

CREATE OR REPLACE FILE FORMAT tb_101.public.csv_ff 
type = 'csv';

CREATE OR REPLACE STAGE tb_101.public.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = tb_101.public.csv_ff;

CREATE OR REPLACE STAGE tb_101.public.truck_reviews_s3load
COMMENT = 'Truck Reviews Stage'
url = 's3://sfquickstarts/tastybytes-voc/'
file_format = tb_101.public.csv_ff;

-- This stage will be used to upload your YAML files.
CREATE OR REPLACE STAGE tb_101.semantic_layer.semantic_model_stage
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Internal stage for uploading Cortex Analyst semantic model YAML files.';

/*--
 raw zone table build 
--*/

-- country table build
CREATE OR REPLACE TABLE tb_101.raw_pos.country
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

-- franchise table build
CREATE OR REPLACE TABLE tb_101.raw_pos.franchise 
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216) 
);

-- location table build
CREATE OR REPLACE TABLE tb_101.raw_pos.location
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
);

-- menu table build
CREATE OR REPLACE TABLE tb_101.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- truck table build 
CREATE OR REPLACE TABLE tb_101.raw_pos.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

-- order_header table build
CREATE OR REPLACE TABLE tb_101.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);

-- order_detail table build
CREATE OR REPLACE TABLE tb_101.raw_pos.order_detail 
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
);

-- customer loyalty table build
CREATE OR REPLACE TABLE tb_101.raw_customer.customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

/*--
 raw_suport zone table build 
--*/
CREATE OR REPLACE TABLE tb_101.raw_support.truck_reviews
(
    order_id NUMBER(38,0),
    language VARCHAR(16777216),
    source VARCHAR(16777216),
    review VARCHAR(16777216),
    review_id NUMBER(38,0)  
);

/*--
 • harmonized view creation
--*/

-- orders_v view
CREATE OR REPLACE VIEW tb_101.harmonized.orders_v
    AS
SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM tb_101.raw_pos.order_detail od
JOIN tb_101.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN tb_101.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN tb_101.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
JOIN tb_101.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN tb_101.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN tb_101.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;

-- loyalty_metrics_v view
CREATE OR REPLACE VIEW tb_101.harmonized.customer_loyalty_metrics_v
    AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM tb_101.raw_customer.customer_loyalty cl
JOIN tb_101.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;

-- truck_reviews_v view
  CREATE OR REPLACE VIEW tb_101.harmonized.truck_reviews_v
      AS
  SELECT DISTINCT
      r.review_id,
      r.order_id,
      oh.truck_id,
      r.language,
      source,
      r.review,
      t.primary_city,
      oh.customer_id,
      TO_DATE(oh.order_ts) AS date,
      m.truck_brand_name
  FROM raw_support.truck_reviews r
  JOIN raw_pos.order_header oh
      ON oh.order_id = r.order_id
  JOIN raw_pos.truck t
      ON t.truck_id = oh.truck_id
  JOIN raw_pos.menu m
      ON m.menu_type_id = t.menu_type_id;

/*--
 • analytics view creation
--*/

-- orders_v view
CREATE OR REPLACE VIEW tb_101.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM tb_101.harmonized.orders_v o;

-- customer_loyalty_metrics_v view
CREATE OR REPLACE VIEW tb_101.analytics.customer_loyalty_metrics_v
COMMENT = 'Tasty Bytes Customer Loyalty Member Metrics View'
    AS
SELECT * FROM tb_101.harmonized.customer_loyalty_metrics_v;

-- truck_reviews_v view
CREATE OR REPLACE VIEW tb_101.analytics.truck_reviews_v 
    AS
SELECT * FROM harmonized.truck_reviews_v;
GRANT USAGE ON SCHEMA raw_support to ROLE tb_admin;
GRANT SELECT ON TABLE raw_support.truck_reviews TO ROLE tb_admin;

-- view for streamlit app
CREATE OR REPLACE VIEW tb_101.analytics.japan_menu_item_sales_feb_2022
AS
SELECT
    DISTINCT menu_item_name,
    date,
    order_total
FROM analytics.orders_v
WHERE country = 'Japan'
    AND YEAR(date) = '2022'
    AND MONTH(date) = '2'
GROUP BY ALL
ORDER BY date;

-- Orders view for the Semantic Layer
CREATE OR REPLACE VIEW tb_101.semantic_layer.orders_v
AS
SELECT * FROM (
    SELECT
        order_id::VARCHAR AS order_id,
        truck_id::VARCHAR AS truck_id,
        order_detail_id::VARCHAR AS order_detail_id,
        truck_brand_name,
        menu_type,
        primary_city,
        region,
        country,
        franchise_flag,
        franchise_id::VARCHAR AS franchise_id,
        location_id::VARCHAR AS location_id,
        customer_id::VARCHAR AS customer_id,
        gender,
        marital_status,
        menu_item_id::VARCHAR AS menu_item_id,
        menu_item_name,
        quantity,
        order_total
    FROM tb_101.harmonized.orders_v
)
LIMIT 10000;

-- Customer Loyalty Metrics view for the Semantic Layer
CREATE OR REPLACE VIEW tb_101.semantic_layer.customer_loyalty_metrics_v
AS
SELECT * FROM (
    SELECT
        cl.customer_id::VARCHAR AS customer_id,
        cl.city,
        cl.country,
        SUM(o.order_total) AS total_sales,
        ARRAY_AGG(DISTINCT o.location_id::VARCHAR) WITHIN GROUP (ORDER BY o.location_id::VARCHAR) AS visited_location_ids_array
    FROM tb_101.harmonized.customer_loyalty_metrics_v AS cl
    JOIN tb_101.harmonized.orders_v AS o
        ON cl.customer_id = o.customer_id
    GROUP BY
        cl.customer_id,
        cl.city,
        cl.country
    ORDER BY
        cl.customer_id
)
LIMIT 10000;

/*--
 raw zone table load 
--*/

-- truck_reviews table load
COPY INTO tb_101.raw_support.truck_reviews
FROM @tb_101.public.truck_reviews_s3load/raw_support/truck_reviews/;

-- country table load
COPY INTO tb_101.raw_pos.country
FROM @tb_101.public.s3load/raw_pos/country/;

-- franchise table load
COPY INTO tb_101.raw_pos.franchise
FROM @tb_101.public.s3load/raw_pos/franchise/;

-- location table load
COPY INTO tb_101.raw_pos.location
FROM @tb_101.public.s3load/raw_pos/location/;

-- menu table load
COPY INTO tb_101.raw_pos.menu
FROM @tb_101.public.s3load/raw_pos/menu/;

-- truck table load
COPY INTO tb_101.raw_pos.truck
FROM @tb_101.public.s3load/raw_pos/truck/;

-- customer_loyalty table load
COPY INTO tb_101.raw_customer.customer_loyalty
FROM @tb_101.public.s3load/raw_customer/customer_loyalty/;

-- order_header table load
COPY INTO tb_101.raw_pos.order_header
FROM @tb_101.public.s3load/raw_pos/order_header/;

-- Setup truck details
USE WAREHOUSE tb_de_wh;

-- order_detail table load
COPY INTO tb_101.raw_pos.order_detail
FROM @tb_101.public.s3load/raw_pos/order_detail/;

-- add truck_build column
ALTER TABLE tb_101.raw_pos.truck
ADD COLUMN truck_build OBJECT;

-- construct an object from year, make, model and store on truck_build column
UPDATE tb_101.raw_pos.truck
    SET truck_build = OBJECT_CONSTRUCT(
        'year', year,
        'make', make,
        'model', model
    );

-- Messing up make data in truck_build object
UPDATE tb_101.raw_pos.truck
SET truck_build = OBJECT_INSERT(
    truck_build,
    'make',
    'Ford',
    TRUE
)
WHERE 
    truck_build:make::STRING = 'Ford_'
    AND 
    truck_id % 2 = 0;

-- truck_details table build 
CREATE OR REPLACE TABLE tb_101.raw_pos.truck_details
AS 
SELECT * EXCLUDE (year, make, model)
FROM tb_101.raw_pos.truck;

/*--
 • New Pipeline: Sales Insights (UDFs, Marts, DQ, Tasks)
    Depends on tb_101.harmonized.orders_v as upstream source
--*/

-- Context for object creation
USE ROLE sysadmin;
USE WAREHOUSE tb_de_wh;
USE DATABASE tb_101;

-- Schemas for new artifacts
CREATE OR REPLACE SCHEMA tb_101.analytics_mart;
CREATE OR REPLACE SCHEMA tb_101.governance_logs;

-- UDFs (in governance schema)
USE SCHEMA tb_101.governance;

CREATE OR REPLACE FUNCTION governance.SAFE_DIVIDE(numerator NUMBER, denominator NUMBER)
RETURNS NUMBER
IMMUTABLE
MEMOIZABLE
COMMENT = 'Safely divide numerator by denominator; returns NULL on zero/NULL denominator'
AS $$ CASE WHEN denominator IS NULL OR denominator = 0 THEN NULL ELSE numerator / denominator END $$;

CREATE OR REPLACE FUNCTION governance.ZSCORE(x NUMBER, mean NUMBER, stddev NUMBER)
RETURNS NUMBER
IMMUTABLE
MEMOIZABLE
COMMENT = 'Standard z-score: (x - mean) / stddev; NULL if stddev is 0/NULL'
AS $$ CASE WHEN stddev IS NULL OR stddev = 0 THEN NULL ELSE (x - mean) / stddev END $$;

CREATE OR REPLACE FUNCTION governance.ROBUST_ZSCORE(x NUMBER, median NUMBER, mad NUMBER)
RETURNS NUMBER
IMMUTABLE
MEMOIZABLE
COMMENT = 'Robust z-score using median and MAD (scaled by 1.4826)'
AS $$ CASE WHEN mad IS NULL OR mad = 0 THEN NULL ELSE (x - median) / (1.4826 * mad) END $$;

CREATE OR REPLACE FUNCTION governance.COALESCE_ZERO(x NUMBER)
RETURNS NUMBER
IMMUTABLE
MEMOIZABLE
COMMENT = 'Coalesce NULL numeric to zero'
AS $$ COALESCE(x, 0) $$;

-- Tables for marts, KPIs, anomalies, and DQ logs
USE SCHEMA tb_101.analytics_mart;

CREATE OR REPLACE TABLE tb_101.analytics_mart.fact_daily_item_sales (
    date DATE,
    menu_item_id VARCHAR,
    menu_item_name STRING,
    truck_brand_name STRING,
    country STRING,
    location_id VARCHAR,
    orders_count NUMBER,
    order_lines NUMBER,
    quantity NUMBER,
    gross_sales NUMBER(38,2),
    net_sales NUMBER(38,2),
    last_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT ifk_fact_daily_item_sales UNIQUE (date, menu_item_id, location_id)
);

CREATE OR REPLACE TABLE tb_101.analytics_mart.kpi_daily_brand (
    date DATE,
    truck_brand_name STRING,
    total_orders NUMBER,
    total_order_lines NUMBER,
    total_quantity NUMBER,
    total_sales NUMBER(38,2),
    avg_order_value NUMBER(38,2),
    unique_customers NUMBER,
    last_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT ifk_kpi_daily_brand UNIQUE (date, truck_brand_name)
);

CREATE OR REPLACE TABLE tb_101.analytics_mart.sales_anomalies (
    date DATE,
    grain STRING,
    id STRING,
    metric_name STRING,
    metric_value NUMBER(38,2),
    zscore NUMBER(38,6),
    robust_zscore NUMBER(38,6),
    anomaly_flag BOOLEAN,
    context VARIANT,
    detected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

USE SCHEMA tb_101.governance_logs;

CREATE OR REPLACE TABLE tb_101.governance_logs.dq_results (
    run_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    check_name STRING,
    severity STRING,
    status STRING,
    failed_count NUMBER,
    sample_rows VARIANT,
    notes STRING
);

-- Stored Procedures (SQL) for incremental builds, DQ, and anomalies
USE SCHEMA tb_101.governance;

CREATE OR REPLACE PROCEDURE governance.sp_build_fact_daily_item_sales(days_back INTEGER DEFAULT 7)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
BEGIN
    CREATE OR REPLACE TEMP TABLE src_fact AS
    SELECT
        DATE(o.order_ts) AS date,
        o.menu_item_id::VARCHAR AS menu_item_id,
        o.menu_item_name AS menu_item_name,
        o.truck_brand_name AS truck_brand_name,
        o.country AS country,
        o.location_id::VARCHAR AS location_id,
        COUNT(DISTINCT o.order_id) AS orders_count,
        COUNT(*) AS order_lines,
        SUM(o.quantity) AS quantity,
        SUM(o.price) AS gross_sales,
        SUM(o.price) AS net_sales
    FROM tb_101.harmonized.orders_v o
    WHERE DATE(o.order_ts) >= DATEADD('day', -days_back, CURRENT_DATE())
    GROUP BY
        DATE(o.order_ts), o.menu_item_id::VARCHAR, o.menu_item_name,
        o.truck_brand_name, o.country, o.location_id::VARCHAR;

    DELETE FROM tb_101.analytics_mart.fact_daily_item_sales
    WHERE date >= DATEADD('day', -days_back, CURRENT_DATE());

    INSERT INTO tb_101.analytics_mart.fact_daily_item_sales (
        date, menu_item_id, menu_item_name, truck_brand_name, country, location_id,
        orders_count, order_lines, quantity, gross_sales, net_sales
    )
    SELECT date, menu_item_id, menu_item_name, truck_brand_name, country, location_id,
           orders_count, order_lines, quantity, gross_sales, net_sales
    FROM src_fact;

    RETURN 'fact_daily_item_sales built for last ' || days_back || ' days';
END;
$$;

CREATE OR REPLACE PROCEDURE governance.sp_build_kpi_daily_brand(days_back INTEGER DEFAULT 7)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
BEGIN
    CREATE OR REPLACE TEMP TABLE src_kpi AS
    SELECT
        f.date,
        f.truck_brand_name,
        SUM(f.orders_count) AS total_orders,
        SUM(f.order_lines) AS total_order_lines,
        SUM(f.quantity) AS total_quantity,
        SUM(f.net_sales) AS total_sales,
        governance.SAFE_DIVIDE(SUM(f.net_sales), NULLIF(SUM(f.orders_count), 0)) AS avg_order_value,
        COUNT(DISTINCT o.customer_id) AS unique_customers
    FROM tb_101.analytics_mart.fact_daily_item_sales f
    LEFT JOIN tb_101.harmonized.orders_v o
        ON o.truck_brand_name = f.truck_brand_name
       AND DATE(o.order_ts) = f.date
    WHERE f.date >= DATEADD('day', -days_back, CURRENT_DATE())
    GROUP BY f.date, f.truck_brand_name;

    DELETE FROM tb_101.analytics_mart.kpi_daily_brand
    WHERE date >= DATEADD('day', -days_back, CURRENT_DATE());

    INSERT INTO tb_101.analytics_mart.kpi_daily_brand (
        date, truck_brand_name, total_orders, total_order_lines, total_quantity, total_sales, avg_order_value, unique_customers
    )
    SELECT date, truck_brand_name, total_orders, total_order_lines, total_quantity, total_sales, avg_order_value, unique_customers
    FROM src_kpi;

    RETURN 'kpi_daily_brand built for last ' || days_back || ' days';
END;
$$;

CREATE OR REPLACE PROCEDURE governance.sp_run_dq_checks(days_back INTEGER DEFAULT 7)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
BEGIN
    INSERT INTO tb_101.governance_logs.dq_results (check_name, severity, status, failed_count, sample_rows, notes)
    SELECT 'FACT_NULL_KEYS', 'ERROR', IFF(cnt=0, 'PASS', 'FAIL'), cnt,
           OBJECT_CONSTRUCT('sample_rows', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM (
               SELECT * FROM tb_101.analytics_mart.fact_daily_item_sales
               WHERE date >= DATEADD('day', -days_back, CURRENT_DATE())
                 AND (menu_item_id IS NULL OR location_id IS NULL)
               LIMIT 10
           ))),
           'menu_item_id/location_id should not be NULL'
    FROM (
        SELECT COUNT(*) AS cnt
        FROM tb_101.analytics_mart.fact_daily_item_sales
        WHERE date >= DATEADD('day', -days_back, CURRENT_DATE())
          AND (menu_item_id IS NULL OR location_id IS NULL)
    );

    INSERT INTO tb_101.governance_logs.dq_results (check_name, severity, status, failed_count, sample_rows, notes)
    SELECT 'FACT_NON_NEGATIVE', 'ERROR', IFF(cnt=0, 'PASS', 'FAIL'), cnt,
           NULL, 'quantity, gross_sales, net_sales must be >= 0'
    FROM (
        SELECT COUNT(*) AS cnt
        FROM tb_101.analytics_mart.fact_daily_item_sales
        WHERE date >= DATEADD('day', -days_back, CURRENT_DATE())
          AND (quantity < 0 OR gross_sales < 0 OR net_sales < 0)
    );

    INSERT INTO tb_101.governance_logs.dq_results (check_name, severity, status, failed_count, sample_rows, notes)
    SELECT 'FACT_DUPLICATES', 'ERROR', IFF(cnt=0, 'PASS', 'FAIL'), cnt,
           NULL, 'Duplicate rows at (date, menu_item_id, location_id)'
    FROM (
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT date, menu_item_id, location_id, COUNT(*) c
            FROM tb_101.analytics_mart.fact_daily_item_sales
            WHERE date >= DATEADD('day', -days_back, CURRENT_DATE())
            GROUP BY date, menu_item_id, location_id
            HAVING COUNT(*) > 1
        )
    );

    RETURN 'DQ checks executed for last ' || days_back || ' days';
END;
$$;

CREATE OR REPLACE PROCEDURE governance.sp_detect_sales_anomalies(days_back INTEGER DEFAULT 30)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
BEGIN
    CREATE OR REPLACE TEMP TABLE base AS
    SELECT
        date,
        menu_item_id,
        menu_item_name,
        truck_brand_name,
        country,
        location_id,
        quantity,
        net_sales
    FROM tb_101.analytics_mart.fact_daily_item_sales
    WHERE date >= DATEADD('day', -(days_back + 60), CURRENT_DATE());

    CREATE OR REPLACE TEMP TABLE stats AS
    SELECT
        b.date,
        b.menu_item_id,
        AVG(b2.net_sales) AS mean_sales,
        STDDEV_SAMP(b2.net_sales) AS std_sales,
        MEDIAN(b2.net_sales) AS med_sales,
        AVG(ABS(b2.net_sales - MEDIAN(b2.net_sales))) AS mad_sales,
        AVG(b2.quantity) AS mean_qty,
        STDDEV_SAMP(b2.quantity) AS std_qty,
        MEDIAN(b2.quantity) AS med_qty,
        AVG(ABS(b2.quantity - MEDIAN(b2.quantity))) AS mad_qty
    FROM base b
    JOIN base b2
        ON b2.menu_item_id = b.menu_item_id
       AND b2.date BETWEEN DATEADD('day', -60, b.date) AND DATEADD('day', -1, b.date)
    GROUP BY b.date, b.menu_item_id;

    CREATE OR REPLACE TEMP TABLE scored AS
    SELECT
        b.date,
        'ITEM' AS grain,
        b.menu_item_id AS id,
        'NET_SALES' AS metric_name,
        b.net_sales AS metric_value,
        governance.ZSCORE(b.net_sales, s.mean_sales, s.std_sales) AS zscore,
        governance.ROBUST_ZSCORE(b.net_sales, s.med_sales, s.mad_sales) AS robust_zscore,
        IFF(ABS(COALESCE(governance.ZSCORE(b.net_sales, s.mean_sales, s.std_sales), 0)) >= 3
            OR ABS(COALESCE(governance.ROBUST_ZSCORE(b.net_sales, s.med_sales, s.mad_sales), 0)) >= 3,
            TRUE, FALSE) AS anomaly_flag,
        OBJECT_CONSTRUCT('brand', b.truck_brand_name, 'country', b.country, 'location', b.location_id) AS context
    FROM base b
    LEFT JOIN stats s
        ON s.date = b.date AND s.menu_item_id = b.menu_item_id
    UNION ALL
    SELECT
        b.date,
        'ITEM' AS grain,
        b.menu_item_id AS id,
        'QUANTITY' AS metric_name,
        b.quantity AS metric_value,
        governance.ZSCORE(b.quantity, s.mean_qty, s.std_qty) AS zscore,
        governance.ROBUST_ZSCORE(b.quantity, s.med_qty, s.mad_qty) AS robust_zscore,
        IFF(ABS(COALESCE(governance.ZSCORE(b.quantity, s.mean_qty, s.std_qty), 0)) >= 3
            OR ABS(COALESCE(governance.ROBUST_ZSCORE(b.quantity, s.med_qty, s.mad_qty), 0)) >= 3,
            TRUE, FALSE) AS anomaly_flag,
        OBJECT_CONSTRUCT('brand', b.truck_brand_name, 'country', b.country, 'location', b.location_id) AS context
    FROM base b
    LEFT JOIN stats s
        ON s.date = b.date AND s.menu_item_id = b.menu_item_id;

    DELETE FROM tb_101.analytics_mart.sales_anomalies
    WHERE date >= DATEADD('day', -days_back, CURRENT_DATE());

    INSERT INTO tb_101.analytics_mart.sales_anomalies (date, grain, id, metric_name, metric_value, zscore, robust_zscore, anomaly_flag, context)
    SELECT date, grain, id, metric_name, metric_value, zscore, robust_zscore, anomaly_flag, context
    FROM scored
    WHERE date >= DATEADD('day', -days_back, CURRENT_DATE());

    RETURN 'Anomalies detected for last ' || days_back || ' days';
END;
$$;

CREATE OR REPLACE PROCEDURE governance.sp_run_new_pipeline(days_back INTEGER DEFAULT 7)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
BEGIN
    CALL governance.sp_build_fact_daily_item_sales(days_back);
    CALL governance.sp_build_kpi_daily_brand(days_back);
    CALL governance.sp_run_dq_checks(days_back);
    CALL governance.sp_detect_sales_anomalies(GREATEST(days_back, 30));
    RETURN 'New pipeline completed for last ' || days_back || ' days';
END;
$$;

-- Scheduled Tasks: daily DAG for new pipeline (use existing privileges role pattern)
USE ROLE accountadmin;
USE WAREHOUSE tb_de_wh;
USE DATABASE tb_101;

CREATE OR REPLACE TASK tb_101.governance.t_np_build_fact_daily
  WAREHOUSE = tb_de_wh
  SCHEDULE = 'USING CRON 20 1 * * * UTC'
  COMMENT = 'Build fact_daily_item_sales (new pipeline)'
AS
CALL tb_101.governance.sp_build_fact_daily_item_sales(7);

CREATE OR REPLACE TASK tb_101.governance.t_np_build_kpi
  WAREHOUSE = tb_de_wh
  AFTER tb_101.governance.t_np_build_fact_daily
AS
CALL tb_101.governance.sp_build_kpi_daily_brand(7);

CREATE OR REPLACE TASK tb_101.governance.t_np_dq_checks
  WAREHOUSE = tb_de_wh
  AFTER tb_101.governance.t_np_build_kpi
AS
CALL tb_101.governance.sp_run_dq_checks(7);

CREATE OR REPLACE TASK tb_101.governance.t_np_anomaly
  WAREHOUSE = tb_de_wh
  AFTER tb_101.governance.t_np_dq_checks
AS
CALL tb_101.governance.sp_detect_sales_anomalies(30);

ALTER TASK tb_101.governance.t_np_build_fact_daily RESUME;
ALTER TASK tb_101.governance.t_np_build_kpi RESUME;
ALTER TASK tb_101.governance.t_np_dq_checks RESUME;
ALTER TASK tb_101.governance.t_np_anomaly RESUME;

-- Grants for new schemas using existing roles
USE ROLE securityadmin;

GRANT ALL ON SCHEMA tb_101.analytics_mart TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.analytics_mart TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.analytics_mart TO ROLE tb_dev;
GRANT ALL ON SCHEMA tb_101.analytics_mart TO ROLE tb_analyst;

GRANT ALL ON SCHEMA tb_101.governance_logs TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.governance_logs TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.governance_logs TO ROLE tb_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.analytics_mart TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.analytics_mart TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.analytics_mart TO ROLE tb_dev;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.analytics_mart TO ROLE tb_analyst;

-- restore role context
USE ROLE sysadmin;
