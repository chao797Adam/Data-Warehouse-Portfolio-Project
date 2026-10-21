{{ config(materialized='table', schema='gold') }}

select
    -- 1. Fact Metrics
    s.sls_ord_num,
    s.sls_order_dt,
    s.sls_sales,
    s.sls_quantity,
    s.sls_price,

    -- 2. Surrogate Keys (The standard Star Schema way)
    c.customer_key,
    p.product_key,

    -- 3. Descriptive columns (For easy BI querying without joins)
    c.cst_firstname,
    c.cst_lastname,
    c.country,
    p.prd_nm,
    p.category_name,
    p.subcategory_name,

    -- 4. Audit timestamp
    s.dwh_create_date

from {{ ref('crm_sales_details') }} s
-- Join using the Business Key
left join {{ ref('dim_customers') }} c on s.sls_cust_id = c.customer_id
left join {{ ref('dim_products') }} p on s.sls_prd_key = p.sls_prd_key
