{{ config(materialized='table', schema='gold') }}

select
    -- Fact Metrics & Dates
    s.sls_ord_num,
    s.sls_order_dt,
    s.sls_ship_dt,  -- Added shipping date
    s.sls_due_dt,  -- Added due date
    s.sls_sales,
    s.sls_quantity,
    s.sls_price,

    -- Surrogate Keys from Dimensions
    c.customer_key,
    p.product_key,

    -- Descriptive columns (for easy BI querying)
    c.cst_firstname,
    c.cst_lastname,
    c.country,
    p.prd_nm,
    p.category_name,
    p.subcategory_name,

    -- Audit timestamp
    s.dwh_create_date as fact_dwh_create_date
from {{ ref('crm_sales_details') }} s
left join {{ ref('dim_customers') }} c on s.sls_cust_id = c.customer_id
left join {{ ref('dim_products') }} p on s.sls_prd_key = p.sls_prd_key
;
