{{ config(materialized='table', schema='gold') }}

select
    -- Fact Metrics
    s.sls_ord_num,
    s.sls_order_dt,
    s.sls_ship_dt,
    s.sls_due_dt,
    s.sls_sales,
    s.sls_quantity,
    s.sls_price,

    -- Surrogate Keys ONLY (No descriptive attributes!)
    c.customer_key,
    p.product_key,

    -- Audit timestamp
    s.dwh_create_date
from {{ ref('crm_sales_details') }} s
left join {{ ref('dim_customers') }} c on s.sls_cust_id = c.customer_id
left join {{ ref('dim_products') }} p on s.sls_prd_key = p.sls_prd_key
;
