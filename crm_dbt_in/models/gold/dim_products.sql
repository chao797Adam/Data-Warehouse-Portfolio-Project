{{ config(materialized='table', schema='gold') }}

select
    -- 1. Generate Surrogate Key
    row_number() over (order by p.prd_id, p.prd_start_dt) as product_key,

    -- 2. Product Info
    p.prd_id,
    p.prd_key,
    p.sls_prd_key,
    p.prd_nm,
    p.prd_cost,
    p.prd_line,
    p.cat_id,
    p.prd_start_dt,
    p.prd_end_dt,
    c.cat as category_name,
    c.subcat as subcategory_name,
    c.maintenance as maintenance_flag,
    p.dwh_create_date as prod_dwh_create_date

from {{ ref('crm_prd_info') }} p
left join {{ ref('erp_product_categories') }} c on p.cat_id = c.id

-- Only keep the current active product info (End Date is NULL)
where p.prd_end_dt is null
