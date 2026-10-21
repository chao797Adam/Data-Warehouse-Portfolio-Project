{{ config(materialized='table', schema='gold') }}

select
    -- 1. Generate Surrogate Key
    row_number() over (order by c.cst_id) as customer_key,

    -- 2. Business Keys
    c.cst_id as customer_id,
    c.cst_key,

    -- 3. Customer Info
    c.cst_firstname,
    c.cst_lastname,
    c.cst_marital_status,

    -- 4. Merge gender: Trust CRM first, fallback to ERP
    case
        when c.cst_gndr is not null and c.cst_gndr != 'n/a'
        then c.cst_gndr
        else coalesce(e.gen, 'n/a')
    end as gender,

    e.bdate as birthdate,
    l.cntry as country,
    c.dwh_create_date

from {{ ref('crm_cust_info') }} c
left join {{ ref('erp_customers') }} e on c.cst_key = e.cid
left join {{ ref('erp_locations') }} l on c.cst_key = l.cid
