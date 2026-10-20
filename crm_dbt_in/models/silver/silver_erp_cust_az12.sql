{{ config(
    materialized='table', 
    schema='silver',
    alias='erp_customers' 
) }}

with
    source_data as (select * from {{ source('bronze_source', 'bronze_erp_cust_az12') }})

select
    -- Remove 'NAS' prefix
    case when cid like 'NAS%' then substring(cid, 4) else cid end as cid,

    case
        when bdate < '1924-01-01' or bdate > current_date() then null else bdate
    end as bdate,

    -- Standardize gender
    case
        when upper(trim(gen)) in ('M', 'MALE')
        then 'Male'
        when upper(trim(gen)) in ('F', 'FEMALE')
        then 'Female'
        else 'n/a'
    end as gen,

    current_timestamp() as dwh_create_date

from source_data
;
