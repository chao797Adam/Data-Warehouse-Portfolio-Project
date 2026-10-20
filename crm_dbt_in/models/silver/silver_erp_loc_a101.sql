{{ config(
    materialized='table', 
    schema='silver',
    alias='erp_locations' 
) }}

with source_data as (select * from {{ source('bronze_source', 'bronze_erp_loc_a101') }})

select
    -- 1. Remove '-' from CID to match CRM cst_key
    replace(trim(cid), '-', '') as cid,

    -- 2. Standardize country names
    case
        -- Handle United States variants
        when upper(trim(cntry)) in ('US', 'USA', 'UNITED STATES')
        then 'United States'

        -- Handle Germany variants
        when upper(trim(cntry)) in ('DE', 'GERMANY')
        then 'Germany'

        -- Keep standard full names
        when upper(trim(cntry)) = 'AUSTRALIA'
        then 'Australia'
        when upper(trim(cntry)) = 'CANADA'
        then 'Canada'
        when upper(trim(cntry)) = 'UNITED KINGDOM'
        then 'United Kingdom'
        when upper(trim(cntry)) = 'FRANCE'
        then 'France'

        -- Handle empty strings or NULLs
        when trim(cntry) = '' or cntry is null
        then 'n/a'

        -- Fallback for unknown codes
        else upper(trim(cntry))
    end as cntry,

    current_timestamp() as dwh_create_date

from source_data
;
