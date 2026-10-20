{{ config(
    materialized='table', 
    schema='silver',
    alias='erp_product_categories' 
) }}

with
    source_data as (
        select * from {{ source('bronze_source', 'bronze_erp_px_cat_g1v2') }}
    )

select
    -- Remove leading/trailing spaces from category ID (used for joining)
    trim(id) as id,
    trim(cat) as cat,
    trim(subcat) as subcat,
    trim(maintenance) as maintenance,

    -- Audit timestamp
    current_timestamp() as dwh_create_date

from source_data
;
