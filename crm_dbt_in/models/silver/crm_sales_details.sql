{{ config(
    materialized='table',
    schema='silver',
    alias='crm_sales_details'
) }}

with
    source_data as (
        select * from {{ source('bronze_source', 'bronze_crm_sales_details') }}
    ),

    -- Step 1: Base filtering (Remove invalid order numbers)
    filtered_data as (select * from source_data where sls_ord_num is not null),

    -- Step 2: Data cleaning, type casting, and business logic correction
    cleaned_data as (
        select
            trim(sls_ord_num) as sls_ord_num,
            trim(sls_prd_key) as sls_prd_key,
            sls_cust_id,

            -- Clean dates
            case
                when sls_order_dt = 0 or length(cast(sls_order_dt as string)) != 8
                then null
                else to_date(cast(sls_order_dt as string), 'yyyyMMdd')
            end as sls_order_dt,

            case
                when sls_ship_dt = 0 or length(cast(sls_ship_dt as string)) != 8
                then null
                else to_date(cast(sls_ship_dt as string), 'yyyyMMdd')
            end as sls_ship_dt,

            case
                when sls_due_dt = 0 or length(cast(sls_due_dt as string)) != 8
                then null
                else to_date(cast(sls_due_dt as string), 'yyyyMMdd')
            end as sls_due_dt,

            -- Fix invalid sales amounts
            case
                when
                    sls_sales is null
                    or sls_sales <= 0
                    or sls_sales != sls_quantity * abs(sls_price)
                then sls_quantity * abs(sls_price)
                else sls_sales
            end as sls_sales,

            sls_quantity,

            -- Fix invalid prices
            case
                when sls_price is null or sls_price <= 0
                then sls_sales / nullif(sls_quantity, 0)
                else sls_price
            end as sls_price,

            -- Audit timestamp
            current_timestamp() as dwh_create_date

        from filtered_data
    )

select *
from cleaned_data
;
