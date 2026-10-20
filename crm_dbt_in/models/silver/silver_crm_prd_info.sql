{{ config(
    materialized='table',
    schema='silver',
    alias='crm_prd_info'
) }}

with
    source_data as (select * from {{ source('bronze_source', 'bronze_crm_prd') }}),

    -- Step 1: Base cleaning, field standardization, and retaining original dates for
    -- auditing purposes.
    step1_base as (
        select
            prd_id,
            trim(prd_key) as prd_key,
            trim(prd_nm) as prd_nm,
            coalesce(prd_cost, 0) as prd_cost,

            -- Standardize product line codes
            case
                upper(trim(prd_line))
                when 'M'
                then 'Mountain'
                when 'R'
                then 'Road'
                when 'S'
                then 'Other Sales'
                when 'T'
                then 'Touring'
                else 'n/a'
            end as prd_line,

            -- Extract category ID and sales table join key
            replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
            substring(trim(prd_key), 7) as sls_prd_key,

            -- Retain original source dates for data audit and tracing
            prd_start_dt as orig_start_dt,
            prd_end_dt as orig_end_dt,

            -- Keep the original Start Date as the baseline
            prd_start_dt as fixed_start_dt,

            current_timestamp() as dwh_create_date
        from source_data
    ),

    -- Step 2: Following the author's method precisely:
    -- Keep Start Date intact, and derive the End Date from the 'NEXT' record's Start
    -- Date - 1 day.
    step2_rebuild_timeline as (
        select
            prd_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            cat_id,
            sls_prd_key,
            orig_start_dt,
            orig_end_dt,
            dwh_create_date,

            -- The start date remains as the original start date
            fixed_start_dt as prd_start_dt,

            -- The end date = Start Date of the 'NEXT' Record - 1 day.
            -- For the latest record (no next record), it evaluates to NULL.
            case
                when
                    lead(fixed_start_dt) over (
                        partition by prd_key order by fixed_start_dt, prd_id
                    )
                    is not null
                then
                    date_sub(
                        lead(fixed_start_dt) over (
                            partition by prd_key order by fixed_start_dt, prd_id
                        ),
                        1
                    )
                else null
            end as prd_end_dt

        from step1_base
    )

select *
from step2_rebuild_timeline
