-- models/silver/silver_crm_cust_info.sql
with
    source_data as (
        -- Reference the bronze source table
        select * from {{ source('bronze_source', 'bronze_crm_cust') }}
    ),

    -- Step 1: Filter out records with NULL primary keys
    filtered_data as (select * from source_data where cst_id is not null),

    -- Step 2: Rank records by create date to find the latest one for each customer
    deduplicated as (
        select
            *,
            row_number() over (partition by cst_id order by cst_create_date desc) as rn
        from filtered_data
    )

-- Step 3: Final select and data cleaning
select
    cst_id,
    -- Remove leading and trailing spaces
    trim(cst_key) as cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,

    -- Standardize marital status
    case
        when upper(trim(cst_marital_status)) = 'M'
        then 'Married'
        when upper(trim(cst_marital_status)) = 'S'
        then 'Single'
        else 'n/a'
    end as cst_marital_status,

    -- Standardize gender
    case
        when upper(trim(cst_gndr)) = 'M'
        then 'Male'
        when upper(trim(cst_gndr)) = 'F'
        then 'Female'
        else 'n/a'
    end as cst_gndr,

    -- Add audit timestamp
    current_timestamp() as dwh_create_date

from deduplicated
-- Only keep the latest record (rn = 1)
where rn = 1
;
