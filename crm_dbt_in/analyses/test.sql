select * from {{ source('bronze_source', 'bronze_erp_CUST_AZ12') }}
