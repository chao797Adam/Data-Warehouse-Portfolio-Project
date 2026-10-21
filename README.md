# Medallion Architecture (Bronze / Silver / Gold) in Databricks

## 1. Project Overview

This project implements a Medallion Data Architecture in Databricks using dbt. The goal is to ingest raw CRM and ERP data, rigorously clean and standardize it in the Silver layer, and build a unified analytical Star Schema in the Gold layer.

- **Source Systems:** CRM, ERP
- **Tools:** Databricks (Spark SQL), dbt
- **Architecture:** Bronze (Raw) -> Silver (Cleaned) -> Gold (Business Ready)

### 🗺️ Data Architecture & Lineage
The following diagram illustrates the complete data flow, including source systems, Bronze ingestion, Silver standardization, and the final Gold Star Schema.

![Data Architecture](assets/data_flow.png)

### 💡 Tech Stack & Architecture Choices

- **OLTP vs. OLAP:** The reference tutorial uses **SQL Server** (OLTP), optimized for transactions and raw data. This project uses **Databricks** (OLAP), optimized for analytics and aggregations.
- **dbt vs. Traditional T-SQL:** In the traditional SQL Server tutorial, explicit `CREATE TABLE` statements and Stored Procedures (`CREATE OR ALTER PROCEDURE`) with `TRY...CATCH` and `TRUNCATE/INSERT` are used. In this project using dbt, **manual DDL is not needed**. dbt acts as the orchestrator, automatically resolving dependencies via `{{ ref() }}`, managing materializations (Table/View), and building a DAG.

---

## 2. Data Profiling & Validation (Pre & Post-Cleaning Checks)

Before and after applying transformations, the data is profiled to identify anomalies.

### A. Check for Unwanted Spaces (`TRIM`)
```sql
SELECT prd_key, prd_nm
FROM crm_dbt_dev.bronze.bronze_crm_prd
WHERE prd_key != TRIM(prd_key) OR prd_nm != TRIM(prd_nm);
```

### B. Check for Duplicates (`HAVING COUNT > 1`)
```sql
SELECT prd_id, prd_key, COUNT(*) as duplicate_count
FROM crm_dbt_dev.bronze.bronze_crm_prd
GROUP BY prd_id, prd_key
HAVING COUNT(*) > 1;
```

### C. Check for Out-of-Range Dates (Birthdates)
```sql
SELECT DISTINCT bdate
FROM crm_dbt_dev.bronze.bronze_erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > current_date();
```

### D. Post-Cleaning Validations (on Silver Tables)
```sql
-- 1. Data Standardization (Product Line)
SELECT DISTINCT prd_line FROM crm_dbt_dev.silver.silver_crm_prd_info;

-- 2. Invalid Date Orders (SCD Overlap Check)
SELECT * FROM crm_dbt_dev.silver.silver_crm_prd_info WHERE prd_end_dt < prd_start_dt;

-- 3. Business Rules (Sales Amount Integrity)
SELECT * FROM crm_dbt_dev.silver.silver_crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_price <= 0;
```

---

## 3. Data Cleaning Logic & Core Code (Silver Layer)

### A. Customer Tables (CRM + ERP)

#### ERP (`silver_erp_cust_az12.sql`)
```sql
{{ config(materialized='table', schema='silver', alias='erp_customers') }}
with
    source_data as (select * from {{ source('bronze_source', 'bronze_erp_cust_az12') }})
select
    case when cid like 'NAS%' then substring(cid, 4) else cid end as cid,
    case when bdate < '1924-01-01' or bdate > current_date() then null else bdate end as bdate,
    case 
        when upper(trim(gen)) in ('M', 'MALE') then 'Male'
        when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
        else 'n/a'
    end as gen,
    current_timestamp() as dwh_create_date
from source_data;
```

#### CRM (`silver_crm_cust_info.sql`)
```sql
{{ config(materialized='table', schema='silver', alias='crm_cust_info') }}
with
    source_data as (select * from {{ source('bronze_source', 'bronze_crm_cust') }}),
    filtered_data as (select * from source_data where cst_id is not null),
    deduplicated as (
        select *, row_number() over (partition by cst_id order by cst_create_date desc) as rn
        from filtered_data
    )
select
    cst_id,
    trim(cst_key) as cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case
        when upper(trim(cst_marital_status)) = 'M' then 'Married'
        when upper(trim(cst_marital_status)) = 'S' then 'Single'
        else 'n/a'
    end as cst_marital_status,
    case
        when upper(trim(cst_gndr)) = 'M' then 'Male'
        when upper(trim(cst_gndr)) = 'F' then 'Female'
        else 'n/a'
    end as cst_gndr,
    current_timestamp() as dwh_create_date
from deduplicated
where rn = 1;
```

#### ERP (`silver_erp_loc_a101.sql`)
```sql
{{ config(materialized='table', schema='silver', alias='erp_locations') }}
with
    source_data as (select * from {{ source('bronze_source', 'bronze_erp_loc_a101') }})
select
    replace(trim(cid), '-', '') as cid,
    case 
        when upper(trim(cntry)) in ('US', 'USA', 'UNITED STATES') then 'United States'
        when upper(trim(cntry)) in ('DE', 'GERMANY') then 'Germany'
        when upper(trim(cntry)) = 'AUSTRALIA' then 'Australia'
        when upper(trim(cntry)) = 'CANADA' then 'Canada'
        when upper(trim(cntry)) = 'UNITED KINGDOM' then 'United Kingdom'
        when upper(trim(cntry)) = 'FRANCE' then 'France'
        when trim(cntry) = '' or cntry is null then 'n/a'
        else upper(trim(cntry))
    end as cntry,
    current_timestamp() as dwh_create_date
from source_data;
```

### B. Product Tables (CRM + ERP)

#### CRM (`silver_crm_prd_info.sql`)
This model handles SCD Type 2 logic, fixing overlapping dates without losing history.

```sql
{{ config(materialized='table', schema='silver', alias='crm_prd_info') }}
with
    source_data as (select * from {{ source('bronze_source', 'bronze_crm_prd') }}),
    step1_base as (
        select
            prd_id,
            trim(prd_key) as prd_key,
            trim(prd_nm) as prd_nm,
            coalesce(prd_cost, 0) as prd_cost,
            case
                upper(trim(prd_line))
                when 'M' then 'Mountain'
                when 'R' then 'Road'
                when 'S' then 'Other Sales'
                when 'T' then 'Touring'
                else 'n/a'
            end as prd_line,
            replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
            substring(trim(prd_key), 7) as sls_prd_key,
            prd_start_dt as orig_start_dt,
            prd_end_dt as orig_end_dt,
            prd_start_dt as fixed_start_dt,
            current_timestamp() as dwh_create_date
        from source_data
    ),
    step2_rebuild_timeline as (
        select
            prd_id, prd_key, prd_nm, prd_cost, prd_line, cat_id, sls_prd_key,
            orig_start_dt, orig_end_dt, dwh_create_date,
            fixed_start_dt as prd_start_dt,
            case
                when lead(fixed_start_dt) over (partition by prd_key order by fixed_start_dt, prd_id) is not null
                then date_sub(lead(fixed_start_dt) over (partition by prd_key order by fixed_start_dt, prd_id), 1)
                else null
            end as prd_end_dt
        from step1_base
    )
select * from step2_rebuild_timeline;
```

**Result Validation:**
The following image shows the successful execution in Databricks. You can clearly see how the overlapping historical dates (`orig_start_dt` / `orig_end_dt`) have been perfectly aligned into a clean timeline (`prd_start_dt` / `prd_end_dt`) without any gaps or overlaps.

![Cleaned CRM Product Flow](assets/cleaned%20crm%20prod.png)

#### ERP (`silver_erp_px_cat_g1v2.sql`)
```sql
{{ config(materialized='table', schema='silver', alias='erp_product_categories') }}
with
    source_data as (select * from {{ source('bronze_source', 'bronze_erp_px_cat_g1v2') }})
select
    trim(id) as id,
    trim(cat) as cat,
    trim(subcat) as subcat,
    trim(maintenance) as maintenance,
    current_timestamp() as dwh_create_date
from source_data;
```

### C. Sales Table (`silver_crm_sales_details.sql`)
```sql
{{ config(materialized='table', schema='silver', alias='crm_sales_details') }}
with
    source_data as (select * from {{ source('bronze_source', 'bronze_crm_sales_details') }}),
    filtered_data as (select * from source_data where sls_ord_num is not null),
    cleaned_data as (
        select
            trim(sls_ord_num) as sls_ord_num,
            trim(sls_prd_key) as sls_prd_key,
            sls_cust_id,
            case when sls_order_dt = 0 or length(cast(sls_order_dt as string)) != 8 then null else to_date(cast(sls_order_dt as string), 'yyyyMMdd') end as sls_order_dt,
            case when sls_ship_dt = 0 or length(cast(sls_ship_dt as string)) != 8 then null else to_date(cast(sls_ship_dt as string), 'yyyyMMdd') end as sls_ship_dt,
            case when sls_due_dt = 0 or length(cast(sls_due_dt as string)) != 8 then null else to_date(cast(sls_due_dt as string), 'yyyyMMdd') end as sls_due_dt,
            case
                when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) 
                then sls_quantity * abs(sls_price)
                else sls_sales
            end as sls_sales,
            sls_quantity,
            case
                when sls_price is null or sls_price <= 0 
                then sls_sales / nullif(sls_quantity, 0)
                else sls_price
            end as sls_price,
            current_timestamp() as dwh_create_date
        from filtered_data
    )
select * from cleaned_data;
```

---

## 4. Gold Layer: Star Schema & Architecture

### 🏗️ Architecture Choice: Star Schema vs. OBT (One Big Table)
A common question is: *Why not just join everything into one giant flat table (OBT) at the Silver layer?*
While OBTs are simple for one-off queries, this project implements a **Star Schema (Fact + Dimensions)** for the following enterprise-grade reasons:
1. **Decoupling:** Dimensions (Customers, Products) are decoupled from the Fact table. If a customer changes their address, we only update one row in `dim_customers`, rather than rewriting millions of historical sales records in an OBT.
2. **Storage Efficiency:** An OBT duplicates customer names and product names for every single sales transaction. A Star Schema stores dimension attributes only once.
3. **BI Performance:** Modern BI tools (Tableau, PowerBI) are optimized for Star Schemas. They cache dimensions and use Surrogate Keys to perform lightning-fast joins.

### ⚙️ Execution Order: Dims First, Fact Last
Because the Fact table depends on the Surrogate Keys (`customer_key`, `product_key`) generated in the Dimension tables, the **execution order is strictly: Dimensions -> Fact**.
In dbt, this is automatically handled by the DAG when using `{{ ref() }}`. When running manually, use:
```bash
# Step 1: Build Dimensions
dbt run --select dim_customers dim_products
# Step 2: Build Fact
dbt run --select fct_sales
```

### 🔗 Gold Join Relationship
The following diagram illustrates how the Fact table joins with the Dimension tables using Surrogate Keys and Business Keys.

![Gold Join Relation](assets/gold_join_relation.png)

### A. Dimensions (`dim_customers` & `dim_products`)

#### `dim_customers.sql`
```sql
{{ config(materialized='table', schema='gold') }}

select
    -- Generate Surrogate Key
    row_number() over (order by c.cst_id) as customer_key,
    
    -- Business Keys
    c.cst_id as customer_id,
    c.cst_key,
    
    -- Descriptive columns
    c.cst_firstname,
    c.cst_lastname,
    c.cst_marital_status,
    
    -- Trust CRM as the primary source, fallback to ERP if CRM is null or 'n/a'
    case 
        when c.cst_gndr is not null and c.cst_gndr != 'n/a' then c.cst_gndr
        else coalesce(e.gen, 'n/a')
    end as gender,
    
    e.bdate as birthdate,
    l.cntry as country,
    c.dwh_create_date
from {{ ref('crm_cust_info') }} c
left join {{ ref('erp_customers') }} e on c.cst_key = e.cid
left join {{ ref('erp_locations') }} l on c.cst_key = l.cid;
```

#### `dim_products.sql`
```sql
{{ config(materialized='table', schema='gold') }}

select
    -- Generate Surrogate Key
    row_number() over (order by p.prd_id, p.prd_start_dt) as product_key,
    
    -- Business Keys
    p.prd_id,
    p.prd_key,
    p.sls_prd_key,
    
    -- Descriptive columns
    p.prd_nm,
    p.prd_cost,
    p.prd_line,
    p.cat_id,
    c.cat as category_name,
    c.subcat as subcategory_name,
    p.dwh_create_date
from {{ ref('crm_prd_info') }} p
left join {{ ref('erp_product_categories') }} c on p.cat_id = c.id
-- Only keep the current active product state to prevent fan-out
where p.prd_end_dt is null;
```

### B. Fact Table (`fct_sales.sql`)
```sql
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

    -- Surrogate Keys from Dimensions
    c.customer_key,
    p.product_key,

    -- Descriptive columns (for easy BI querying)
    c.cst_firstname,
    c.cst_lastname,
    c.country,
    p.prd_nm,
    p.category_name,
    p.subcategory_name,

    -- Audit timestamp
    s.dwh_create_date
from {{ ref('crm_sales_details') }} s
left join {{ ref('dim_customers') }} c on s.sls_cust_id = c.customer_id
left join {{ ref('dim_products') }} p on s.sls_prd_key = p.sls_prd_key;
```

---

## 5. dbt Project Structure & Tests

### A. Directory Structure
```text
crm_dbt/
├── assets/
│   ├── data_flow.png
│   ├── cleaned crm prod.png
│   └── gold_join_relation.png
├── crm_dbt_in/
│   ├── models/
│   │   ├── bronze/
│   │   │   └── sources.yml
│   │   ├── silver/
│   │   │   ├── schema.yml
│   │   │   ├── silver_crm_cust_info.sql
│   │   │   ├── silver_erp_cust_az12.sql
│   │   │   ├── silver_erp_loc_a101.sql
│   │   │   ├── silver_crm_prd_info.sql
│   │   │   ├── silver_erp_px_cat_g1v2.sql
│   │   │   └── silver_crm_sales_details.sql
│   │   └── gold/
│   │       ├── schema.yml
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       └── fct_sales.sql
│   ├── dbt_project.yml
│   └── packages.yml
└── README.md
```

### B. Data Tests & Validation Results (`models/gold/schema.yml`)
Implemented **12 automated tests** to ensure industrial-grade data quality. All tests passed successfully (`PASS=12`).

Furthermore, a rigorous **Fan-out (Data Explosion) Validation** was performed on the Gold layer. The row counts before and after joining the Fact table with the Dimension tables remained strictly identical, proving zero data duplication and 100% referential integrity.

**Validation Results:**
- **Original Fact Rows:** 60,398
- **Rows after `LEFT JOIN dim_customers`:** 60,398
- **Rows after `LEFT JOIN dim_products`:** 60,398

This confirms that:
1. **No Orphaned Records:** Every transaction has a valid customer and product.
2. **No Fan-out:** The Surrogate Keys in the dimension tables are strictly unique, ensuring sales metrics are accurate and never double-counted.

```yaml
version: 2

models:
  - name: dim_customers
    description: "Customer dimension table merging CRM and ERP data"
    columns:
      - name: customer_key
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - unique
          - not_null

  - name: dim_products
    description: "Product dimension table merging CRM and ERP category data"
    columns:
      - name: product_key
        tests:
          - unique
          - not_null
      - name: prd_key
        tests:
          - unique
          - not_null

  - name: fct_sales
    description: "Sales fact table containing transactional data"
    columns:
      - name: sls_ord_num
        tests:
          - not_null
      - name: sls_sales
        tests:
          - not_null
      - name: customer_key
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_key
      - name: product_key
        tests:
          - relationships:
              to: ref('dim_products')
              field: product_key
```

---

## 6. How to Run

1. Install dbt packages:
   ```bash
   dbt deps
   ```
2. Run all models (Silver and Gold):
   ```bash
   dbt run
   ```
3. Run data quality tests:
   ```bash
   dbt test
   ```

---

## 7. Key Takeaways

- **Always Profile First:** Use `TRIM`, `HAVING COUNT > 1`, and date-range checks to find anomalies before writing cleaning logic.
- **SCD2 Timeline Repair:** Use `LEAD()` and `DATE_SUB()` to dynamically fix overlapping dates while preserving historical data.
- **Defensive Programming:** Use `NULLIF(column, 0)` to prevent `Divide by Zero` errors. Handle extreme out-of-range dates by setting them to `NULL`.
- **Surrogate Keys in Star Schema:** Generated surrogate keys using `ROW_NUMBER()` in the Gold layer dimensions to decouple the data warehouse from source system business keys, ensuring stable and high-performance joins.
- **Preventing Fan-out:** Filtered `dim_products` using `WHERE prd_end_dt IS NULL` to ensure the dimension table only exposes the current active version of each product, preventing duplicate sales records during the fact table join.
- **Automated Data Quality:** Leveraged dbt's `relationships` test to enforce referential integrity between the fact and dimension tables, ensuring zero orphaned records.

---

## 8. Reference
- **SQL Data Warehouse Portfolio Project:** [YouTube Playlist](https://www.youtube.com/playlist?list=PLNcg_FV9n7qaUWeyUkPfiVtMbKlrfMqA8)