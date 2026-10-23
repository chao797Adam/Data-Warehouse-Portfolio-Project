{{ config(materialized='table', schema='gold') }}

with
    customer_orders as (
        -- Layer 1: Base metrics at the customer grain
        select
            c.customer_key,
            -- Using MAX to avoid 'not in GROUP BY' errors while keeping the business
            -- key
            max(c.cst_key) as cst_key,
            c.cst_firstname || ' ' || c.cst_lastname as customer_name,
            c.birthdate,
            -- Calculate age exactly once at the lowest grain
            datediff(year, c.birthdate, current_date()) as age,
            count(distinct fs.sls_ord_num) as total_orders,
            sum(fs.sls_sales) as total_sales,
            sum(fs.sls_quantity) as total_quantity,
            -- Count how many unique products this customer has bought
            count(distinct fs.product_key) as total_products,
            min(fs.sls_order_dt) as first_purchase,
            max(fs.sls_order_dt) as last_purchase,
            datediff(
                month, min(fs.sls_order_dt), max(fs.sls_order_dt)
            ) as lifespan_months
        from {{ ref('fct_sales') }} fs
        left join {{ ref('dim_customers') }} c on fs.customer_key = c.customer_key
        group by c.customer_key, customer_name, c.birthdate
    ),
    customer_kpis as (
        -- Layer 2: Derived KPIs, Segmentation, and Age Groups
        select
            *,
            round(total_sales / nullif(total_orders, 0), 2) as avg_order_value,
            round(total_sales / nullif(lifespan_months, 0), 2) as avg_monthly_spend,
            datediff(month, last_purchase, current_date()) as recency_months,

            -- 1. Customer Value Segment
            case
                when lifespan_months is null
                then 'Unknown'
                when lifespan_months >= 12 and total_sales > 5000
                then 'VIP'
                when lifespan_months >= 12 and total_sales <= 5000
                then 'Regular'
                else 'New'
            end as customer_segment,

            -- 2. Age Group Segmentation (Using the pre-calculated age)
            case
                when birthdate is null
                then 'Unknown Age'
                when age < 20
                then 'Under 20'
                when age between 20 and 29
                then '20-29'
                when age between 30 and 39
                then '30-39'
                when age between 40 and 49
                then '40-49'
                else '50 and above'
            end as age_group

        from customer_orders
    )
-- Final output: A unified Customer 360 view
select
    customer_key,
    cst_key,
    customer_name,
    customer_segment,
    age,
    age_group,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    avg_order_value,
    avg_monthly_spend,
    recency_months,
    lifespan_months
from customer_kpis
order by total_sales desc
;
