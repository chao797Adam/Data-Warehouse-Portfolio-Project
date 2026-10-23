{{ config(materialized='table', schema='gold') }}

with
    product_sales_metrics as (
        -- Layer 1: Base metrics at the product grain
        select
            p.product_key,
            p.prd_nm as product_name,
            p.category_name,
            p.subcategory_name,
            -- Time metrics
            min(fs.sls_order_dt) as first_sale_date,
            max(fs.sls_order_dt) as last_sale_date,
            datediff(
                month, min(fs.sls_order_dt), max(fs.sls_order_dt)
            ) as lifespan_months,
            -- Volume and Reach metrics
            count(distinct fs.sls_ord_num) as total_orders,
            count(distinct fs.customer_key) as total_customers,
            sum(fs.sls_sales) as total_sales,
            sum(fs.sls_quantity) as total_quantity
        from {{ ref('fct_sales') }} fs
        left join {{ ref('dim_products') }} p on fs.product_key = p.product_key
        group by p.product_key, p.prd_nm, p.category_name, p.subcategory_name
    ),
    product_kpis as (
        -- Layer 2: Derived KPIs and Performance Segmentation
        select
            *,
            -- Calculate weighted average selling price (Total Sales / Total Quantity)
            -- This is more statistically accurate than AVG(price per row)
            round(total_sales / nullif(total_quantity, 0), 2) as avg_selling_price,

            -- Product Performance Segmentation based on Revenue
            case
                when total_sales > 50000
                then 'High-Performer'
                when total_sales >= 10000
                then 'Mid-Range'
                else 'Low-Performer'
            end as product_segment

        from product_sales_metrics
    )
-- Final output: A unified Product 360 view
select
    product_key,
    product_name,
    category_name,
    subcategory_name,
    product_segment,
    first_sale_date,
    last_sale_date,
    lifespan_months,
    total_orders,
    total_customers,
    total_sales,
    total_quantity,
    avg_selling_price
from product_kpis
order by total_sales desc
;
