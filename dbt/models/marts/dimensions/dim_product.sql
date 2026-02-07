{{
  config(
    materialized='table',
    tags=['dimension', 'scd_type1']
  )
}}

/*
Dimension: Product

Business Logic:
- Type 1 SCD (overwrite)
- One row per product
- Includes product performance metrics

Owner: Data Engineering Team
SLA: Daily refresh by 6 AM
*/

with products as (
    select * from {{ source('silver', 'products_silver') }}
),

product_sales as (
    select
        product_id,
        count(*) as total_orders,
        sum(total_amount) as total_revenue,
        count(distinct customer_id) as unique_customers
    from {{ source('silver', 'orders_silver') }}
    group by 1
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.price_usd as price,  -- Rename for consistency
        
        -- Performance metrics
        coalesce(ps.total_orders, 0) as total_orders,
        coalesce(ps.total_revenue, 0) as total_revenue,
        coalesce(ps.unique_customers, 0) as unique_customers,
        
        -- Product tier
        case
            when ps.total_revenue >= 10000 then 'Top Performer'
            when ps.total_revenue >= 5000 then 'Strong Performer'
            when ps.total_revenue >= 1000 then 'Average Performer'
            else 'Low Performer'
        end as performance_tier,
        
        -- Timestamps
        current_timestamp() as dbt_updated_at
        
    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from final
