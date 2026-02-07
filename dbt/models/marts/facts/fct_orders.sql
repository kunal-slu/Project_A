{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    merge_update_columns=['total_amount', 'product_id', 'updated_at'],
    tags=['fact', 'incremental']
  )
}}

/*
Fact Table: Orders

Business Logic:
- Incremental load with merge strategy
- Handles late-arriving data (updates last 3 days)
- One row per order

Late Data Strategy:
- Reprocess rolling 3-day window
- Merge updates existing records
- Safe for late corrections

Owner: Data Engineering Team
SLA: Hourly refresh
*/

with orders as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        total_amount,
        current_timestamp() as updated_at
    from {{ source('silver', 'orders_silver') }}
    
    {% if is_incremental() %}
    -- Reprocess last 3 days to handle late-arriving data
    where order_date >= dateadd(day, -3, current_date())
    {% endif %}
),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.order_date,
        o.total_amount,
        o.updated_at,
        
        -- Add useful business attributes
        date_trunc('month', o.order_date) as order_month,
        date_trunc('quarter', o.order_date) as order_quarter,
        year(o.order_date) as order_year,
        
        -- Add customer segment at time of order
        c.customer_segment,
        
        -- Add product category
        p.category as product_category,
        
        -- Calculate margin (simplified)
        o.total_amount - (p.price_usd * 0.6) as estimated_margin
        
    from orders o
    left join {{ ref('dim_customer') }} c on o.customer_id = c.customer_id
    left join {{ ref('dim_product') }} p on o.product_id = p.product_id
)

select * from enriched
