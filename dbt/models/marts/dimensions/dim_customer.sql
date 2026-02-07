{{
  config(
    materialized='table',
    tags=['dimension', 'scd_type1']
  )
}}

/*
Dimension: Customer

Business Logic:
- Type 1 SCD (overwrite)
- One row per customer
- Includes customer attributes and calculated metrics

Owner: Data Engineering Team
SLA: Daily refresh by 6 AM
*/

with customers as (
    select * from {{ source('silver', 'customers_silver') }}
),

orders_agg as (
    select
        customer_id,
        count(*) as lifetime_orders,
        sum(total_amount) as lifetime_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ source('silver', 'orders_silver') }}
    group by 1
),

behavior_agg as (
    select
        customer_id,
        count(*) as total_events,
        count(distinct session_id) as total_sessions
    from {{ source('silver', 'customer_behavior_silver') }}
    group by 1
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        c.registration_date,
        
        -- Aggregated metrics
        coalesce(o.lifetime_orders, 0) as lifetime_orders,
        coalesce(o.lifetime_value, 0) as lifetime_value,
        o.first_order_date,
        o.last_order_date,
        
        -- Behavior metrics
        coalesce(b.total_events, 0) as total_events,
        coalesce(b.total_sessions, 0) as total_sessions,
        
        -- Customer segmentation
        case
            when o.lifetime_value >= 1000 then 'VIP'
            when o.lifetime_value >= 500 then 'High Value'
            when o.lifetime_value >= 100 then 'Regular'
            else 'New'
        end as customer_segment,
        
        -- Timestamps
        current_timestamp() as dbt_updated_at
        
    from customers c
    left join orders_agg o on c.customer_id = o.customer_id
    left join behavior_agg b on c.customer_id = b.customer_id
)

select * from final
