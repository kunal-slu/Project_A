{{ config(materialized='table') }}

/*
Fact table: Customer engagement metrics.

Aggregates customer behavior events into engagement metrics.
Business-friendly metrics for analytics dashboards.
*/

with base as (
    select * from {{ ref('stg_customer_behavior') }}
),

agg as (
    select
        customer_id,

        -- Event counts
        count(*) as total_events,
        sum(case when event_type = 'page_view' then 1 else 0 end) as page_views,
        sum(case when event_type = 'product_view' then 1 else 0 end) as product_views,
        sum(case when event_type = 'add_to_cart' then 1 else 0 end) as add_to_carts,
        sum(case when event_type = 'purchase' then 1 else 0 end) as purchase_events,
        sum(case when event_type = 'email_open' then 1 else 0 end) as email_opens,
        sum(case when event_type = 'email_click' then 1 else 0 end) as email_clicks,

        -- Time metrics
        min(event_ts) as first_event_date,
        max(event_ts) as last_event_date,

        -- Revenue
        sum(coalesce(revenue, 0)) as total_revenue,

        -- Engagement
        count(distinct session_id) as total_sessions,
        count(distinct to_date(event_ts)) as active_days,
        count(distinct browser) as browsers_used,
        count(distinct device_type) as devices_used

    from base
    group by 1
),

engagement_scores as (
    select
        *,
        
        -- Calculate engagement score (0-100)
        least(100, (
            (total_events * 0.1) +
            (total_sessions * 2) +
            (active_days * 5) +
            (purchase_events * 10)
        )) as engagement_score,
        
        -- Calculate conversion rate
        case
            when add_to_carts > 0
            then cast(purchase_events as double) / add_to_carts * 100
            else 0
        end as cart_to_purchase_rate
        
    from agg
)

select * from engagement_scores
