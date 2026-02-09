{{ config(materialized='view') }}

/*
Staging model for customer behavior data.

Reads from Silver layer and performs basic cleaning/validation.
*/

select
    customer_id,
    event_type,
    event_ts,
    session_id,
    device_type,
    browser,
    revenue,
    time_spent_seconds

from {{ source('silver', 'customer_behavior_silver') }}

where customer_id is not null
  and event_ts is not null
