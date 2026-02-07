{{ config(materialized='view') }}

/*
Staging model for customer behavior data.

Reads from Snowflake raw source and performs basic cleaning/validation.
*/

select
    event_id,
    customer_id,
    event_name,
    event_ts,
    session_id,
    url_1,
    url_2,
    device_type,
    browser,
    country,
    revenue,
    _run_id,
    _exec_date

from {{ source('raw', 'customer_behavior') }}

where event_id is not null
  and customer_id is not null
  and event_ts is not null

