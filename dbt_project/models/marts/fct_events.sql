{{ config(
    materialized='table',
    tags=['marts']
) }}

with base as (
    select
        user_id,
        event_type,
        date(ts) as event_date,
        price,
        quantity
    from {{ ref('stg_events') }}
    where is_valid
),

agg as (
    select
        user_id,
        event_type,
        event_date,
        count(*)                         as event_count,
        sum(quantity)                    as total_quantity,
        sum(price * quantity)            as total_revenue
    from base
    group by user_id, event_type, event_date
)

select *
from agg