{{ config(
    materialized='incremental',
    unique_key='event_id',
    tags=['staging']
) }}

with source as (

    select
        event_id,
        raw_data,
        received_at
    from RAW.EVENTS_JSON

    {% if is_incremental() %}
      where received_at > coalesce((
        select max(ts) from {{ this }}
      ), '1900-01-01'::timestamp_ntz)
    {% endif %}

),

parsed as (

    select
        coalesce(event_id, raw_data:event_id::string)          as event_id,

        raw_data:event_type::string                            as event_type,
        raw_data:user_id::string                               as user_id,
        raw_data:product_id::string                            as product_id,
        raw_data:price::number(12,2)                           as price,
        raw_data:currency::string                              as currency,
        raw_data:quantity::int                                 as quantity,
        raw_data:session_id::string                            as session_id,
        raw_data:user_agent::string                            as user_agent,

        raw_data:ts::timestamp_ntz                             as ts,

        received_at                                            as inserted_at,

        null::string                                           as source_topic,

        raw_data                                               as raw_payload,

        case
          when coalesce(event_id, raw_data:event_id::string) is null
            or raw_data:event_type is null
            or raw_data:user_id is null
          then false
          else true
        end                                                    as is_valid,

        case
          when coalesce(event_id, raw_data:event_id::string) is null
            then 'MISSING_EVENT_ID'
          when raw_data:event_type is null
            then 'MISSING_EVENT_TYPE'
          when raw_data:user_id is null
            then 'MISSING_USER_ID'
          else null
        end                                                    as error_reason

    from source
)

select *
from parsed
