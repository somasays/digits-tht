{{ config(materialized='view', tags=['fleet']) }}

-- Records that could not be interpreted, kept whole with the rule that refused them.
-- The payload is the exact bytes Kafka carried, so a rejected record can be read back
-- and re-decided without going to the broker for it again.

select
    quarantine_rule,
    quarantine_detail,
    payload,
    event_id,
    topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingest_run_id,
    _ingested_at_utc
from {{ ref('staging_fleet_trip_classified') }}
where quarantine_rule is not null
