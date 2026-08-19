{{ config(materialized='view', tags=['fleet']) }}

-- Keep rejected payloads and their rules for investigation or later reclassification.

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
