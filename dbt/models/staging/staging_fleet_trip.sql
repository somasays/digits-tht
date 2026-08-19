{{ config(materialized='view', tags=['fleet']) }}

-- Cast valid records to the canonical input types. This view keeps every Kafka delivery.
-- Canonical publication reduces repeated deliveries to one row per event_id.

select
    cast(pickup_at_utc  as timestamp)     as pickup_utc,
    cast(dropoff_at_utc as timestamp)     as dropoff_utc,
    cast(pickup_location_id  as int)      as PULocationID,
    cast(dropoff_location_id as int)      as DOLocationID,
    cast(passenger_count as bigint)       as passenger_count,
    cast(trip_distance_miles as double)   as trip_distance,
    cast(fare_amount as double)           as fare_amount,
    cast(tip_amount  as double)           as tip_amount,
    cast(total_amount as double)          as total_amount,

    -- Preserve the producer's timestamp-resolution results.
    coalesce(dst_shifted, false)          as dst_shifted,
    coalesce(dst_ambiguous, false)        as dst_ambiguous,
    coalesce(boundary_straggler, false)   as boundary_straggler,

    source_period                         as _source_period,
    -- Source checksum from the acquired TLC file.
    source_checksum                       as _source_checksum,
    _ingested_at_utc,

    event_id,
    vehicle_id,
    ingest_run_id,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
from {{ ref('staging_fleet_trip_classified') }}
where quarantine_rule is null
