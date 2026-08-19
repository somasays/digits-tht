{{ config(materialized='view', tags=['fleet']) }}

-- Fleet trips that can be interpreted. Types are spelled out rather than inferred: a
-- silently widened column surfaces as a wrong number downstream rather than an error.
--
-- One row per Kafka record, not per trip: the transport may repeat an event and this view
-- keeps every copy. Collapsing to one row per event_id happens at canonical publication.

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

    -- The producer's resolver output, carried rather than recomputed. On the replayed
    -- months no row falls in a DST gap, but a source that never reports the branch it
    -- took cannot be distinguished from one that never had to take it.
    coalesce(dst_shifted, false)          as dst_shifted,
    coalesce(dst_ambiguous, false)        as dst_ambiguous,
    coalesce(boundary_straggler, false)   as boundary_straggler,

    source_period                         as _source_period,
    -- Real lineage: the acquired TLC tree is checksum-addressed and the producer read
    -- the sha256 from the path it replayed.
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
