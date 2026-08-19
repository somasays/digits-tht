{{ config(materialized='table', tags=['fleet']) }}

-- The only place a fleet payload is parsed, and a table rather than a view because of
-- it. As a view every downstream model and test re-ran from_json over the whole raw
-- table: at 50k records that is free, at 8.1M it was about ten re-parses and the build
-- stopped finishing. Materialising here parses once. Still no incremental machinery --
-- raw is append-only, so a rebuild is a rebuild. The valid and quarantine models filter this
-- one, so a rule can never mean two different things depending on which side reads it,
-- and the rejection rate has a single denominator.
--
-- Fleet instants are authoritative UTC: the producer resolved civil time through the
-- pipeline's own resolver before publishing. Nothing here converts a zone, and a value
-- without an explicit Z is quarantined rather than guessed at.

with raw as (
    select
        topic,
        partition                          as kafka_partition,
        offset                             as kafka_offset,
        timestamp                          as kafka_timestamp,
        ingest_run_id,
        _ingested_at_utc,
        cast(value as string)              as payload
    from {{ source('raw', 'raw_fleet_trip_event') }}
),

parsed as (
    select
        *,
        -- PERMISSIVE: a payload that is not JSON yields nulls rather than failing the
        -- build, so the record survives to be quarantined with its bytes intact.
        from_json(payload, '{{ var("fleet_event_schema") }}') as event
    from raw
),

classified as (
    select
        topic, kafka_partition, kafka_offset, kafka_timestamp,
        ingest_run_id, _ingested_at_utc, payload,

        event.event_id,
        event.vehicle_id,
        event.pickup_at_utc,
        event.dropoff_at_utc,
        event.pickup_location_id,
        event.dropoff_location_id,
        event.passenger_count,
        event.trip_distance_miles,
        event.fare_amount,
        event.tip_amount,
        event.total_amount,
        event.dst_shifted,
        event.dst_ambiguous,
        event.boundary_straggler,
        event.source_period,
        event.source_checksum,

        case
            when event is null or event.event_type is null
                then 'fleet.payload_unreadable'
            when event.schema_version != '{{ var("fleet_schema_version") }}'
                or event.event_type != '{{ var("fleet_event_type") }}'
                then 'fleet.payload_unreadable'
            -- passenger_count is absent on roughly a fifth of real rows and is not
            -- required; a trip without one is still a trip.
            when event.event_id is null or event.vehicle_id is null
                or event.pickup_at_utc is null or event.dropoff_at_utc is null
                or event.pickup_location_id is null or event.dropoff_location_id is null
                or event.trip_distance_miles is null or event.fare_amount is null
                or event.total_amount is null
                then 'fleet.contract_violation'
            when event.pickup_at_utc not like '%Z' or event.dropoff_at_utc not like '%Z'
                then 'fleet.contract_violation'
            else null
        end as quarantine_rule
    from parsed
)

select
    *,
    case
        when quarantine_rule = 'fleet.payload_unreadable'
            then 'not JSON, or not a supported schema_version and event_type'
        when quarantine_rule = 'fleet.contract_violation'
            then 'a required field is absent, or an instant carries no explicit UTC'
    end as quarantine_detail
from classified
