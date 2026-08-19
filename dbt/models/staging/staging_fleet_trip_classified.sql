{{ config(materialized='table', tags=['fleet']) }}

-- Parse once into a table so downstream models do not repeat from_json over the raw data.
-- Valid and quarantine views share this classification and its rejection denominator.
--
-- The producer supplies authoritative UTC. Values without an explicit Z are quarantined.

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
        -- Invalid JSON yields nulls so the record can be quarantined instead of failing dbt.
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
            -- passenger_count is optional because it is missing in valid source trips.
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
