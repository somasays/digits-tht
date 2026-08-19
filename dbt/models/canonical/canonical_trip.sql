{{ config(materialized='view') }}

-- Domain names, the borough's station, and flags that describe a trip without discarding it.
-- Nothing is filtered: staging already decided which rows are interpretable.

with trip as (
    -- One row per event, not per Kafka record. Delivery is at-least-once and a replay
    -- repeats events deliberately, so raw and staging both carry copies; this is the
    -- only place they collapse. Ordering by the Kafka coordinates makes the surviving
    -- copy the same one on every rebuild -- the key is vehicle_id so copies should share
    -- a partition, but that is a property of the partitioner, not a guarantee.
    select
        pickup_utc, dropoff_utc, PULocationID, DOLocationID,
        passenger_count, trip_distance, fare_amount, tip_amount, total_amount,
        dst_shifted, dst_ambiguous, boundary_straggler,
        _source_period, _source_checksum, _ingested_at_utc
    from (
        select *,
               row_number() over (
                   partition by event_id
                   order by kafka_timestamp, kafka_partition, kafka_offset
               ) as _copy
        from {{ ref('staging_fleet_trip') }}
    )
    where _copy = 1
),

zone as (
    select * from {{ ref('taxi_zone_lookup') }}
),

joined as (
    select
        trip.pickup_utc,
        trip.dropoff_utc,
        date_trunc('hour', trip.pickup_utc)                       as pickup_utc_hour,
        (unix_timestamp(trip.dropoff_utc)
         - unix_timestamp(trip.pickup_utc)) / 60.0                as trip_duration_minutes,

        trip.PULocationID                                         as pickup_location_id,
        trip.DOLocationID                                         as dropoff_location_id,
        zone.Borough                                              as pickup_borough,
        zone.Zone                                                 as pickup_zone,

        -- Nearest station with a full hourly record; the rest have none close enough.
        case zone.Borough
            when 'Manhattan' then 'central_park'
            when 'Queens'    then 'laguardia'
            when 'Bronx'     then 'laguardia'
            when 'Brooklyn'  then 'jfk'
        end                                                       as weather_station,

        trip.passenger_count,
        trip.trip_distance                                        as trip_distance_miles,
        trip.fare_amount,
        trip.tip_amount,
        trip.total_amount,

        trip.dst_shifted,
        trip.dst_ambiguous,
        trip.boundary_straggler,
        trip._source_period,
        trip._source_checksum,
        trip._ingested_at_utc
    from trip
    left join zone
        on trip.PULocationID = zone.LocationID
),

flagged as (
    select
        *,
        trip_distance_miles
            / nullif(trip_duration_minutes / 60.0, 0)  as average_speed_mph,

        total_amount < 0                               as has_negative_total,
        -- Missing and zero differ: unreported count vs reported empty trip.
        passenger_count is null                        as passenger_count_missing,
        passenger_count = 0                            as passenger_count_zero,
        coalesce(pickup_borough in ('Unknown', 'N/A'), true)
                                                       as pickup_zone_unknown,
        trip_distance_miles <= 0
            or trip_distance_miles > 100               as distance_implausible,
        trip_duration_minutes <= 0
            or trip_duration_minutes > 1440            as duration_implausible
    from joined
)

select
    *,
    coalesce(average_speed_mph > 100, false) as speed_implausible,
    -- One definition, so every mart counts the same population. Excluded trips stay, flagged.
    not (
        has_negative_total
        or distance_implausible
        or duration_implausible
        or coalesce(average_speed_mph > 100, false)
    )                                        as is_movement_eligible
from flagged
