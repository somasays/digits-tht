{{ config(materialized='table') }}

-- Demand per station-hour. Denominators stay apart: an hour with no reading is not a dry
-- hour, and a trip that failed a plausibility check still happened.

select
    pickup_utc_hour                                              as utc_hour,
    weather_station,

    count(*)                                                     as trips,
    count_if(is_movement_eligible)                               as movement_eligible_trips,
    count_if(has_weather)                                        as weather_matched_trips,

    sum(total_amount)                                            as total_revenue,
    avg(case when is_movement_eligible then trip_distance_miles end)
                                                                 as avg_distance_miles,
    avg(case when is_movement_eligible then trip_duration_minutes end)
                                                                 as avg_duration_minutes,

    max(precip_in)                                               as precip_in,
    max(temperature_f)                                           as temperature_f,

    -- Attribute of the hour's one reading; count_if would have counted trips.
    max(precip_suspect)                                          as precip_suspect,

    -- Every branch positive, so a blank or unparsed reading lands outside the buckets
    -- instead of defaulting to dry.
    case
        when max(precip_raw) = 'T' then 'trace'
        when max(precip_in) > 0    then 'measurable'
        when max(precip_in) = 0    then 'dry'
    end                                                          as precip_bucket

from {{ ref('fact_trip_enriched') }}
where weather_station is not null
group by pickup_utc_hour, weather_station
