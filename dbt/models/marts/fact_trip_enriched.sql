{{ config(materialized='table') }}

-- Trip grain, weather attached. Row count must equal canonical_trip; a difference means the
-- weather key was not unique and trips have been multiplied.

select
    trip.*,

    weather.precip_in,
    weather.precip_raw,
    weather.precip_suspect,
    weather.temperature_f,
    weather.relative_humidity,
    weather.utc_hour is not null as has_weather

from {{ ref('canonical_trip') }} as trip
left join {{ source('staging', 'staging_weather_hour') }} as weather
    on  trip.weather_station = weather.station_key
    and trip.pickup_utc_hour = weather.utc_hour
