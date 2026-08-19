-- The enriched fact joins trips to weather on this pair. A duplicate multiplies trips
-- silently, so this has to fail the build rather than be noticed later.
select station_key, utc_hour
from {{ source('staging', 'staging_weather_hour') }}
group by station_key, utc_hour
having count(*) > 1
