{{ config(severity='warn') }}

-- Two holes, not one: an hour with no observation, and an hour whose observation omits
-- precipitation. Warn, so patchy weather cannot block publishing trip truth. Window frozen
-- to 2025-01..04 because a test cannot read config.yaml. Baselines in design doc 11.1.
with hourly as (
    select station_key,
           count(*)                                 as hours,
           count_if(coalesce(precip_raw, '') != '') as hours_with_precip
    from {{ source('staging', 'staging_weather_hour') }}
    where utc_hour >= '2025-01-01' and utc_hour < '2025-05-01'
    group by station_key
)

select station_key, hours, hours_with_precip
from hourly
where hours < 0.95 * 2880
   or hours_with_precip < 0.90 * hours
