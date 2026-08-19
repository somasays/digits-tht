-- Nothing else covers the group-by itself losing or duplicating trips.
select fact.n as fact_trips, mart.n as mart_trips
from (select count(*) as n from {{ ref('fact_trip_enriched') }}
      where weather_station is not null) as fact,
     (select sum(trips) as n from {{ ref('hourly_trip_activity') }}) as mart
where fact.n != mart.n
