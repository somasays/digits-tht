-- Attaching weather must not change how many trips there are. This is the check that a
-- non-unique weather key would break.
select canonical.n as canonical_trips, enriched.n as enriched_trips
from (select count(*) as n from {{ ref('canonical_trip') }}) as canonical,
     (select count(*) as n from {{ ref('fact_trip_enriched') }}) as enriched
where canonical.n != enriched.n
