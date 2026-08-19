{{ config(tags=['fleet']) }}

-- Kafka repeats; canonical must not. Replaying the same trips produces the same event
-- ids, so a second replay adds rows to raw and to staging but must leave canonical
-- exactly as it was. Nothing else in the graph asserts that.
select canonical.n as canonical_rows, events.n as distinct_events
from (select count(*) as n from {{ ref('canonical_trip') }}) as canonical,
     (select count(distinct event_id) as n from {{ ref('staging_fleet_trip') }}) as events
where canonical.n != events.n
