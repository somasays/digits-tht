{{ config(tags=['fleet']) }}

-- The publication gate. dbt runs a model's tests before its dependents, so an ingestion
-- run that rejects too much blocks canonical_trip instead of publishing into it.
--
-- Numerator: records quarantined. Denominator: records classified from one ingestion run.
-- Window: one completed ingest_run_id, every run in the schema, not merely the most
-- recent -- a bad run must not be masked by a good one that follows it. Threshold is the
-- same 0.01 declared in config/config.yaml for the batch path; it is a literal here
-- because a dbt test cannot read that file, as weather_coverage.sql already notes.
-- See design doc 11.1.
select
    ingest_run_id,
    count(*)                                          as classified,
    count_if(quarantine_rule is not null)             as quarantined,
    count_if(quarantine_rule is not null) / count(*)  as rejected_fraction
from {{ ref('staging_fleet_trip_classified') }}
group by ingest_run_id
having count_if(quarantine_rule is not null) > 0.01 * count(*)
