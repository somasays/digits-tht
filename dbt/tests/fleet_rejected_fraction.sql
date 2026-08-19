{{ config(tags=['fleet']) }}

-- Block canonical publication when any ingestion run rejects more than 1% of its records.
-- The literal matches config/config.yaml because this dbt test cannot read that file.
select
    ingest_run_id,
    count(*)                                          as classified,
    count_if(quarantine_rule is not null)             as quarantined,
    count_if(quarantine_rule is not null) / count(*)  as rejected_fraction
from {{ ref('staging_fleet_trip_classified') }}
group by ingest_run_id
having count_if(quarantine_rule is not null) > 0.01 * count(*)
