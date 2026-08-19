{{ config(severity='warn') }}

-- One check over the eight plausibility flags; they stay on the model for drill-down.
-- Baseline 95.44%, so 90% catches a wholesale shift without firing on monthly variation.
select count(*) as trips, count_if(is_movement_eligible) as eligible
from {{ ref('canonical_trip') }}
having count_if(is_movement_eligible) < 0.90 * count(*)
