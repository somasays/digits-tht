# Streaming architecture implementation

## Summary

- **Trip ingestion:** March and April 2025 TLC trips pass through Kafka, Spark Structured Streaming, Delta, and dbt into trip and hourly tables. Existing business models remain in dbt.
- **Vehicle response:** a separate Kafka consumer publishes a receipt for each trip with the required receipt fields. It does not use Spark, Delta, or dbt.
- **Weather:** NOAA weather remains a batch source because the task does not require live weather events.
- **Scope:** this is a fixed replay of historical data, not a live feed.
- **Vehicle identity:** TLC has no vehicle ID. The replay creates stable IDs and labels them as synthetic. They demonstrate receipt routing but cannot support analysis by vehicle.
- **Profitable time analysis:** the weekend-night and Monday-morning comparison uses trip times and money fields, so synthetic vehicle IDs do not affect it.

## Requirements

The assignment assumes and addresses seven requirements:

- **The task must be promotable to prod without rearchitecture or engineering**
  - All layers and components are modular and can flexibly scale to the business and volume needs
  - All components are based on open standards and can 1:1 be switched for managed counterpart with cloud providers
  - The local flow processed more than 8.1 million source rows. Raw, canonical, and fact counts reconcile. All 28 dbt build steps pass.
  - Receipt and analytics consumers run separately. Spark checkpoints progress. Delta stores rebuildable input. dbt tests can stop publication.
  - Production replaces the local broker and metastore with managed messaging, Delta on GCS, BigQuery serving, real vehicle identity, and production monitoring.
- **Tables must be ready for analysis.**
  - Canonical and enriched tables contain one row per trip. The hourly table contains one row per station and hour.
  - Quality flags keep unusual trips visible.
  - Total trips, usable movement trips, and weather-matched trips remain separate counts.
- **Opeartional and Analytics flows are orthogonal and should not affect one another**
  - The receipt consumer uses its own Kafka consumer group and checks only the fields needed for a receipt.
  - It publishes to a topic keyed by vehicle before committing input offsets.
  - The demo stops at the receipt topic. A production fleet gateway is not implemented.
- **Reliability is non-negotiable**
  - Reliability is a cross cutting concern and is addressed at every stage.
  - Duplicates, events well past their watermark window and bad records do not pollute downstream and are handled within their stage
- **Duplicate delivery must not change business results.**
  - Source checksum and row number produce a stable `event_id`. `receipt_id` comes from that event ID.
  - Raw keeps every delivery, while `canonical_trip` keeps one trip per event ID.
  - The fast demo sends the same trips twice and verifies that only raw and staging counts grow.
- **Local timestamps must become reliable UTC times.**
  - TLC times are New York local times without an offset.
  - The replay resolves them before creating events and carries the resolver flags forward.
  - No real replay row falls in the missing DST hour. A synthetic test covers that case.
- **Invalid events must remain available without entering trusted tables.**
  - Raw ingestion retains the original Kafka key and payload with topic, partition, offset, and ingestion metadata before parsing.
  - One classified model parses the record and assigns a rule.
  - Valid and rejected views use that result. The rejected view keeps the payload, Kafka position, rule, and error detail.
- **Weather joins must not add or remove trips.**
  - Weather is unique by station and UTC hour.
  - A left join preserves trips with missing weather.
  - Tests check weather uniqueness and the enriched trip count. Missing weather remains different from dry weather.


## Architecture

```text
TLC Parquet (March + April)                  NOAA observations
          |                                      |
          | source validation, timezone          | acquire and stage
          | resolution, event construction       v
          v                              staging_weather_hour (Delta)
fleet.trip.completed.v1
          |
          +---- receipt service ----> fleet.vehicle.receipts.v1
          |       independent consumer group
          |
          +---- Spark Structured Streaming (AvailableNow)
                         |
                         v
              raw_fleet_trip_event (Delta)
                         |
                         v
              staging_fleet_trip_classified (dbt table)
                   | clean                 | rejected
                   v                       v
              staging_fleet_trip      staging_fleet_trip_quarantine
                   |
                   v
              canonical_trip (one row per event_id)
                   |
                   v
              fact_trip_enriched (one row per trip)
                   |
                   v
              hourly_trip_activity (one row per station-hour)
```

### Receipt path

```text
fleet.trip.completed.v1 → receipt consumer → fleet.vehicle.receipts.v1
          vehicle key                            vehicle key
```

- The consumer in [`fleet.py`](../src/pipeline/fleet.py) has its own Kafka consumer group.
- It checks only the fields needed for a receipt.
- It finishes publishing receipts before it commits input offsets.
- A repeated event produces the same `receipt_id`.
- The producer creates stable `sim-vehicle-NNNN` IDs because TLC has no vehicle identity.
- Each event labels that identity as `synthetic`, so it cannot be used for vehicle-level analysis.
- The implementation publishes to the vehicle-keyed receipt topic. A fleet gateway and physical-car consumer are out of scope.

### Analytics path

```text
trip topic → Spark ingestion → raw Delta → classified/valid/quarantine
                                                ↓
                                      canonical → enriched fact → hourly mart
```

## Why this stack is used

The mmodular architecture separates source handling, event transport, storage, and analytical modeling. Each part can therefore change without requiring the others to be rewritten.

### Kafka

Kafka helps separates the two(and more) uses of a completed-trip event:

- The receipt service reads the event and responds to the vehicle.
- The analytics consumer writes the event into analytical storage.

Each consumer keeps its own progress. Spark or dbt can be stopped without stopping receipts. Kafka also allows the demo to resend events and prove that duplicate delivery does not increase canonical trip counts.

This directly satisfies the requirement to send data to cars while processing the same events for analytics.

### Spark

Spark performs the narrow distributed task for which it is needed: reading Kafka and writing a large number of records into Delta.

It provides:

- Kafka offset and transport metadata.
- Checkpoint-based restart.
- Streaming writes into Delta.
- The same ingestion pattern for the finite replay and future continuous processing.

`Trigger.AvailableNow` processes the current backlog and stops for the demo. Production can use a continuous trigger with the same processing flow ([Spark Structured Streaming guide](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html)).

Spark does not own business models. Keeping business logic out of the ingestion avoids creating a Python transformation stack that must be maintained separately from the analytical SQL.

### Delta

  Delta is the table format for the implemented analytical stack.

  Spark writes the batch staging tables and incoming Kafka records as Delta tables. dbt also materializes the classified fleet records, enriched trip facts, and hourly marts as Delta tables. The canonical, valid, and quarantine models are views over this retained Delta data.

  This provides a consistent storage structure across ingestion and analytics:

  - Kafka records are stored before parsing or filtering.
  - Invalid records remain available for investigation.
  - dbt can rebuild classification, canonical views, facts, and marts from retained lower layers.
  - Committed Delta versions prevent downstream models from reading an incomplete write.
  - Spark can use the same tables for batch and streaming operations.
  - Local storage can be replaced with GCS without changing the table format.

  Delta therefore does more than connect Spark ingestion to dbt. It provides the storage structure used throughout the local implementation and proposed production data platform. Its transactions, schema controls, history, and support for batch and streaming access protect that structure
  (Delta Lake documentation (https://docs.delta.io/)).

  In production, the retained layers can remain as Delta tables on GCS (Delta storage configuration (https://docs.delta.io/delta-storage/)). BigQuery can provide governed access to selected Delta tables through BigLake, while frequently queried facts and marts can be published as native
  BigQuery tables when interactive performance or BI concurrency requires it (BigQuery Delta Lake tables (https://docs.cloud.google.com/bigquery/docs/create-delta-lake-table)). This avoids maintaining every data layer in both systems.

  The precise implementation statement is:

  - All physical analytical tables use Delta.
  - Some dbt relations are views rather than physical tables.
  - BigQuery is part of the production proposal, not the local implementation.

### dbt

The main value of dbt in this solution is that it keeps the complete analytical stack modular and portable.

The analytical flow is divided into models with clear responsibilities:

```text
Kafka record
→ classified record
→ valid or rejected record
→ one canonical trip
→ one weather-enriched trip
→ one station-hour row
```

Each model depends on the previous one, and tests protect the point where the meaning of a row changes. Classification tests stop invalid records from reaching canonical. Uniqueness tests protect the trip grain. Reconciliation tests prove that the weather join and hourly aggregation have not added or removed trips. `dbt build` stops later models when a required upstream test fails ([dbt build reference](https://docs.getdbt.com/reference/commands/build)).

This structure also separates the analytics code from the execution platform. The same dbt project was moved from a Databricks-oriented setup to local Spark by changing the adapter configuration and resolving a small number of engine-specific details. The canonical, fact, mart, and quality logic did not need to be rebuilt in another framework.

That is the concrete portability benefit in this task: the analytical model is not tied to the original warehouse or to the Kafka ingestion program.

For production, the same separation allows retained layers to stay in Delta on GCS while analyst-facing facts and marts move to BigQuery. This still requires adapter tests and changes for SQL differences. dbt does not guarantee that every query runs unchanged across engines. It does keep the model boundaries, dependencies, tests, and business definitions in one project ([dbt supported platforms](https://docs.getdbt.com/docs/supported-data-platforms)).

Source links:

- Events, receipts, and ingestion: [`fleet.py`](../src/pipeline/fleet.py) and [`test_fleet.py`](../tests/test_fleet.py).
- Acquisition and timestamps: [`download.py`](../src/pipeline/acquisition/download.py), [`validation.py`](../src/pipeline/validation.py), and [`dst.py`](../src/pipeline/dst.py).
- Classification: [`staging_fleet_trip_classified.sql`](../dbt/models/staging/staging_fleet_trip_classified.sql) and its valid and rejected views.
- Analysis models: [`canonical_trip.sql`](../dbt/models/canonical/canonical_trip.sql), [`fact_trip_enriched.sql`](../dbt/models/marts/fact_trip_enriched.sql), and [`hourly_trip_activity.sql`](../dbt/models/marts/hourly_trip_activity.sql).
- Quality checks and commands: [`dbt/tests`](../dbt/tests) and the [`Makefile`](../Makefile).

## Handling failures

The design handles failures by keeping the input data and making repeated processing safe.

### Source file problems

- Downloads first land under a temporary path.
- Content length, content type, readability, and TLC column types are checked before publication.
- A SHA-256 directory and atomic rename make the same downloaded file use one stored copy.
- Unknown compatible columns are reported. Missing or retyped required columns stop publication. (Backward Compatibility)
- Local timestamps are resolved to UTC before event construction.
- Unusable timestamps and rows beyond the period tolerance are excluded and counted.

### Duplicate events

- Event IDs come from source checksum and row number. Receipt IDs come from event IDs.
- Kafka delivery is treated as at least once.
- The Spark checkpoint resumes ingestion from the last processed Kafka position and avoids unnecessary repeated work during normal recovery.
- Raw Delta retains the deliveries read from Kafka, including repeated deliveries.
- `canonical_trip` uses `row_number()` to keep one row per `event_id`.

The system tracks delivery, trip, and receipt IDs separately:

| Concern | Identity | Purpose |
|---|---|---|
| Transport record | topic, partition, offset | Prove what Kafka delivery was ingested |
| Logical trip event | `event_id` | Make redelivery harmless to analytical publication |
| Vehicle response | `receipt_id` derived from `event_id` | Make the acknowledgement stable across attempts |

The checkpoint controls ingestion progress, but correctness does not depend on every delivery appearing only once. An event or receipt can repeat, so stable IDs make repeats safe and the canonical model publishes one trip per `event_id`. The implementation does not claim that the whole flow is exactly-once.

### Rule changes

- Raw Delta keeps the Kafka payload instead of repairing it in place.
- The classified table parses it once and assigns one rule when invalid.
- Valid and rejected views use the same classification result.
- The rejected view keeps payload text, Kafka position, rule, and error detail.
- Classification, canonical, fact, and hourly models can be rebuilt from retained lower layers.
- Acquisition storage and raw Kafka landing are append-oriented. Derived dbt tables are reproducible, not immutable.

### Weather join problems

- Weather has one row per `(station, UTC hour)`.
- A left join keeps trips that have no weather observation.
- One test checks weather-key uniqueness. Another checks the enriched trip count.
- Missing weather remains different from zero precipitation.
- Unlikely trip values remain visible with flags.

## Data quality gates

Some records cannot be read as trips. These include invalid JSON, unsupported versions, missing required fields, and timestamps without an explicit `Z`. They go to the rejected view. Negative totals and unlikely movement values remain in the trip table with flags because the trip may still have happened.

Representative gates are:

| Boundary | Risk | Control and reason | Failure outcome | Evidence |
|---|---|---|---|---|
| Download → source files | Partial or changed downloads | Byte count, source metadata, SHA-256, and atomic promotion prevent half-written files from becoming inputs | Acquisition fails. No source is published | [`download.py`](../src/pipeline/acquisition/download.py), [`test_acquisition.py`](../tests/test_acquisition.py), [`test_acquisition_noaa.py`](../tests/test_acquisition_noaa.py) |
| TLC row → fleet event | Naive time interpreted incorrectly or outside its period | Existing resolver converts New York civil time to UTC and records DST/boundary evidence | Unusable source rows are not emitted and are counted | [`validation.py`](../src/pipeline/validation.py), [`dst.py`](../src/pipeline/dst.py), [`fleet.py`](../src/pipeline/fleet.py), [`test_dst.py`](../tests/test_dst.py) |
| Trip event → receipt | Envelope cannot support a trustworthy response | Receipt-specific version, identity, amount, and UTC-instant checks | No receipt is issued. Analytics evaluates its separate contract | [`fleet.py`](../src/pipeline/fleet.py), [`test_fleet.py`](../tests/test_fleet.py) |
| Kafka → raw Delta | A restart repeats ingestion work | A stable Spark checkpoint resumes from recorded Kafka progress. Raw retains each delivery and its Kafka metadata. Logical duplicates are handled in canonical | The ingestion job resumes from its checkpoint. A republished event remains in raw and is deduplicated later | [`fleet.py`](../src/pipeline/fleet.py), [`sources.yml`](../dbt/models/sources.yml) |
| Raw → classified | Malformed or unsupported envelopes | One parse assigns `fleet.payload_unreadable` or `fleet.contract_violation`. Original payload and coordinates remain available | Record is quarantined, not silently discarded | [`staging_fleet_trip_classified.sql`](../dbt/models/staging/staging_fleet_trip_classified.sql), [`staging_fleet_trip_quarantine.sql`](../dbt/models/staging/staging_fleet_trip_quarantine.sql) |
| Classified → canonical | A bad ingestion run overwhelms valid data | Rejected fraction must remain at or below 1% for every ingestion run | Error-severity test blocks downstream build | [`fleet_rejected_fraction.sql`](../dbt/tests/fleet_rejected_fraction.sql), [`dbt_project.yml`](../dbt/dbt_project.yml) |
| Staging → canonical | At-least-once redelivery inflates trips | One deterministic row per `event_id`. A uniqueness test verifies the published grain | Build fails if a logical event appears twice | [`canonical_trip.sql`](../dbt/models/canonical/canonical_trip.sql), [`fleet_event_unique_in_canonical.sql`](../dbt/tests/fleet_event_unique_in_canonical.sql) |
| Weather → fact | Non-unique weather hours multiply trips | Weather-hour uniqueness plus fact/canonical row reconciliation | Enriched fact is not published | [`weather_hour_is_unique_per_station.sql`](../dbt/tests/weather_hour_is_unique_per_station.sql), [`enrichment_preserves_trip_grain.sql`](../dbt/tests/enrichment_preserves_trip_grain.sql) |
| Fact → hourly mart | Aggregation drops or duplicates trips | Hourly reconciliation checks the mart against fact rows attributable to a weather station | Mart publication fails | [`hourly_reconciles_to_fact.sql`](../dbt/tests/hourly_reconciles_to_fact.sql), [`hourly_trip_activity.sql`](../dbt/models/marts/hourly_trip_activity.sql) |

Additional policy choices:

- Weather coverage and movement eligibility are warnings, not blockers. Valid trips remain available during an unusual period or when weather is missing.
- The hourly table keeps `trips`, `movement_eligible_trips`, and `weather_matched_trips` as separate counts. Missing weather is never labelled dry.
- `passenger_count` is nullable because 18.8% of the measured April rows omit it and TLC does not guarantee it. Canonical keeps these trips and sets `passenger_count_missing`.
- Negative amounts and unlikely distance, duration, or speed values remain with flags. Movement measures can exclude them without removing trip and revenue records.

## Modeling approach

The table below shows what one row means in each model and who uses it:

| Layer | Grain | Responsibility | Intended consumer |
|---|---|---|---|
| `raw_fleet_trip_event` | One ingested Kafka record | Transport evidence and ingestion lineage before interpretation | Recovery, audit, and classification |
| `staging_fleet_trip_classified` | One decision per raw record | Parse once and assign one technical rule | Valid/quarantine routing and quality gates |
| `staging_fleet_trip` | One analytically valid delivery | Typed trip fields plus Kafka lineage | Canonical publication |
| `staging_fleet_trip_quarantine` | One rejected delivery | Payload text, coordinates, rule, and detail | Data-quality investigation and controlled reclassification |
| `canonical_trip` | One logical trip per `event_id` | Resolve redelivery, apply business names and zone mapping, derive duration, retain quality flags | Reusable trip-level analytics |
| `fact_trip_enriched` | One canonical trip | Attach optional station-hour weather without changing trip grain | Trip-level weather analysis and mart construction |
| `hourly_trip_activity` | One UTC station-hour | Publish demand, revenue, distance, duration, weather coverage, and precipitation category | Notebook, BI, and recurring analysis |

Analysis notes:

- UTC is used for storage and weather joins.
- The notebook converts UTC to `America/New_York` before defining weekend nights and Monday mornings.
- The comparison reports revenue, not profit, because the source has no fuel, vehicle, labour, or idle-time costs.
- Weekend-night rides are shorter and worth less individually, but their higher volume produces about 50% more revenue per hour than Monday mornings.
- Synthetic vehicle identity does not affect this result because the comparison uses real trip times and amounts, not vehicle IDs.

## Local execution and evidence

Prerequisites are Docker, JDK 17, and `uv`. No cloud account, credentials, `.env`, Databricks workspace, Thrift server, or external metastore is required.

```bash
# Install locked Python and development dependencies
make setup

# Run all current Python/Spark tests and validate configuration
make test
make validate-config

# Representative end-to-end demonstration (50,000 valid events by default,
# both months, invalid fixtures, and deterministic redelivery)
make streaming-demo-fast

# Execute the analysis against the resulting marts
make notebook

# Stop Kafka, remove its local volume/checkpoint, and drop the raw table
make streaming-down
```

For the measured full-volume path:

```bash
make streaming-demo-full
make notebook
```

The full target publishes all March and April trips once. The fast target sends its selected trips twice. Raw and staging counts increase, while the canonical count stays the same. The final `fleet verify` command prints counts for each layer and fails if they do not reconcile.

`make notebook` writes the executed analysis to `notebooks/taxi_analysis_executed.ipynb`. dbt writes compiled models and run results under `dbt/target/`. Local Delta tables use the configured `var/warehouse` and staging paths. Component commands include `make streaming-up`, `make stage-weather`, `make dbt-build`, and `make dbt-parse`.

### Evidence verified for this submission

The following commands were rerun against the current branch on 18 August 2026:

| Command | Current result |
|---|---|
| `make test` | 112 tests passed in 41.80 seconds |
| `make dbt-build` | 28/28 resources passed in 58.75 seconds: one seed, three tables, three views, and 21 data tests |
| `pipeline fleet verify --expect-quarantined 5` | Reconciled successfully against the existing full-demo tables |

The current reconciliation was:

| Layer | Rows |
|---|---:|
| Raw Kafka records | 8,115,813 |
| Classified records | 8,115,813 |
| Staged valid records | 8,115,808 |
| Quarantined records | 5 |
| Distinct logical events | 8,115,808 |
| Canonical trips | 8,115,808 |
| Enriched fact trips | 8,115,808 |
| Mart-attributable trips | 8,095,968 |

Evidence notes:

- `hourly_trip_activity` contains only trips that map to a weather station. `hourly_reconciles_to_fact` compares the same group of trips.
- The executed analysis is stored at `notebooks/taxi_analysis_executed.ipynb`.
- The original replay and ingestion time was not saved as a separate evidence file, so it is not reported.
- The demo uses one broker and a fixed replay. It checks the data flow, not production speed or uptime.
- A receipt is published with a simulated vehicle key. Delivery through a fleet gateway is proposed, not implemented.
