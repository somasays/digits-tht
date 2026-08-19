# Production architecture and rollout

## Production requirements

Production does not need to copy the laptop setup. It must keep these rules:

- one versioned completed-trip event as the source of truth,
- independent receipt, fraud, and analytical consumers,
- the same event ID when a trip is delivered more than once,
- storage of the original event before parsing,
- rejected records stored for review instead of silently repaired,
- one row per trip in the canonical table,
- a receipt path whose availability and latency do not depend on the analytics platform,
- retained input and quarantine evidence from which derived data can be rebuilt,
- linked analysis models with tests that can stop publication,
- SQL models that can move to the chosen serving warehouse without rewriting business meaning.

The one-broker Docker setup, embedded Derby metastore, and `AvailableNow` trigger are only for local use. Production does not need to keep them.

**Status:** this document proposes a production design. Statements about the local replay describe the current code. Service placement, regional operation, retention rules, service targets, and delivery through a fleet gateway are proposals. The fraud model and the protocol used to contact a car are out of scope.

## Target architecture

```text
Vehicles / fleet gateway
        |
        | versioned trip.completed events, real vehicle identity
        v
Managed event backbone
        |
        +---- operational receipt service ----> receipt topic ----> fleet gateway ----> car
        |          regional, low-latency, independently deployed
        |
        +---- low-latency fraud processor ----> risk decision / alert
        |          independent deployment and progress
        |
        +---- streaming ingestion
                   |
                   v
          Delta raw tables on GCS
                   |
                   v
          classified / valid / quarantine Delta tables
                   |
                   v
          canonical trip Delta table
                   |
                   +---- governed investigation / replay
                   |
                   v
          BigQuery-native fact and marts
                   |
                   v
          BI, notebooks, forecasting, operational analytics

NOAA / weather provider ---- scheduled acquisition ---- weather Delta table ----+
```

- **Receipts:** use a separate subscription, scaling policy, response-time target, and fleet-gateway connection. Analytics delays do not stop receipts.
- **Fraud:** runs as another independent consumer. Its model and actions are out of scope.
- **Vehicle identity:** production events use fleet-issued IDs. Synthetic replay IDs are rejected outside simulation and cannot support vehicle-level analysis.

## Storage and analysis tables

Production keeps the same responsibilities while changing where they run:

- Managed Kafka or Pub/Sub carries events.
- Spark on Dataproc writes retained data to Delta on GCS.
- dbt builds the analysis tables and publishes selected tables to BigQuery.
- A table is copied to both Delta and BigQuery only when each copy has a clear use.

### Delta on GCS

Keep raw, rejected, staging, and canonical data in Delta on GCS:

- Raw input records what the event system delivered before parsing.
- Rejected data must remain available after messages expire from Kafka or Pub/Sub so operators can inspect and replay it.
- Staging stores the result of contract checks without parsing the full history for every query.
- Canonical stores one row per trip event for reuse by several data products. BigQuery is therefore not the only recovery path.

Delta provides:

- transactions on GCS, with the required settings and permissions for multiple writers ([Delta storage configuration](https://docs.delta.io/delta-storage/)),
- table history and retained files for rollback, audit, and reprocessing ([Delta batch and time-travel documentation](https://docs.delta.io/delta-batch/)),
- recovery when trips repeat, validation rules change, or rejected records need review.

### BigQuery tables for analysts

- Store `fact_trip_enriched` and `hourly_trip_activity` as native BigQuery tables for dashboards, scheduled analysis, and concurrent SQL users.
- Keep models, dependencies, tests, and business definitions in one dbt project.
- Use the BigQuery adapter to publish fact and hourly tables while Delta remains the storage layer below them.
- Test SQL differences and adapter behavior before release.
- Publish one enriched row per trip and one aggregate row per station-hour.
- Advance the analyst-facing dataset or view only after `dbt build` passes.

This structure already allowed the project to move from Databricks to local Spark without rewriting the canonical, fact, and hourly models. Production keeps the same benefit instead of rebuilding the analytics stack as warehouse-specific jobs.

Use BigLake for investigation and controlled access to Delta tables on GCS. Do not use it as the main write path for analyst tables.

## Production reliability

### System reliability

The local broker is a single point of failure. Production changes this as follows:

- Use managed Kafka or Pub/Sub across several zones.
- Deploy and scale receipt, fraud, and analytics consumers separately.
- Keep receipt capacity separate from weather and analytics backlogs.
- Preserve `event_id` when a producer resends a trip.
- Publish a stable `receipt_id` before acknowledging the input, the fleet gateway ignores IDs it has already handled.
- Use Kafka transactions and/or a durable outbox if input and output must become one atomic operation.

Monitoring should show whether cars and analysts are affected:

- receipt time and failed receipt publication,
- consumer lag before messages expire,
- checkpoint progress,
- time from raw input to canonical trips,
- failed hourly-table publication.

Recovery tests must cover consumer restart, replay, checkpoint recovery, and a regional failure.

For event-contract changes:

- Use a **schema registry** and CI check once vehicle producers deploy separately.
- Allow (backward) compatible changes and reject breaking changes before release.
- Test old and new events against receipt and analytics contracts.
- use versioining for breaking and incompatible changes (versioning of events, new topic for new version etc.)
- Keep the contracts separate: an event may receive a receipt and still be rejected from analytics.

### Data platform reliability

Data retention must cover each recovery need:

- GCS protects raw events during the audit and replay period.
- Delta log and file retention cover incident response, rollback, and long-running stream readers.
- `VACUUM` runs only after those periods because time travel needs table history and data files ([Delta retention and `VACUUM`](https://docs.delta.io/delta-batch/)).
- Rejected records remain available for investigation and replay.

Rebuild classified, canonical, fact, and hourly tables from stored lower layers instead of editing rows by hand. Build a candidate version and check:

- one canonical row per deterministic event ID.
- rejected-event rate.
- event and weather uniqueness.
- trip counts after the weather join.
- hourly totals.

Point users to the candidate only after it passes. CI should run Python, Spark, event-contract, and dbt checks before release.

Each run publishes:

- input volume,
- rejection counts and reasons,
- unique trip events and trip-count checks,
- consumer and checkpoint lag,
- age of the latest published data.

Report these by producer version, region, and ingestion run. One global rate can hide a broken small producer behind the rest of the fleet.

## CI/CD

The receipt service, streaming ingestion, and dbt project should be released separately. A change to an hourly model does not need to redeploy receipt processing, and a receipt change does not need to restart the Spark stream.

For each pull request, CI should:

- run Python tests for event creation, timestamp handling, receipt validation, and deterministic IDs,
- run Spark tests for Kafka ingestion and Delta writes,
- run dbt models and quality tests against small fixed inputs,
- check event-contract compatibility when the message schema changes,
- build versioned application images once the checks pass.

The same image and dbt revision should move through development, staging, and production. Staging should replay a fixed set of trips and verify receipt counts, rejected records, canonical trip counts, weather-join reconciliation, and checkpoint recovery. This uses the same checks demonstrated by the local fast replay with isolated topics, checkpoints, storage paths, and dbt schemas.

Production rollout should start with one region. Receipt latency, publication failures, consumer lag, Delta progress, rejection rate, and dbt quality results should be checked before expanding. Application rollback must keep the raw Delta data and streaming checkpoint. dbt should build candidate tables and point users to them only after the blocking tests pass.

## Data retention and deletion

Retention periods should come from recovery and audit needs, not from default service settings.

### Kafka or Pub/Sub retention

Kafka or Pub/Sub should keep messages long enough for a normal outage and short replay. It is not the permanent archive. Set retention longer than the planned outage and response window, and alert before consumer lag reaches that limit. Delta remains the long-term analytical recovery source.

### GCS and Delta retention

Use separate storage prefixes/buckets and lifecycle policies for raw, quarantine, checkpoints, and derived tables:

- Raw event data should be retained for the agreed audit/recomputation horizon, encrypted, access-controlled, and partitioned by ingestion/event date to bound scans. Older raw partitions can transition from GCS Standard to colder storage classes after the active replay window.
- Quarantine should retain enough history to diagnose producer regressions and replay corrected records. Ownership and resolution status belong in an operational workflow, not only a table.
- Checkpoints are service state. Back them up or protect them according to recovery objectives, and never point a new query identity at an old checkpoint without a migration plan.
- Classified staging can be rebuilt. Canonical trip data receives longer analytical retention because it is the reusable logical-event boundary and shortens recovery for multiple downstream products.
- BigQuery facts and marts are rebuildable serving products. Their retention and partition-expiration policy follows actual consumption and recovery-time needs, snapshots are useful for fast rollback, not as the only source of truth.

Delta has separate rules for table history and old data files. Schedule `VACUUM` only after the audit, streaming, and rollback periods are agreed ([Delta retention and `VACUUM`](https://docs.delta.io/delta-batch/)).

### Rebuilding data

A rebuild should create new table versions or a new dataset. Run all quality checks and compare counts and business measures with the current version. Point users to the new version only after it passes. Do not overwrite the only known-good hourly tables while the rebuild is running.

## Choosing Kafka or Pub/Sub

Production can use managed Kafka or Google Cloud Pub/Sub. Pub/Sub is not a direct replacement for the current Kafka code. It changes message positions, replay, checkpoints, and connectors. Event IDs and the later data models can remain the same.

| Decision concern | Managed Kafka | Pub/Sub |
|---|---|---|
| Implemented semantics retained | Topics, partitions, offsets, consumer groups, vehicle-key partitioning, and the Spark Kafka connector remain recognizable | Consumers become subscriptions, vehicle routing/order uses an ordering key |
| Independent consumers | Separate receipt, fraud, and analytics consumer groups | Separate receipt, fraud, and analytics subscriptions |
| Recovery and replay | Offset retention and consumer-group position, Spark checkpoint records Kafka progress | Subscription retention, acknowledgement state, seek/snapshot policy, and connector checkpointing |
| Raw transport identity | `(topic, partition, offset)` | Subscription/message identity plus publish and ingestion metadata |
| Migration cost | Lower because it preserves the implemented boundary | Higher because producer, consumers, raw schema, reconciliation, and operations change |
| Invariant that does not change | Stable `event_id`, independent consumers, deterministic receipt identity, retained input, canonical deduplication | The same |

**Proposed managed Kafka path.** Managed Kafka is the lowest-risk promotion when compatibility with the implemented protocol, partition/offset lineage, consumer groups, and Spark connector is valuable. The current raw coordinate identity remains unchanged and existing replay and receipt code need fewer adaptations. A multi-zone managed service replaces the local broker.

**Proposed Pub/Sub path.** Choose Pub/Sub when the company prefers a managed GCP messaging service over Kafka operations. A vehicle ID can be an ordering key, but Pub/Sub ordering differs from a Kafka partition ([Pub/Sub publishing guidance](https://docs.cloud.google.com/pubsub/docs/publish-best-practices)).

Pub/Sub may deliver a message again. Stable event and receipt IDs and canonical duplicate removal are still required ([subscription behavior](https://docs.cloud.google.com/pubsub/docs/subscription-overview)). Raw records would use Pub/Sub message and subscription details instead of Kafka offsets.

In either path, Spark streaming can run on Dataproc, Delta tables can reside in GCS, and analyst-facing models can be published to BigQuery. The Dataproc BigQuery connector supplies the managed Spark-to-BigQuery bridge ([Dataproc BigQuery connector](https://docs.cloud.google.com/dataproc/docs/concepts/connectors/bigquery)).
