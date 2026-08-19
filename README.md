# Taxi streaming analytics

This project replays March and April 2025 NYC TLC trips through Kafka. A receipt consumer
responds on a vehicle-keyed topic, while Spark writes the same trip events to Delta for dbt
to classify, model, test, and aggregate. NOAA weather remains a batch source.

## Documentation

- [`docs/submission_implementation.md`](docs/submission_implementation.md) explains the
  requirements, implemented architecture, technology choices, reliability behavior, data
  quality gates, model grains, measured results, and local execution evidence.
- [`docs/production_promotion.md`](docs/production_promotion.md) describes the proposed
  managed event backbone, Delta on GCS, BigQuery serving, retention, monitoring, recovery,
  and production rollout.

Start with the implementation document when reviewing the submitted solution. Use the
production document for decisions that are proposed rather than implemented locally.

```text
TLC trips → Kafka ┬→ receipt consumer → receipt topic
                  └→ Spark → Delta → dbt → trip facts and hourly activity

NOAA weather ─────────────────────────→ weather-enriched trip facts
```

This is a finite replay of historical data, not a live fleet feed. TLC does not provide
vehicle identity, so the replay uses stable IDs labelled as synthetic. They demonstrate
receipt routing but must not be used for vehicle-level analysis.

## Run locally

Prerequisites:

- Docker with Compose
- JDK 17
- [`uv`](https://docs.astral.sh/uv/)

Install the locked dependencies and run the test suite:

```bash
make setup
make test
make validate-config
```

Run the representative end-to-end demonstration:

```bash
make streaming-demo-fast
```

This starts Kafka, downloads the configured TLC and NOAA data, publishes a representative
trip selection, issues receipts, writes raw Delta data, builds and tests the dbt models,
replays the selection, and verifies that canonical trip counts do not increase.

Execute the analysis after the demonstration:

```bash
make notebook
```

The command reads [`notebooks/taxi_analysis.ipynb`](notebooks/taxi_analysis.ipynb) and writes
the results to
[`notebooks/taxi_analysis_executed.ipynb`](notebooks/taxi_analysis_executed.ipynb).

Optional commands:

```bash
make streaming-demo-full  # replay all configured March and April trips
make dbt-parse             # validate the dbt project without building it
make dbt-build             # rebuild and test models from the current local data
make streaming-down        # stop Kafka and remove local streaming state
```

The full demonstration downloads source data and processes more than eight million trips,
so the fast target is the normal review and development path.

## Navigate the source

| Path | Purpose |
|---|---|
| [`src/pipeline/fleet.py`](src/pipeline/fleet.py) | TLC replay, deterministic event and vehicle IDs, receipts, Kafka-to-Delta ingestion, and reconciliation |
| [`src/pipeline/acquisition/`](src/pipeline/acquisition/) | TLC and NOAA download, validation, checksums, and stored source versions |
| [`src/pipeline/validation.py`](src/pipeline/validation.py) and [`src/pipeline/dst.py`](src/pipeline/dst.py) | TLC row rules and local-time-to-UTC resolution before events enter Kafka |
| [`src/pipeline/staging.py`](src/pipeline/staging.py) | Batch weather staging and Delta writes |
| [`src/pipeline/spark.py`](src/pipeline/spark.py) | Local Spark and Delta session configuration |
| [`dbt/models/staging/`](dbt/models/staging/) | Parse each raw fleet record once, then route it to valid or rejected views |
| [`dbt/models/canonical/canonical_trip.sql`](dbt/models/canonical/canonical_trip.sql) | Publish one logical trip per deterministic `event_id` |
| [`dbt/models/marts/`](dbt/models/marts/) | Preserve trip grain during weather enrichment and publish hourly activity |
| [`dbt/tests/`](dbt/tests/) | Blocking grain, rejection-rate, enrichment, and aggregation checks |
| [`tests/`](tests/) | Python, timestamp, event, receipt, acquisition, and Spark tests |
| [`Makefile`](Makefile) | Reproducible local commands and end-to-end demonstration order |
| [`config/config.yaml`](config/config.yaml) | Source periods, weather stations, timezone, and quality thresholds |
