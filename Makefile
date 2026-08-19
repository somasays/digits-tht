CONFIG := config/config.yaml
DBT := DBT_PROFILES_DIR=dbt uv run dbt

# The Spark tests need a real JVM. macOS ships a stub at /usr/bin/java that exists and fails
# when run, so a known JDK is preferred over whatever is on PATH.
JAVA_HOME ?= $(firstword $(wildcard /opt/homebrew/opt/openjdk@17 /usr/local/opt/openjdk@17))
export JAVA_HOME

# The local demonstration: schema, raw tree, and how much of the replay to send.
LOCAL_SCHEMA ?= pipeline
RAW_ROOT ?= var/raw/tlc
FLEET_LIMIT ?= 25000
LIMIT_FLAG = $(if $(strip $(FLEET_LIMIT)),--limit $(FLEET_LIMIT),)
DEMO_PERIOD_START ?= 2025-03
DEMO_PERIOD_END ?= 2025-04
INVALID ?= 5
PIPELINE := uv run python -m pipeline.cli

.PHONY: setup test validate-config dbt-parse dbt-build \
        streaming-up streaming-down streaming-demo-fast streaming-demo-full \
        stage-weather notebook

setup:
	uv sync --extra dev

test:
	uv run pytest

validate-config:
	uv run python -m pipeline.cli validate-config --config $(CONFIG)

# Executes the analysis against the marts the demo built and writes a separate executed
# notebook beside the source notebook. Reads the warehouse; builds nothing.
notebook:
	uv run python -m pipeline.notebook

dbt-parse:
	$(DBT) parse --project-dir dbt

dbt-build:
	$(DBT) build --project-dir dbt

# --- local streaming demonstration -------------------------------------------------------
# Every step is its own shell, so a non-zero exit stops the target. The Spark steps run in
# sequence on purpose: the local metastore is embedded Derby and admits one JVM at a time.

# Kafka only. Topic creation is explicit because auto-creation is off, so a mistyped topic
# fails here rather than quietly appearing.
streaming-up:
	docker compose up -d --wait
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic fleet.trip.completed.v1 --partitions 3 --replication-factor 1
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic fleet.vehicle.receipts.v1 --partitions 3 --replication-factor 1

# -v drops the broker's volumes, so Kafka offsets restart at zero. Remove raw data and the
# checkpoint with the broker state so the next local demonstration starts from one
# consistent set of Kafka deliveries.
streaming-down:
	docker compose down -v
	rm -rf var/streaming
	$(PIPELINE) fleet drop-raw --staging-schema $(LOCAL_SCHEMA)

# Acquires the trip files the replay reads, and stages the weather the fact joins. The
# trips themselves are not staged here -- they go to Kafka and come back through dbt.
stage-weather:
	$(PIPELINE) run --config $(CONFIG) \
		--period-start $(DEMO_PERIOD_START) --period-end $(DEMO_PERIOD_END) \
		--raw-path var/raw --staging-schema $(LOCAL_SCHEMA)

# A representative subset: both months, the 9 March transition day, rows with no passenger
# count, the invalid fixtures, and a deliberate redelivery. Minutes, not tens of minutes.
#
# stage-weather first: it acquires the files the replay reads, and stages the weather the
# enriched fact joins. Neither arrives over Kafka.
REDELIVER ?= yes
streaming-demo-fast: streaming-up stage-weather
	$(PIPELINE) fleet replay --config $(CONFIG) --raw-path $(RAW_ROOT) \
		--period-start $(DEMO_PERIOD_START) --period-end $(DEMO_PERIOD_END) \
		$(LIMIT_FLAG) --inject-invalid $(INVALID)
	$(PIPELINE) fleet receipts
	$(PIPELINE) fleet ingest --staging-schema $(LOCAL_SCHEMA)
	$(MAKE) dbt-build
ifneq ($(strip $(REDELIVER)),)
	@echo "\n--- replaying the same trips again: raw and staging grow, canonical must not ---"
	@# The identical selection, so every event is one already published. A smaller limit
	@# would sample a different stride and publish trips the first pass never sent, which
	@# would grow canonical legitimately and prove nothing about redelivery. No invalid
	@# fixtures this time: they are already quarantined and would double the count.
	$(PIPELINE) fleet replay --config $(CONFIG) --raw-path $(RAW_ROOT) \
		--period-start $(DEMO_PERIOD_START) --period-end $(DEMO_PERIOD_END) $(LIMIT_FLAG)
	$(PIPELINE) fleet ingest --staging-schema $(LOCAL_SCHEMA)
	$(MAKE) dbt-build
endif
	$(PIPELINE) fleet verify --staging-schema $(LOCAL_SCHEMA) --expect-quarantined $(INVALID)

# Every March and April trip: 8,115,808 events. Evidence of scale, so it publishes once --
# redelivery is a behavioural property and streaming-demo-fast already proves it at 50k.
# Doing it again at eight million would double the run and prove nothing new.
streaming-demo-full: streaming-up
	$(MAKE) streaming-demo-fast FLEET_LIMIT= REDELIVER=
