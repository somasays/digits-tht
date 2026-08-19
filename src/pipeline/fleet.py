"""The fleet event path: trips onto Kafka, and the two things that read them back.

One topic, two independent consumer groups. The receipt service reads a completed trip
and posts the receipt it owes; Spark reads the same trip and lands it in raw Delta. They
share nothing but the topic, which is the point -- receipts keep being issued while the
analytical stack is down, and a trip published by any producer earns one, not only the
trips this module happens to replay.

The receipt path is deliberately free of Spark, Delta and dbt imports, because an import
graph that cannot express that independence is not evidence of it.

What crosses the boundary is a versioned JSON envelope keyed by vehicle. Timestamps are
resolved to UTC here, at the producer, using the pipeline's own resolver -- so nothing
downstream has to infer a civil offset, and dbt never reinterprets an instant.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path

logger = logging.getLogger("pipeline.fleet")

SCHEMA_VERSION = "1"
EVENT_TYPE = "trip.completed"
RECEIPT_TYPE = "receipt.issued"

TOPIC_TRIPS = "fleet.trip.completed.v1"
TOPIC_RECEIPTS = "fleet.vehicle.receipts.v1"

BOOTSTRAP = "localhost:9092"
GROUP_RECEIPTS = "receipt-service-v1"
GROUP_ANALYTICS = "analytics-raw-v1"

# Fixed namespaces, so an id depends only on its inputs and never on when it was produced.
NS_EVENT = uuid.UUID("6f1d5f4a-0b8e-5c2a-9d3e-000000000001")
NS_VEHICLE = uuid.UUID("6f1d5f4a-0b8e-5c2a-9d3e-000000000003")
NS_RECEIPT = uuid.UUID("6f1d5f4a-0b8e-5c2a-9d3e-000000000004")

# TLC records no vehicle. The replay invents one so there is something to send a receipt
# back to, and says so in every envelope it writes. See vehicle_id() for what this costs.
VEHICLE_POOL = 500

CURRENCY = "USD"


def vehicle_id(checksum: str, row: int) -> str:
    """A synthetic vehicle for a source trip.

    TLC publishes no vehicle identity, so this is invented and the assignment is
    arbitrary. Anything keyed on it -- per-vehicle profitability, utilisation, shift
    patterns, trajectories -- would be measuring this function rather than the fleet.
    The existing analysis is unaffected: it keys on instants and trip money.

    uuid5 rather than hash(): hash is salted per process, so the same trip would land on
    a different vehicle in the consumer than in the producer.
    """
    return "sim-vehicle-%04d" % (uuid.uuid5(NS_VEHICLE, f"{checksum}:{row}").int % VEHICLE_POOL)


def _instant(value) -> str:
    """Format a resolved instant with an explicit Z.

    The resolver returns tz-aware UTC, so this only formats; it never converts. An
    offset-free string here would be a value some consumer has to guess at, which is
    exactly what the contract refuses.
    """
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _number(value):
    """None for pandas NA, otherwise a plain Python number.

    passenger_count is absent on roughly a fifth of real rows, so null is a value the
    contract carries rather than a reason to reject the trip.
    """
    import pandas as pd

    return None if pd.isna(value) else value.item() if hasattr(value, "item") else value


def trip_events(frame, checksum: str, period: str):
    """Turn resolved trip rows into completed-trip envelopes.

    `frame` has already been through validation.apply_rules, so instants are UTC and the
    resolver's flags are present. Rows it quarantined are TLC-source problems -- an
    unreadable timestamp, a pickup nowhere near the period -- and are not the fleet
    contract's business, so the caller drops them before getting here.
    """
    for row in frame.itertuples():
        index = int(row.Index)
        yield {
            "schema_version": SCHEMA_VERSION,
            "event_type": EVENT_TYPE,
            # Name-based: same source row, same id, in any process, on any day.
            "event_id": str(uuid.uuid5(NS_EVENT, f"{checksum}:{index}")),
            "vehicle_id": vehicle_id(checksum, index),
            # Declared, so no consumer can mistake a simulated car for a real one.
            "vehicle_identity_type": "synthetic",
            "source": "tlc_replay",
            "pickup_at_utc": _instant(row.pickup_utc),
            "dropoff_at_utc": _instant(row.dropoff_utc),
            "pickup_location_id": int(row.PULocationID),
            "dropoff_location_id": int(row.DOLocationID),
            "passenger_count": _number(row.passenger_count),
            "trip_distance_miles": _number(row.trip_distance),
            "fare_amount": _number(row.fare_amount),
            "tip_amount": _number(getattr(row, "tip_amount", None)),
            "total_amount": _number(row.total_amount),
            "currency": CURRENCY,
            # The resolver's own output. A native fleet producer emitting UTC would have
            # nothing to report here, but a replay converted civil time and must say how.
            "dst_shifted": bool(row.dst_shifted),
            "dst_ambiguous": bool(row.dst_ambiguous),
            "boundary_straggler": bool(row.boundary_straggler),
            "source_period": period,
            "source_checksum": checksum,
        }


def receipt_for(event: dict) -> dict | None:
    """The receipt owed for a completed trip, or None if the event cannot support one.

    This reads whatever is on the topic, which is why it validates. The contract is
    narrower than the analytical one on purpose: a receipt needs an identity, a vehicle to
    send it to, an amount to state, and an instant that carries its zone. It does not need
    locations or distance, which the trip fact does. Two contracts over one envelope, not
    one contract duplicated.
    """
    if not isinstance(event, dict):
        return None
    if event.get("schema_version") != SCHEMA_VERSION or event.get("event_type") != EVENT_TYPE:
        return None
    required = ("event_id", "vehicle_id", "total_amount", "dropoff_at_utc")
    if any(event.get(field) is None for field in required):
        return None
    # The receipt restates this instant in a field named _utc, so it has to be one.
    if not str(event["dropoff_at_utc"]).endswith("Z"):
        return None

    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": RECEIPT_TYPE,
        # Derived from identity alone, never from the payload: a redelivery carrying a
        # mutated body still settles onto the same receipt rather than issuing a second.
        "receipt_id": str(uuid.uuid5(NS_RECEIPT, event["event_id"])),
        "causation_event_id": event["event_id"],
        "vehicle_id": event["vehicle_id"],
        "completed_at_utc": event["dropoff_at_utc"],
        "fare_amount": event.get("fare_amount"),
        "tip_amount": event.get("tip_amount"),
        "total_amount": event["total_amount"],
        "currency": event.get("currency", CURRENCY),
    }


# --- Kafka -----------------------------------------------------------------------------
# Everything above is a pure function of its inputs and is tested without a broker.


def _producer(bootstrap: str):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap,
        # Bytes pass through untouched, so the invalid fixtures stay malformed on the
        # wire instead of being made well-formed by their own serializer.
        key_serializer=lambda k: k if isinstance(k, bytes) else k.encode(),
        value_serializer=lambda v: v if isinstance(v, bytes)
        else json.dumps(v, separators=(",", ":")).encode(),
        # Every replica must have the record before it counts as sent. The demonstration
        # runs one broker, so this costs nothing here and is the setting a second one needs.
        acks="all",
        linger_ms=50,
        batch_size=256 * 1024,
        compression_type="gzip",
    )


INVALID_FIXTURES = [
    b"{not json at all",
    b'{"schema_version":"99","event_type":"trip.completed","event_id":"i2"}',
    b'{"schema_version":"1","event_type":"trip.started","event_id":"i3"}',
    b'{"schema_version":"1","event_type":"trip.completed","event_id":"i4"}',
    b'{"schema_version":"1","event_type":"trip.completed","event_id":"i5",'
    b'"vehicle_id":"sim-vehicle-0001","pickup_at_utc":"2025-03-15 12:00:00",'
    b'"dropoff_at_utc":"2025-03-15 12:10:00","pickup_location_id":1,"dropoff_location_id":2,'
    b'"trip_distance_miles":1.0,"fare_amount":10.0,"total_amount":12.0,"currency":"USD"}',
]
"""Five records that must not reach the marts: unparseable, unsupported version, wrong
event type, missing required fields, and an instant with no zone. Published alongside the
real replay so the quarantine path is exercised by the demonstration rather than asserted."""


def replay(period_start: str, period_end: str, raw_root: Path, config, *,
           limit: int | None = None, inject_invalid: int = 0) -> dict:
    """Publish completed-trip events for an inclusive period range.

    Reads the acquired TLC parquet, resolves civil time to UTC through the pipeline's own
    rules, and writes one keyed event per trip. What reads them back is not this function's
    business: the receipt service and the analytical ingest each subscribe for themselves.
    """
    import pandas as pd

    # From config, not pipeline: pipeline imports staging, which imports pyspark, and
    # this module's whole point is that it does not.
    from pipeline.config import period_range
    from pipeline.staging import check_readable
    from pipeline.validation import apply_rules

    sources = {}
    for period in period_range(period_start, period_end):
        matches = sorted(Path(raw_root).glob(f"yellow/{period}/*/*.parquet"))
        if not matches:
            raise FileNotFoundError(f"no acquired TLC file for {period} under {raw_root}")
        sources[period] = matches[0]

    # The whole range is refused before a single event is published, so a bad file in the
    # second period cannot leave the first period's trips already on the topic. Only the
    # parquet footer is read.
    check_readable(sources)

    producer = _producer(BOOTSTRAP)
    started = time.monotonic()
    emitted = skipped = 0

    for period, path in sources.items():
        # The raw tree is checksum-addressed, so the directory name is the file's sha256.
        # That makes source lineage real for these events rather than invented.
        checksum = path.parent.name

        frame = pd.read_parquet(path)
        if limit is not None:
            # Spread across the month, not the first N rows. head() returns the opening
            # hours of the 1st, which gives a couple of dozen station-hours and no
            # weather to speak of -- the pipeline reconciles on it but the analysis it
            # feeds cannot say anything. The index is preserved, so event ids are still
            # the ones those source rows always get.
            frame = frame.iloc[:: max(len(frame) // limit, 1)].head(limit)
        resolved = apply_rules(frame, period, config.taxi_civil_timezone,
                               config.boundary_tolerance_days)
        usable = resolved[resolved["quarantine_rule"].isna()]
        skipped += len(resolved) - len(usable)

        for event in trip_events(usable, checksum, period):
            producer.send(TOPIC_TRIPS, key=event["vehicle_id"], value=event)
            emitted += 1
            if emitted % 250_000 == 0:
                rate = emitted / max(time.monotonic() - started, 1e-9)
                logger.info("replayed %s events (%.0f/s)", f"{emitted:,}", rate)

    for raw in INVALID_FIXTURES[:inject_invalid]:
        producer.send(TOPIC_TRIPS, key=b"sim-vehicle-0000", value=raw)

    producer.flush()
    producer.close()

    elapsed = time.monotonic() - started
    counts = {
        "emitted": emitted,
        "skipped": skipped,
        "invalid": min(inject_invalid, len(INVALID_FIXTURES)),
        "elapsed_seconds": round(elapsed, 1),
        "rate_per_second": round(emitted / elapsed) if elapsed else 0,
    }
    logger.info(
        "replay complete: emitted=%s skipped=%s invalid=%s in %ss (%s/s)",
        f"{counts['emitted']:,}", counts["skipped"], counts["invalid"],
        counts["elapsed_seconds"], f"{counts['rate_per_second']:,}",
    )
    return counts


def receipts(*, idle_timeout: float = 15.0) -> dict:
    """Read completed trips and post the receipt each one owes.

    Its own consumer group, so it sees every trip regardless of what the analytical
    ingest is doing, and needs nothing from Spark, Delta or dbt to keep working.

    Offsets are committed after the broker has the receipts, never before, which makes
    delivery at-least-once rather than at-most-once: a crash between the two replays the
    trip and reissues a receipt carrying the same id. That is why the identity is derived
    from the event rather than generated.

    Stops when the topic goes quiet, so the demonstration is finite without being told how
    many records to expect.
    """
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        TOPIC_TRIPS, bootstrap_servers=BOOTSTRAP, group_id=GROUP_RECEIPTS,
        auto_offset_reset="earliest", enable_auto_commit=False,
        value_deserializer=lambda v: v,
        consumer_timeout_ms=int(idle_timeout * 1000), max_poll_records=1000,
    )
    producer = _producer(BOOTSTRAP)
    started = last_record_at = time.monotonic()
    consumed = issued = unreceiptable = 0

    for message in consumer:
        consumed += 1
        last_record_at = time.monotonic()
        try:
            event = json.loads(message.value)
        except (ValueError, TypeError):
            event = None
        receipt = receipt_for(event)
        if receipt is None:
            # The analytical path keeps the bytes and quarantines them; the receipt path
            # simply owes nothing.
            unreceiptable += 1
        else:
            producer.send(TOPIC_RECEIPTS, key=receipt["vehicle_id"], value=receipt)
            issued += 1
        if consumed % 10_000 == 0:
            producer.flush()
            consumer.commit()
            logger.info("consumed %s, issued %s receipts", f"{consumed:,}", f"{issued:,}")

    producer.flush()
    consumer.commit()
    producer.close()
    consumer.close()

    working = max(last_record_at - started, 1e-9)
    counts = {"consumed": consumed, "issued": issued, "unreceiptable": unreceiptable,
              "elapsed_seconds": round(working, 1),
              "rate_per_second": round(consumed / working) if consumed else 0}
    logger.info(
        "receipts complete: consumed=%s issued=%s unreceiptable=%s in %ss (%s/s, "
        "excluding the idle wait that ends the run)",
        f"{counts['consumed']:,}", f"{counts['issued']:,}", counts["unreceiptable"],
        counts["elapsed_seconds"], f"{counts['rate_per_second']:,}",
    )
    return counts


RAW_TABLE = "raw_fleet_trip_event"
CHECKPOINT_ROOT = Path("var/streaming/checkpoints")


def ingest(schema: str, *, run_id: str | None = None) -> dict:
    """Land every Kafka record in append-only Delta, byte for byte.

    The value is stored exactly as it arrived: unparsed, unrepaired, not deduplicated.
    Interpreting it is dbt's job, and keeping the original is what makes a later change
    of interpretation a rebuild rather than a re-ingestion.

    Trigger.AvailableNow drains the backlog and stops, which is what makes the
    demonstration finite. Continuous triggering is a deployment choice, not a different
    transformation.
    """
    from pyspark.sql.functions import current_timestamp, lit

    from pipeline.spark import DELTA, KAFKA, local_session

    run_id = run_id or f"ingest-{int(time.time())}"
    spark = local_session("fleet-ingest", packages=(DELTA, KAFKA))
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

    # Stable, and named for what it tracks. A per-run checkpoint would re-read the topic
    # from the beginning every run and write the same Kafka coordinates again.
    checkpoint = CHECKPOINT_ROOT / TOPIC_TRIPS / GROUP_ANALYTICS
    checkpoint.parent.mkdir(parents=True, exist_ok=True)

    frame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC_TRIPS)
        .option("kafka.group.id", GROUP_ANALYTICS)
        # Only consulted when the checkpoint is absent; afterwards the checkpoint decides.
        # Without it a first run would start at the tail and silently skip the backlog.
        .option("startingOffsets", "earliest")
        .load()
        .select(
            "key", "value", "topic", "partition", "offset", "timestamp", "timestampType",
            # Names an ingestion execution, never an event. Deduplication is by event_id,
            # in canonical; this is lineage and the denominator for the rejection rate.
            lit(run_id).alias("ingest_run_id"),
            current_timestamp().alias("_ingested_at_utc"),
        )
    )

    query = (
        frame.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        # A registered table, not a path: dbt runs in its own JVM and finds this through
        # the shared metastore.
        .toTable(f"{schema}.{RAW_TABLE}")
    )
    query.awaitTermination()

    total = spark.table(f"{schema}.{RAW_TABLE}").count()
    this_run = spark.table(f"{schema}.{RAW_TABLE}").where(f"ingest_run_id = '{run_id}'").count()
    spark.stop()

    logger.info("ingest %s: appended=%s table_total=%s", run_id, this_run, total)
    return {"ingest_run_id": run_id, "appended": this_run, "raw_total": total}


def drop_raw(schema: str) -> None:
    """Forget the ingested records. Only safe alongside dropping the broker's volumes,
    which is the one situation that invalidates them: Kafka offsets restart at zero and
    the coordinates already in raw would be issued again."""
    from pipeline.spark import local_session

    spark = local_session("fleet-drop-raw")
    spark.sql(f"DROP TABLE IF EXISTS {schema}.{RAW_TABLE}")
    spark.stop()
    logger.info("dropped %s.%s", schema, RAW_TABLE)


def verify(schema: str, expect_quarantined: int) -> dict:
    """Print the analytical counts, and assert the one thing nothing else does.

    The reconciliation is already enforced where it belongs: canonical against distinct
    event_id is fleet_event_unique_in_canonical, fact against canonical is
    enrichment_preserves_trip_grain, and raw against valid-plus-quarantined is structural,
    since the two views are complementary filters over an unfiltered table. `dbt build`
    runs before this and fails the demonstration on any of them.

    Receipts are not counted here. A receipt is an obligation to a vehicle and a trip row
    is an analytical fact; they answer to different consumers on different timescales, and
    tying one's count to the other's would invent a constraint neither side has.
    """
    from pipeline.spark import local_session

    spark = local_session("fleet-verify")
    one = lambda sql: spark.sql(sql).collect()[0][0]

    counts = {
        "raw_records": one(f"select count(*) from {schema}.{RAW_TABLE}"),
        "classified": one(f"select count(*) from {schema}.staging_fleet_trip_classified"),
        "staged_valid": one(f"select count(*) from {schema}.staging_fleet_trip"),
        "quarantined": one(f"select count(*) from {schema}.staging_fleet_trip_quarantine"),
        "distinct_events": one(f"select count(distinct event_id) from {schema}.staging_fleet_trip"),
        "canonical_trips": one(f"select count(*) from {schema}.canonical_trip"),
        "fact_trips": one(f"select count(*) from {schema}.fact_trip_enriched"),
        "mart_trips": one(f"select coalesce(sum(trips), 0) from {schema}.hourly_trip_activity"),
    }
    spark.stop()

    width = max(len(name) for name in counts)
    for name, value in counts.items():
        print(f"  {name.replace('_', ' '):<{width}}  {value:>12,}")
    if counts["staged_valid"] != counts["distinct_events"]:
        print(f"  {counts['staged_valid'] - counts['distinct_events']:,} repeated Kafka records "
              "collapse at canonical publication, not before.")

    if counts["quarantined"] != expect_quarantined:
        raise ValueError(
            f"quarantined {counts['quarantined']}, expected {expect_quarantined}")
    print("\n  reconciled")
    return counts
