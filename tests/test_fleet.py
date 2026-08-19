"""The fleet event contract: replay identity, resolved instants, and receipts.

No broker and no JVM. Everything with a branch in it is a function of its arguments, so
what a Kafka fixture would add here is coverage of kafka-python, not of this code. The
transport is exercised by `make streaming-demo`.
"""
import json

import pandas as pd
import pytest

from pipeline import fleet
from pipeline.dst import resolve_local_to_utc
from pipeline.validation import apply_rules

NY = "America/New_York"
CHECKSUM = "abc123"


def _frame(pickups, dropoffs=None, **extra):
    """A minimal TLC-shaped frame, matching what staging reads."""
    n = len(pickups)
    return pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(pickups),
        "tpep_dropoff_datetime": pd.to_datetime(dropoffs if dropoffs else pickups),
        "PULocationID": [161] * n,
        "DOLocationID": [237] * n,
        "passenger_count": [1] * n,
        "trip_distance": [2.5] * n,
        "fare_amount": [14.0] * n,
        "tip_amount": [3.0] * n,
        "total_amount": [21.5] * n,
        **extra,
    })


def _events(frame, period="2025-03"):
    resolved = apply_rules(frame, period, NY, 1)
    usable = resolved[resolved["quarantine_rule"].isna()]
    return list(fleet.trip_events(usable, CHECKSUM, period))


# --- identity ---------------------------------------------------------------------------

def test_replaying_the_same_rows_produces_the_same_events():
    """Re-replay must be a no-op downstream, which it can only be if ids are derived."""
    first = _events(_frame(["2025-03-15 12:00", "2025-03-16 08:30"]))
    second = _events(_frame(["2025-03-15 12:00", "2025-03-16 08:30"]))
    assert first == second
    assert len({event["event_id"] for event in first}) == 2


def test_vehicle_id_is_stable_and_not_process_salted():
    """hash() is salted per process, so a hashed vehicle would differ between the
    producer and any consumer that recomputed it. This pins the value itself."""
    assert fleet.vehicle_id(CHECKSUM, 0) == fleet.vehicle_id(CHECKSUM, 0)
    assert fleet.vehicle_id(CHECKSUM, 0).startswith("sim-vehicle-")
    # A literal, so a change of derivation is a failing test rather than silent drift.
    assert fleet.vehicle_id("abc123", 7) == "sim-vehicle-0154"


def test_events_declare_their_identity_as_synthetic():
    """TLC has no vehicle column; nothing downstream may mistake this for a real one."""
    event = _events(_frame(["2025-03-15 12:00"]))[0]
    assert event["vehicle_identity_type"] == "synthetic"
    assert event["source"] == "tlc_replay"


# --- instants ---------------------------------------------------------------------------

def test_every_instant_carries_an_explicit_zone():
    for event in _events(_frame(["2025-03-15 12:00"])):
        for field in ("pickup_at_utc", "dropoff_at_utc"):
            assert event[field].endswith("Z"), field


def test_event_utc_matches_the_resolver():
    """The producer owns UTC resolution; this is what lets dbt do none of it.

    A nonexistent local time is used deliberately: no real row in the replayed months
    falls in the gap, so without a synthetic one the shift branch would never be
    exercised anywhere in the fleet path.
    """
    local = ["2025-03-09 02:30:00"]
    expected = resolve_local_to_utc(pd.Series(pd.to_datetime(local)), NY)

    event = _events(_frame(local))[0]

    assert event["pickup_at_utc"] == expected["utc"][0].strftime("%Y-%m-%dT%H:%M:%SZ")
    # The resolver shifted it forward; the envelope has to say so, or the evidence that
    # this instant was repaired is lost at the first hop.
    assert expected["dst_shifted"][0]
    assert event["dst_shifted"] is True


def test_ordinary_times_are_not_flagged():
    event = _events(_frame(["2025-03-15 12:00"]))[0]
    assert event["dst_shifted"] is False
    assert event["dst_ambiguous"] is False


# --- fields the source does not guarantee ------------------------------------------------

def test_a_missing_passenger_count_is_carried_not_refused():
    """Roughly a fifth of real rows have none. Requiring it would quarantine them all."""
    frame = _frame(["2025-03-15 12:00"])
    frame["passenger_count"] = [None]
    event = _events(frame)[0]
    assert event["passenger_count"] is None


def test_events_are_json_serialisable():
    """pandas scalars survive itertuples and would fail at the serializer, not here."""
    for event in _events(_frame(["2025-03-15 12:00"])):
        json.loads(json.dumps(event))


# --- receipts ----------------------------------------------------------------------------

def test_receipt_id_is_derived_from_the_event():
    event = _events(_frame(["2025-03-15 12:00"]))[0]
    assert fleet.receipt_for(event)["receipt_id"] == fleet.receipt_for(event)["receipt_id"]


def test_receipt_id_ignores_a_mutated_payload():
    """Delivery is at-least-once. A redelivery that differs must still settle onto one
    receipt, so identity cannot depend on the amounts."""
    event = _events(_frame(["2025-03-15 12:00"]))[0]
    mutated = dict(event, total_amount=999.0, tip_amount=0.0)
    assert fleet.receipt_for(mutated)["receipt_id"] == fleet.receipt_for(event)["receipt_id"]


@pytest.mark.parametrize("raw", fleet.INVALID_FIXTURES)
def test_no_receipt_for_an_unreceiptable_event(raw):
    """The service reads whatever is on the topic, including these."""
    try:
        event = json.loads(raw)
    except ValueError:
        return  # unparseable never reaches receipt_for
    assert fleet.receipt_for(event) is None


def test_a_payload_that_is_not_an_object_owes_nothing():
    assert fleet.receipt_for(None) is None
    assert fleet.receipt_for(5) is None


def test_a_receipt_names_the_event_that_caused_it():
    event = _events(_frame(["2025-03-15 12:00"]))[0]
    receipt = fleet.receipt_for(event)
    assert receipt["causation_event_id"] == event["event_id"]
    assert receipt["vehicle_id"] == event["vehicle_id"]


# --- the isolation claim -----------------------------------------------------------------

def test_the_receipt_path_does_not_import_spark():
    """Receipts must keep being issued while the analytical stack is down.

    That is an architectural claim, and the cheapest way for it to quietly stop being
    true is a module-level import somewhere on the path. Run in a subprocess because
    pyspark is certainly already imported in this one.
    """
    import subprocess
    import sys
    import textwrap

    probe = textwrap.dedent("""
        import sys
        from pipeline import fleet
        from pipeline.cli import build_parser
        # Name what has to stay Spark-free, not just that something is. This assertion
        # went on passing once when the service it describes had been deleted, because
        # a module with no receipt path trivially has no Spark on it.
        assert callable(fleet.receipts), "the receipt service is gone"
        assert callable(fleet.receipt_for), "the receipt contract is gone"
        build_parser().parse_args(["fleet", "receipts"])
        assert "pyspark" not in sys.modules, "Spark reached the receipt path"
        assert "delta" not in sys.modules, "Delta reached the receipt path"
    """)
    subprocess.run([sys.executable, "-c", probe], check=True)
