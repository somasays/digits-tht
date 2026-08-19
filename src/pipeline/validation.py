"""The TLC trip source contract and the trip row rules.

A renamed or retyped column read without checking becomes a column of nulls or a silent
coercion, so the schema is inspected before any row is interpreted.
"""
from __future__ import annotations

import pandas as pd
import pyarrow as pa

from pipeline.dst import resolve_local_to_utc

# Columns without which a trip cannot be interpreted. Their absence fails the file.
REQUIRED_COLUMNS: dict[str, str] = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "PULocationID": "integer",
    "DOLocationID": "integer",
    "passenger_count": "number",
    "trip_distance": "number",
    "fare_amount": "number",
    "total_amount": "number",
}

# Columns the source is known to send. A file missing one is still readable; a file carrying
# something outside this set is reported so the addition is noticed rather than absorbed.
KNOWN_COLUMNS: frozenset[str] = frozenset(REQUIRED_COLUMNS) | {
    "VendorID",
    "RatecodeID",
    "store_and_fwd_flag",
    "payment_type",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
}


def _matches(kind: str, arrow_type: pa.DataType) -> bool:
    if kind == "timestamp":
        return pa.types.is_timestamp(arrow_type)
    if kind == "integer":
        return pa.types.is_integer(arrow_type)
    # Counts and money arrive as either integers or floats across TLC's history.
    return pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type)


def schema_problems(schema: pa.Schema) -> list[str]:
    """Reasons this file cannot be read under the contract. Empty means it can."""
    present = {field.name: field.type for field in schema}
    problems = []
    for name, kind in REQUIRED_COLUMNS.items():
        if name not in present:
            problems.append(f"missing required column {name}")
        elif not _matches(kind, present[name]):
            problems.append(f"{name} is {present[name]}, expected {kind}")
    return problems


def unknown_columns(schema: pa.Schema) -> list[str]:
    """Columns the contract does not describe. Backward compatible, so reported not refused."""
    return sorted(field.name for field in schema if field.name not in KNOWN_COLUMNS)


# Stable identifiers, recorded against every quarantined row.
RULE_TIMESTAMP_UNUSABLE = "trip.timestamp_unusable"
RULE_PERIOD_OUT_OF_RANGE = "trip.period_out_of_range"


def apply_rules(frame: pd.DataFrame, source_period: str, tz: str,
                boundary_tolerance_days: int) -> pd.DataFrame:
    """Resolve timestamps and classify each row.

    Adds pickup_utc, dropoff_utc, the three technical flags, and quarantine_rule, which is
    null for rows that can be staged. A row is refused only when it cannot be read: an
    unusable timestamp, or a pickup so far outside the period that the file cannot be
    describing it. A pickup just outside the period is a real trip that began before
    midnight, so it is kept and flagged.
    """
    pickup = resolve_local_to_utc(frame["tpep_pickup_datetime"], tz)
    dropoff = resolve_local_to_utc(frame["tpep_dropoff_datetime"], tz)

    start = pd.Timestamp(f"{source_period}-01")
    end = start + pd.offsets.MonthBegin(1)
    tolerance = pd.Timedelta(days=boundary_tolerance_days)
    local_pickup = frame["tpep_pickup_datetime"]

    outside = (local_pickup < start) | (local_pickup >= end)
    beyond_tolerance = (local_pickup < start - tolerance) | (local_pickup >= end + tolerance)

    unusable = pickup["parse_failed"] | dropoff["parse_failed"]

    quarantine_rule = pd.Series(pd.NA, index=frame.index, dtype="object")
    quarantine_rule[beyond_tolerance] = RULE_PERIOD_OUT_OF_RANGE
    quarantine_rule[unusable] = RULE_TIMESTAMP_UNUSABLE

    return frame.assign(
        pickup_utc=pickup["utc"],
        dropoff_utc=dropoff["utc"],
        dst_shifted=pickup["dst_shifted"] | dropoff["dst_shifted"],
        dst_ambiguous=pickup["dst_ambiguous"] | dropoff["dst_ambiguous"],
        boundary_straggler=outside & ~beyond_tolerance & ~unusable,
        quarantine_rule=quarantine_rule,
    )
