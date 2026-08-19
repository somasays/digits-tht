import glob

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.validation import REQUIRED_COLUMNS, schema_problems, unknown_columns

# The schema every 2025 file carries, as verified against the real deliveries.
REAL_SCHEMA = pa.schema([
    ("VendorID", pa.int32()),
    ("tpep_pickup_datetime", pa.timestamp("us")),
    ("tpep_dropoff_datetime", pa.timestamp("us")),
    ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()),
    ("RatecodeID", pa.int64()),
    ("store_and_fwd_flag", pa.large_string()),
    ("PULocationID", pa.int32()),
    ("DOLocationID", pa.int32()),
    ("payment_type", pa.int64()),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    ("congestion_surcharge", pa.float64()),
    ("Airport_fee", pa.float64()),
    ("cbd_congestion_fee", pa.float64()),
])


def _without(schema, name):
    return pa.schema([f for f in schema if f.name != name])


def _retyped(schema, name, new_type):
    return pa.schema([pa.field(f.name, new_type) if f.name == name else f for f in schema])


def test_the_real_schema_satisfies_the_contract():
    assert schema_problems(REAL_SCHEMA) == []


@pytest.mark.parametrize("column", sorted(REQUIRED_COLUMNS))
def test_every_required_column_is_actually_required(column):
    problems = schema_problems(_without(REAL_SCHEMA, column))
    assert any(column in problem for problem in problems)


def test_an_optional_column_does_not_make_a_file_unreadable():
    assert schema_problems(_without(REAL_SCHEMA, "tip_amount")) == []


@pytest.mark.parametrize("column", ["tpep_pickup_datetime", "PULocationID"])
def test_a_required_column_arriving_as_text_is_refused(column):
    problems = schema_problems(_retyped(REAL_SCHEMA, column, pa.string()))
    assert any(column in problem for problem in problems)


@pytest.mark.parametrize("new_type", [pa.int8(), pa.int64(), pa.uint32()])
def test_integer_width_changes_are_accepted(new_type):
    # A location id is the same identifier whatever its width.
    assert schema_problems(_retyped(REAL_SCHEMA, "PULocationID", new_type)) == []


@pytest.mark.parametrize("new_type", [pa.int64(), pa.float32()])
def test_a_count_may_arrive_as_integer_or_float(new_type):
    assert schema_problems(_retyped(REAL_SCHEMA, "passenger_count", new_type)) == []


def test_a_renamed_required_column_is_refused():
    renamed = pa.schema([
        pa.field("pickup_datetime", f.type) if f.name == "tpep_pickup_datetime" else f
        for f in REAL_SCHEMA
    ])
    assert any("tpep_pickup_datetime" in p for p in schema_problems(renamed))


def test_an_added_column_is_reported_but_does_not_fail_the_file():
    # Backward compatible, so the run continues; silence would let the addition pass unnoticed.
    extended = pa.schema(list(REAL_SCHEMA) + [pa.field("new_surcharge", pa.float64())])
    assert schema_problems(extended) == []
    assert unknown_columns(extended) == ["new_surcharge"]


def test_the_real_schema_carries_nothing_unrecognised():
    assert unknown_columns(REAL_SCHEMA) == []


def test_a_renamed_column_both_fails_and_is_reported():
    # The old name is required and gone; the new one is unrecognised. Both facts are useful.
    renamed = pa.schema([
        pa.field("pickup_datetime", f.type) if f.name == "tpep_pickup_datetime" else f
        for f in REAL_SCHEMA
    ])
    assert any("tpep_pickup_datetime" in p for p in schema_problems(renamed))
    assert unknown_columns(renamed) == ["pickup_datetime"]


def test_contract_matches_every_real_file_on_disk():
    files = sorted(glob.glob("var/raw/tlc/yellow/*/*/*.parquet"))
    if not files:
        pytest.skip("no acquired TLC files present")
    for path in files:
        assert schema_problems(pq.ParquetFile(path).schema_arrow) == [], path
