import pandas as pd
import pytest

from pipeline.validation import (
    RULE_PERIOD_OUT_OF_RANGE,
    RULE_TIMESTAMP_UNUSABLE,
    apply_rules,
)

NY = "America/New_York"
PERIOD = "2025-03"
TOLERANCE = 1


def _rows(pickups, dropoffs=None, **extra):
    frame = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(pickups),
        "tpep_dropoff_datetime": pd.to_datetime(dropoffs if dropoffs else pickups),
    })
    for name, values in extra.items():
        frame[name] = values
    return apply_rules(frame, PERIOD, NY, TOLERANCE)


def test_an_ordinary_trip_is_accepted_and_unflagged():
    result = _rows(["2025-03-15 12:00:00"])
    assert pd.isna(result["quarantine_rule"].iloc[0])
    assert not result[["dst_shifted", "dst_ambiguous", "boundary_straggler"]].iloc[0].any()
    assert result["pickup_utc"].iloc[0] == pd.Timestamp("2025-03-15 16:00:00", tz="UTC")


def test_a_missing_timestamp_is_quarantined():
    # The contract guarantees a timestamp column, so "unusable" means absent rather than
    # malformed; a string could never reach here.
    result = _rows([None])
    assert result["quarantine_rule"].iloc[0] == RULE_TIMESTAMP_UNUSABLE


def test_an_unusable_dropoff_also_quarantines_the_row():
    # Duration cannot be derived from half a trip.
    result = _rows(["2025-03-15 12:00:00"], [None])
    assert result["quarantine_rule"].iloc[0] == RULE_TIMESTAMP_UNUSABLE


@pytest.mark.parametrize("pickup", ["2007-12-05 18:45:00", "2009-01-01 00:19:34"])
def test_the_real_gross_violations_are_quarantined(pickup):
    # The two rows the March file actually contains.
    result = _rows([pickup])
    assert result["quarantine_rule"].iloc[0] == RULE_PERIOD_OUT_OF_RANGE


def test_a_trip_starting_just_before_the_month_is_kept_and_flagged():
    # A real trip that began before midnight; quarantining it would discard revenue.
    result = _rows(["2025-02-28 23:50:00"])
    assert pd.isna(result["quarantine_rule"].iloc[0])
    assert result["boundary_straggler"].iloc[0]


def test_a_trip_starting_just_after_the_month_is_kept_and_flagged():
    result = _rows(["2025-04-01 00:00:17"])
    assert pd.isna(result["quarantine_rule"].iloc[0])
    assert result["boundary_straggler"].iloc[0]


def test_the_tolerance_boundary_separates_flagging_from_quarantine():
    inside = _rows(["2025-02-28 12:00:00"])          # ~12h before, within a day
    outside = _rows(["2025-02-27 12:00:00"])         # ~36h before, beyond a day
    assert inside["boundary_straggler"].iloc[0]
    assert pd.isna(inside["quarantine_rule"].iloc[0])
    assert outside["quarantine_rule"].iloc[0] == RULE_PERIOD_OUT_OF_RANGE
    assert not outside["boundary_straggler"].iloc[0]


def test_a_pickup_inside_the_month_is_never_a_straggler():
    result = _rows(["2025-03-01 00:00:00", "2025-03-31 23:59:59"])
    assert not result["boundary_straggler"].any()


def test_the_spring_gap_is_flagged_but_still_accepted():
    result = _rows(["2025-03-09 02:30:00"])
    assert pd.isna(result["quarantine_rule"].iloc[0])
    assert result["dst_shifted"].iloc[0]
    assert not result["dst_ambiguous"].iloc[0]


def test_the_repeated_autumn_hour_is_flagged_but_still_accepted():
    # Belongs to a different period: a November pickup in a March file is out of range,
    # which is itself the rule working.
    frame = pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime(["2025-11-02 01:30:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2025-11-02 01:45:00"]),
    })
    result = apply_rules(frame, "2025-11", NY, TOLERANCE)
    assert pd.isna(result["quarantine_rule"].iloc[0])
    assert result["dst_ambiguous"].iloc[0]
    assert not result["dst_shifted"].iloc[0]


def test_a_flag_on_either_endpoint_marks_the_row():
    # The gap is crossed by the dropoff, not the pickup.
    result = _rows(["2025-03-09 01:30:00"], ["2025-03-09 02:30:00"])
    assert result["dst_shifted"].iloc[0]


def test_business_anomalies_are_not_this_stage_s_concern():
    # Negative fares and unknown zones are questions about whether a trip is reasonable,
    # not whether it is readable. dbt flags them; staging keeps them untouched.
    result = _rows(["2025-03-15 12:00:00"] * 2, fare_amount=[-50.0, 10.0],
                   PULocationID=[264, 132])
    assert result["quarantine_rule"].isna().all()
    assert not result["boundary_straggler"].any()
    assert result["fare_amount"].tolist() == [-50.0, 10.0]


def test_an_unusable_timestamp_outranks_an_out_of_range_one():
    # Both conditions hold; the row is reported under the reason that stopped it first.
    result = _rows([None])
    assert result["quarantine_rule"].iloc[0] == RULE_TIMESTAMP_UNUSABLE


def test_source_columns_survive_unchanged():
    result = _rows(["2025-03-15 12:00:00"], VendorID=[2], total_amount=[31.4])
    assert result["VendorID"].iloc[0] == 2
    assert result["total_amount"].iloc[0] == 31.4
