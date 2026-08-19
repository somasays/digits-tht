from datetime import datetime, timezone

import pandas as pd
import pytest

from pipeline.dst import resolve_local_to_utc

NY = "America/New_York"

# 2025 US Eastern transitions: clocks jump 02:00 -> 03:00 on 9 March (02:00-02:59 never
# occurs), and 02:00 -> 01:00 on 2 November (01:00-01:59 occurs twice).


def _resolve(values):
    return resolve_local_to_utc(pd.Series(pd.to_datetime(values)), NY)


def _utc(text):
    """Expected instants written out directly, not computed through the code under test."""
    return pd.Timestamp(text, tz="UTC")


@pytest.mark.parametrize(
    "local, expected_utc",
    [
        # Winter, EST = UTC-5.
        ("2025-01-15 12:00:00", "2025-01-15 17:00:00"),
        ("2025-03-09 01:30:00", "2025-03-09 06:30:00"),
        # Summer, EDT = UTC-4.
        ("2025-06-15 12:00:00", "2025-06-15 16:00:00"),
        ("2025-03-09 03:30:00", "2025-03-09 07:30:00"),
        # Either side of the autumn transition, unambiguous.
        ("2025-11-02 00:30:00", "2025-11-02 04:30:00"),
        ("2025-11-02 03:30:00", "2025-11-02 08:30:00"),
    ],
)
def test_normal_times_use_the_offset_in_force(local, expected_utc):
    result = _resolve([local])
    assert result["utc"].iloc[0] == _utc(expected_utc)
    assert not result["dst_shifted"].iloc[0]
    assert not result["dst_ambiguous"].iloc[0]


def test_offsets_differ_across_the_spring_transition():
    # The same clock reading maps to a different instant depending on the season, which is the
    # whole reason the source cannot be read as UTC.
    winter = _resolve(["2025-01-15 12:00:00"])["utc"].iloc[0]
    summer = _resolve(["2025-06-15 12:00:00"])["utc"].iloc[0]
    assert winter.hour == 17
    assert summer.hour == 16


@pytest.mark.parametrize("local", ["2025-03-09 02:00:00", "2025-03-09 02:30:00",
                                   "2025-03-09 02:59:00"])
def test_nonexistent_spring_times_are_shifted_forward_and_flagged(local):
    result = _resolve([local])
    assert result["dst_shifted"].iloc[0]
    assert not result["dst_ambiguous"].iloc[0]
    # Moved to the first valid wall-clock time, 03:00 EDT = 07:00Z.
    assert result["utc"].iloc[0] == _utc("2025-03-09 07:00:00")


def test_boundary_minutes_around_the_spring_gap_are_not_flagged():
    before = _resolve(["2025-03-09 01:59:00"])
    after = _resolve(["2025-03-09 03:00:00"])
    assert not before["dst_shifted"].iloc[0]
    assert not after["dst_shifted"].iloc[0]
    assert before["utc"].iloc[0] == _utc("2025-03-09 06:59:00")
    assert after["utc"].iloc[0] == _utc("2025-03-09 07:00:00")


def test_a_repaired_time_collides_with_a_genuine_one_so_only_the_flag_separates_them():
    # 02:30 never occurred and is repaired to 03:00; a genuine 03:00 resolves to the same
    # instant. After conversion the values are identical, so the flag is the only surviving
    # evidence -- which is precisely what SQL cannot produce.
    result = _resolve(["2025-03-09 02:30:00", "2025-03-09 03:00:00"])
    assert result["utc"].iloc[0] == result["utc"].iloc[1]
    assert result["dst_shifted"].tolist() == [True, False]


@pytest.mark.parametrize(
    "local, expected_utc",
    [
        # The earlier occurrence is EDT (UTC-4); the later one is an hour further on.
        ("2025-11-02 01:00:00", "2025-11-02 05:00:00"),
        ("2025-11-02 01:30:00", "2025-11-02 05:30:00"),
        ("2025-11-02 01:59:00", "2025-11-02 05:59:00"),
    ],
)
def test_ambiguous_autumn_times_take_the_earlier_instant_and_are_flagged(local, expected_utc):
    result = _resolve([local])
    assert result["dst_ambiguous"].iloc[0]
    assert not result["dst_shifted"].iloc[0]
    assert result["utc"].iloc[0] == _utc(expected_utc)


def test_even_pandas_constructor_refuses_to_resolve_an_ambiguous_time():
    # Guessing is not available anywhere in the stack: a policy has to be declared. This is
    # the behaviour Spark SQL lacks, which is why resolution lives here.
    with pytest.raises(ValueError, match="Cannot infer dst time"):
        pd.Timestamp("2025-11-02 01:30:00", tz=NY)


def test_both_readings_of_the_repeated_hour_exist_and_the_earlier_is_chosen():
    # 01:30 corresponds to two real instants an hour apart. Resolution must pick one, and the
    # flag records that a choice was made.
    resolved = _resolve(["2025-11-02 01:30:00"])["utc"].iloc[0]
    earlier = _utc("2025-11-02 05:30:00")
    later = _utc("2025-11-02 06:30:00")
    assert (later - earlier) == pd.Timedelta(hours=1)
    assert resolved == earlier


def test_a_trip_spanning_the_spring_transition_keeps_a_positive_duration():
    result = _resolve(["2025-03-09 01:45:00", "2025-03-09 03:15:00"])
    duration = result["utc"].iloc[1] - result["utc"].iloc[0]
    # 90 minutes of clock time, but only 30 minutes actually elapsed.
    assert duration == pd.Timedelta(minutes=30)


def test_a_trip_spanning_the_autumn_transition_keeps_a_positive_duration():
    result = _resolve(["2025-11-02 01:30:00", "2025-11-02 03:00:00"])
    duration = result["utc"].iloc[1] - result["utc"].iloc[0]
    # The repeated hour makes the elapsed time longer than the clock suggests.
    assert duration == pd.Timedelta(hours=2, minutes=30)


def test_missing_and_unparseable_values_are_reported_without_flags():
    result = resolve_local_to_utc(pd.Series([None, "not a timestamp", "2025-01-15 12:00:00"]), NY)
    assert result["parse_failed"].tolist() == [True, True, False]
    assert pd.isna(result["utc"].iloc[0])
    assert pd.isna(result["utc"].iloc[1])
    # An absent value is not a daylight-saving case.
    assert not result["dst_shifted"].any()
    assert not result["dst_ambiguous"].any()


def test_result_is_aligned_to_the_input_index():
    values = pd.Series(pd.to_datetime(["2025-01-15 12:00:00", "2025-06-15 12:00:00"]),
                       index=[10, 20])
    result = resolve_local_to_utc(values, NY)
    assert result.index.tolist() == [10, 20]


def test_tz_aware_input_is_rejected():
    aware = pd.Series(pd.to_datetime(["2025-01-15 12:00:00"]).tz_localize("UTC"))
    with pytest.raises(ValueError, match="naive"):
        resolve_local_to_utc(aware, NY)


def test_resolved_instants_match_independently_calculated_values():
    # Computed with the standard library rather than the pandas path under test.
    from zoneinfo import ZoneInfo

    for local in ["2025-01-15 12:00:00", "2025-06-15 12:00:00", "2025-11-02 03:30:00"]:
        naive = datetime.fromisoformat(local)
        expected = naive.replace(tzinfo=ZoneInfo(NY)).astimezone(timezone.utc)
        actual = _resolve([local])["utc"].iloc[0]
        assert actual.to_pydatetime() == expected
