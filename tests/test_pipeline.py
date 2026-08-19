from pathlib import Path

import pytest

from pipeline.acquisition.download import AcquiredFile
from pipeline.config import Config
from pipeline.config import period_range
from pipeline.pipeline import run_pipeline


def _acquired(name):
    return AcquiredFile("checksum", "acquired", Path(name), 10, 1)


def test_period_range_crosses_a_year_boundary():
    assert period_range("2024-12", "2025-02") == ["2024-12", "2025-01", "2025-02"]


def test_run_pipeline_preserves_source_and_staging_order(monkeypatch, tmp_path):
    trips = [_acquired("jan.parquet"), _acquired("feb.parquet")]
    weather = [_acquired("cp.csv"), _acquired("jfk.csv")]
    calls = []

    monkeypatch.setattr(
        "pipeline.pipeline.acquire_periods",
        lambda periods, root: calls.append(("acquire_trips", periods, root)) or trips,
    )
    monkeypatch.setattr(
        "pipeline.pipeline.acquire_station_years",
        lambda stations, years, root: calls.append(("acquire_weather", stations, years, root))
        or weather,
    )
    monkeypatch.setattr(
        "pipeline.pipeline.check_readable",
        lambda paths: calls.append(("check", paths)),
    )
    monkeypatch.setattr(
        "pipeline.pipeline.stage_weather",
        lambda *args, **kwargs: calls.append(("stage_weather", args[1], args[2], kwargs)),
    )

    config = Config(
        taxi_civil_timezone="America/New_York",
        weather_stations={"central_park": "cp", "jfk": "jfk"},
        weather_fixed_utc_offset_hours=-5,
        boundary_tolerance_days=1,
        max_rejected_fraction=0.01,
    )
    run_pipeline(object(), config, tmp_path, "catalog.staging", "2025-01", "2025-02")

    # Trips are acquired and checked but not staged: the replay reads the files and
    # publishes them, and dbt stages what comes back off the topic.
    assert [call[0] for call in calls] == [
        "acquire_trips", "acquire_weather", "check",
        "stage_weather", "stage_weather",
    ]
    assert calls[0][1] == ["2025-01", "2025-02"]
    assert calls[1][2] == ["2025"]
    assert [call[1:3] for call in calls[-2:]] == [
        ("central_park", "2025"), ("jfk", "2025")
    ]


def test_run_pipeline_rejects_an_inverted_period_range(tmp_path):
    config = Config(
        taxi_civil_timezone="America/New_York",
        weather_stations={"central_park": "cp"},
        weather_fixed_utc_offset_hours=-5,
        boundary_tolerance_days=1,
        max_rejected_fraction=0.01,
    )
    with pytest.raises(ValueError, match="empty period range"):
        run_pipeline(object(), config, tmp_path, "catalog.staging", "2025-04", "2025-01")
