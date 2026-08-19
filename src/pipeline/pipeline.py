from __future__ import annotations

import logging
from pathlib import Path

from pipeline.acquisition.noaa import acquire_station_years
from pipeline.acquisition.tlc import acquire_periods
from pipeline.config import Config, period_range
from pipeline.staging import check_readable, stage_weather

logger = logging.getLogger("pipeline")


def run_pipeline(
    spark,
    config: Config,
    raw_root: Path,
    staging_schema: str,
    period_start: str,
    period_end: str,
) -> None:
    """Acquire and publish one inclusive period range.

    Trips are acquired but not staged: the replay reads the files and publishes them to
    Kafka, and dbt stages what comes back. Weather has no streaming path and is staged here.
    """
    raw_root = Path(raw_root)
    periods = period_range(period_start, period_end)
    if not periods:
        raise ValueError(f"empty period range: {period_start}..{period_end}")
    years = sorted({period.split("-")[0] for period in periods})
    stations = config.weather_stations
    weather_labels = [(station, year) for year in years for station in stations]

    trips = acquire_periods(periods, raw_root)
    for period, acquired in zip(periods, trips):
        logger.info(
            "tlc %s: %s checksum=%s rows=%s bytes=%s",
            period, acquired.disposition, acquired.checksum_sha256[:12],
            acquired.row_count, acquired.file_size_bytes,
        )

    weather = acquire_station_years(stations, years, raw_root)
    for (station, year), acquired in zip(weather_labels, weather):
        logger.info(
            "noaa %s %s: %s checksum=%s rows=%s bytes=%s",
            station, year, acquired.disposition, acquired.checksum_sha256[:12],
            acquired.row_count, acquired.file_size_bytes,
        )

    by_period = dict(zip(periods, trips))
    # Refuse the full request before publishing any period if one file is unreadable.
    check_readable({period: acquired.raw_path for period, acquired in by_period.items()})

    for (station, year), acquired in zip(weather_labels, weather):
        stage_weather(
            spark, station, year, acquired.raw_path, acquired.checksum_sha256,
            staging_schema, offset_hours=config.weather_fixed_utc_offset_hours,
        )
