from __future__ import annotations

import csv
from pathlib import Path

from pipeline.acquisition.download import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_CONCURRENT,
    AcquiredFile,
    AcquisitionError,
    FileSpec,
    acquire_files,
)

NOAA_LCD_URL_TEMPLATE = (
    "https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/{station_id}.csv"
)

# STATION and DATE carry source identity and the timestamp resolved to UTC; REPORT_TYPE
# drives observation selection; HourlyPrecipitation is the measure the analysis depends on.
# A delivery missing any of these is unusable downstream, so promotion is blocked.
REQUIRED_COLUMNS = ("STATION", "DATE", "REPORT_TYPE", "HourlyPrecipitation")

__all__ = ["AcquisitionError", "acquire_station_years"]


def acquire_station_years(
    stations: dict[str, str],
    years: list[str],
    raw_root: Path,
    *,
    urls: dict[tuple[str, str], str] | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
) -> list[AcquiredFile]:
    """Acquire the yearly LCD delivery for each station, for each requested year."""
    raw_root = Path(raw_root)
    urls = urls or {}
    specs = []
    for year in years:
        for station_key, station_id in stations.items():
            source_url = urls.get((station_key, year)) or NOAA_LCD_URL_TEMPLATE.format(
                year=year, station_id=station_id
            )
            specs.append(
                FileSpec(
                    source_url=source_url,
                    destination_prefix=raw_root / "noaa" / "lcd" / station_key / year,
                    validate=_validate_csv,
                    manifest_extra={
                        "station_key": station_key,
                        "station_id": station_id,
                        "year": year,
                        "delivery_format": "csv",
                    },
                )
            )
    return acquire_files(
        specs, raw_root, max_attempts=max_attempts, max_concurrent=max_concurrent
    )

def _validate_csv(path: Path, content_type: str | None) -> int:
    if content_type is None or "csv" not in content_type.lower():
        raise AcquisitionError(
            f"unexpected content type for {path.name}: {content_type!r} (expected text/csv)"
        )

    with open(path, "r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise AcquisitionError(f"downloaded file has no header row: {path.name}") from None
        missing = [column for column in REQUIRED_COLUMNS if column not in header]
        if missing:
            raise AcquisitionError(
                f"downloaded file {path.name} is missing required columns {missing}; "
                f"found: {header}"
            )
        return sum(1 for _ in reader)
