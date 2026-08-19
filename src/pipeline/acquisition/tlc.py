from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from pipeline.acquisition.download import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_CONCURRENT,
    AcquiredFile,
    AcquisitionError,
    FileSpec,
    acquire_files,
)

TLC_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{period}.parquet"

__all__ = ["AcquisitionError", "acquire_periods"]


def acquire_periods(
    periods: list[str],
    raw_root: Path,
    *,
    urls: dict[str, str] | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
) -> list[AcquiredFile]:
    """Acquire the monthly TLC yellow taxi file for each YYYY-MM period."""
    raw_root = Path(raw_root)
    urls = urls or {}
    specs = [
        FileSpec(
            source_url=urls.get(period) or TLC_URL_TEMPLATE.format(period=period),
            destination_prefix=raw_root / "tlc" / "yellow" / period,
            validate=_validate_parquet,
            manifest_extra={"period": period},
        )
        for period in periods
    ]
    return acquire_files(
        specs, raw_root, max_attempts=max_attempts, max_concurrent=max_concurrent
    )

def _validate_parquet(path: Path, content_type: str | None) -> int:
    try:
        return pq.ParquetFile(path).metadata.num_rows
    except Exception as exc:
        raise AcquisitionError(f"downloaded file is not valid parquet: {path.name}: {exc}") from exc
