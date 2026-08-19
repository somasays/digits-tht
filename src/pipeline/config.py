from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    taxi_civil_timezone: str
    weather_stations: dict[str, str]
    weather_fixed_utc_offset_hours: int
    boundary_tolerance_days: int
    max_rejected_fraction: float


def _require(mapping: dict, path: str) -> object:
    node = mapping
    for key in path.split("."):
        if not isinstance(node, dict) or key not in node:
            raise ConfigError(f"missing required config value: {path}")
        node = node[key]
    return node


def load_config(path: str | Path) -> Config:
    raw_text = Path(path).read_text()
    raw = yaml.safe_load(raw_text)
    if not isinstance(raw, dict):
        raise ConfigError(f"{path} did not parse to a mapping")

    civil_timezone = _require(raw, "taxi.civil_timezone")

    try:
        ZoneInfo(civil_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ConfigError(f"taxi.civil_timezone is not a known zone: {civil_timezone!r}") from exc

    stations = _require(raw, "weather.stations")
    if not isinstance(stations, dict) or not stations:
        raise ConfigError("weather.stations must be a non-empty mapping of station_key to station_id")
    for key, value in stations.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ConfigError("weather.stations must map string station_key to string station_id")

    offset_hours = _require(raw, "weather.fixed_utc_offset_hours")
    if not isinstance(offset_hours, int):
        raise ConfigError("weather.fixed_utc_offset_hours must be an integer")

    thresholds = _require(raw, "thresholds")
    if not isinstance(thresholds, dict):
        raise ConfigError("thresholds must be a mapping")
    tolerance = thresholds.get("boundary_tolerance_days")
    if not isinstance(tolerance, int) or tolerance < 0:
        raise ConfigError(
            "thresholds.boundary_tolerance_days must be a non-negative integer; it is frozen "
            "from measurement and must not be defaulted"
        )
    rejected = thresholds.get("max_rejected_fraction")
    if not isinstance(rejected, (int, float)) or isinstance(rejected, bool) \
            or not 0 <= rejected <= 1:
        raise ConfigError(
            "thresholds.max_rejected_fraction must be a fraction between 0 and 1; a quality "
            "bound must be declared, not defaulted"
        )

    return Config(
        taxi_civil_timezone=civil_timezone,
        weather_stations=stations,
        weather_fixed_utc_offset_hours=offset_hours,
        boundary_tolerance_days=tolerance,
        max_rejected_fraction=float(rejected),
    )


def period_range(start: str, end: str) -> list[str]:
    """Every YYYY-MM period from start to end inclusive."""
    start_year, start_month = (int(part) for part in start.split("-"))
    end_year, end_month = (int(part) for part in end.split("-"))
    periods = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month, year = 1, year + 1
    return periods
