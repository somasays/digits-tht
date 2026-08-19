import copy
from pathlib import Path

import pytest
import yaml

from pipeline.config import ConfigError, load_config

REPO_CONFIG = Path(__file__).parent.parent / "config" / "config.yaml"

# The shape every case starts from; each failing case below breaks exactly one thing in it.
VALID = {
    "taxi": {
        "civil_timezone": "America/New_York",
    },
    "weather": {
        "stations": {"central_park": "72505394728", "jfk": "74486094789"},
        "fixed_utc_offset_hours": -5,
    },
    "thresholds": {"boundary_tolerance_days": 1, "max_rejected_fraction": 0.01},
}


def _written(tmp_path, mutate=None):
    raw = copy.deepcopy(VALID)
    if mutate:
        mutate(raw)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_the_shipped_config_is_valid():
    """The versioned file the pipeline actually runs on, not a copy of it."""
    config = load_config(REPO_CONFIG)
    assert config.taxi_civil_timezone == "America/New_York"
    assert set(config.weather_stations) == {"central_park", "laguardia", "jfk"}
    assert config.weather_fixed_utc_offset_hours == -5
    assert config.boundary_tolerance_days == 1
    assert config.max_rejected_fraction == 0.01


def test_a_valid_config_loads(tmp_path):
    config = load_config(_written(tmp_path))
    assert config.weather_stations == {"central_park": "72505394728", "jfk": "74486094789"}


@pytest.mark.parametrize("mutate, expected", [
    (lambda c: c["taxi"].pop("civil_timezone"), "taxi.civil_timezone"),
    (lambda c: c.update(weather={"stations": {}, "fixed_utc_offset_hours": -5}),
     "weather.stations"),
    (lambda c: c.update(thresholds=[1, 2]), "thresholds must be a mapping"),
    # Both thresholds are measured once and pinned; defaulting either would let a rerun
    # silently reclassify rows.
    (lambda c: c["thresholds"].pop("boundary_tolerance_days"), "boundary_tolerance_days"),
    (lambda c: c["thresholds"].pop("max_rejected_fraction"), "max_rejected_fraction"),
])
def test_an_invalid_config_is_refused(tmp_path, mutate, expected):
    with pytest.raises(ConfigError, match=expected):
        load_config(_written(tmp_path, mutate))
