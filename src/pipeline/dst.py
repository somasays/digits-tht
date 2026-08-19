"""Resolution of naive local wall-clock timestamps to UTC instants.

One local time can map to no instant (spring transition) or two (autumn), and either repair
lands on an instant a genuine local time also produces, so the branch taken is reported too.
"""
from __future__ import annotations

import pandas as pd


def resolve_local_to_utc(values: pd.Series, tz: str) -> pd.DataFrame:
    """Resolve naive local wall-clock values in `tz` to UTC instants.

    Returns utc, dst_shifted, dst_ambiguous and parse_failed, aligned to `values`.
    Nonexistent times move forward to the first valid wall-clock time; ambiguous times take
    the earlier instant. Spark SQL resolves both silently, which is why this lives here (§8.1).
    """
    parsed = pd.to_datetime(values, errors="coerce")
    parse_failed = parsed.isna()

    if getattr(parsed.dtype, "tz", None) is not None:
        raise ValueError("values must be naive local timestamps, not tz-aware")

    index = pd.DatetimeIndex(parsed)
    resolved = index.tz_localize(tz, nonexistent="shift_forward", ambiguous=True)

    # Asking for NaT on one branch marks the rows that took it, so the flags follow the
    # resolver's own behaviour rather than restating the zone's rules.
    shifted = index.tz_localize(tz, nonexistent="NaT", ambiguous=True).isna()
    ambiguous = index.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT").isna()

    return pd.DataFrame(
        {
            "utc": pd.Series(resolved, index=values.index).dt.tz_convert("UTC"),
            "dst_shifted": pd.Series(shifted, index=values.index) & ~parse_failed,
            "dst_ambiguous": pd.Series(ambiguous, index=values.index) & ~parse_failed,
            "parse_failed": parse_failed,
        },
        index=values.index,
    )
