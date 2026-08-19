"""Publication of weather observations into Delta staging.

Trips no longer land here: they arrive over Kafka and are staged by dbt. What remains is
the weather the enriched fact joins, and the source contract check the replay producer
calls before it publishes anything.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, isnan, to_timestamp, when

from pipeline.validation import apply_rules, schema_problems, unknown_columns

logger = logging.getLogger("pipeline")

WEATHER_TABLE = "staging_weather_hour"

OBSERVATION_COLUMNS = ["DATE", "REPORT_TYPE", "HourlyPrecipitation",
                       "HourlyDryBulbTemperature", "HourlyRelativeHumidity"]

class StagingError(RuntimeError):
    """Raised when a period cannot be staged, or when rows would be lost by staging it."""


def check_readable(raw_paths: dict[str, Path]) -> None:
    """Refuse the whole request unless every period can be read.

    Only the footer is touched, so checking all before writing any keeps a partially
    populated range unreachable (§21.3).
    """
    for period, path in sorted(raw_paths.items()):
        schema = pq.ParquetFile(path).schema_arrow
        problems = schema_problems(schema)
        if problems:
            raise StagingError(f"{period}: {'; '.join(problems)}")
        unknown = unknown_columns(schema)
        if unknown:
            logger.info("tlc %s: unrecognised columns %s", period, unknown)


def stage_weather(
    spark: SparkSession,
    station_key: str,
    year: str,
    csv_path: Path,
    checksum: str,
    staging_schema: str,
    *,
    offset_hours: int,
) -> None:
    """Select one routine observation per UTC hour and replace that station-year."""
    try:
        observations = pd.read_csv(csv_path, dtype=str, usecols=OBSERVATION_COLUMNS)
    except ValueError as exc:
        # usecols names the columns it could not find, which is the contract check.
        raise StagingError(f"{station_key} {year}: {exc}") from exc
    hourly = observations[observations.REPORT_TYPE.str.strip() == "FM-15"].copy()
    if hourly.empty:
        # An empty table takes this station's enrichment with it, silently.
        raise StagingError(f"{station_key} {year}: no FM-15 observations in {csv_path.name}")

    # DATE is Local Standard Time all year: a constant shift, not a zone lookup.
    utc = pd.to_datetime(hourly.DATE) - pd.Timedelta(hours=offset_hours)
    # Text, because createDataFrame reinterprets naive timestamps in the host zone (§8.3).
    hourly["utc_hour"] = utc.dt.floor("h").dt.strftime("%Y-%m-%d %H:%M:%S")
    # The later report holds the accumulation; sorting keeps the choice file-order independent.
    hourly = hourly.sort_values("DATE").drop_duplicates("utc_hour", keep="last")

    # Kept as text too: a trace and an absent reading are unmeasured, and neither is dry.
    precip_raw = hourly.HourlyPrecipitation.fillna("").str.strip()
    # NOAA suffixes a suspect reading with "s". Coercing it to NaN deleted real rainfall and
    # left the hour looking dry, so the value is kept and the doubt flagged.
    suspect = precip_raw.str.endswith("s")
    precip_in = pd.to_numeric(
        precip_raw.str.removesuffix("s").replace("T", "0"), errors="coerce"
    )
    # Neither blank, trace, nor number: report rather than pass as an unmeasured hour.
    unparsed = precip_raw[precip_in.isna() & (precip_raw != "")].unique()
    if len(unparsed):
        logger.warning("weather %s %s: unparsed precipitation values %s",
                       station_key, year, sorted(unparsed)[:10])

    rows = pd.DataFrame({
        "station_key": station_key,
        "utc_hour": hourly.utc_hour,
        "precip_in": precip_in,
        "precip_raw": precip_raw,
        "precip_suspect": suspect,
        "temperature_f": pd.to_numeric(hourly.HourlyDryBulbTemperature, errors="coerce"),
        "relative_humidity": pd.to_numeric(hourly.HourlyRelativeHumidity, errors="coerce"),
        "_source_period": year,
        "_source_checksum": checksum,
    })

    frame = (
        spark.createDataFrame(rows)
        .withColumn("utc_hour", to_timestamp("utc_hour"))
        # An unmeasured reading arrives from pandas as NaN, which IS NULL does not match.
        .withColumn("precip_in", when(~isnan("precip_in"), col("precip_in")))
        .withColumn("_ingested_at_utc", current_timestamp())
    )
    _replace(frame, f"{staging_schema}.{WEATHER_TABLE}",
             f"_source_period = '{year}' AND station_key = '{station_key}'")
    # Different gaps: a full file with blank precipitation is still a hole.
    measured = int((precip_raw != "").sum())
    logger.info("staged weather %s %s: %s hours, %s with precipitation (%.1f%%), %s suspect",
                station_key, year, len(rows), measured,
                100.0 * measured / len(rows), int(suspect.sum()))


def _replace(frame: DataFrame, table: str, predicate: str) -> None:
    """Replace exactly the rows the predicate selects, leaving the rest untouched.

    The predicate differs by source: a weather year is shared by every station, so a
    year-only predicate would delete the others. Delta rejects rows falling outside it, so a
    mislabelled row fails the write instead of landing elsewhere. REPLACE WHERE rather than
    the writer option, which a named table rejects as "truncate in batch mode".
    """
    spark = frame.sparkSession
    # Every staging write routes through here, so this is the one place that has to know
    # the schema might not exist yet. A fresh metastore has nothing in it.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    if not spark.catalog.tableExists(table):
        frame.limit(0).write.format("delta").saveAsTable(table)
    frame.createOrReplaceTempView("incoming")
    spark.sql(f"INSERT INTO {table} REPLACE WHERE {predicate} SELECT * FROM incoming")
