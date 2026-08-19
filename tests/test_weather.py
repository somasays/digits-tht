"""Weather staging tests. Spark tier: deselect with -m 'not spark'."""
import pytest

from pipeline.staging import WEATHER_TABLE, StagingError, stage_weather

pytestmark = pytest.mark.spark

LST_OFFSET = -5

HEADER = ("DATE,REPORT_TYPE,HourlyPrecipitation,HourlyDryBulbTemperature,"
          "HourlyRelativeHumidity\n")


def _csv(tmp_path, *rows, name="lcd.csv"):
    path = tmp_path / name
    path.write_text(HEADER + "".join(rows))
    return path


def _staged(spark, tmp_path, staging, *rows, station="central_park", year="2025"):
    stage_weather(spark, station, year, _csv(tmp_path, *rows), "chk",
                  staging, offset_hours=LST_OFFSET)
    return spark.table(f"{staging}.{WEATHER_TABLE}")


def _hours(frame):
    """Read instants through Spark; collect() would shift them to the driver's zone (§8.3)."""
    return [r[0] for r in frame.selectExpr("cast(utc_hour as string)").collect()]


@pytest.mark.parametrize("observed, expected_hour", [
    # Local Standard Time all year, so both seasons shift by exactly five hours. A
    # daylight-aware conversion would give 16:00 for the July row.
    ("2025-01-15T12:51:00", "2025-01-15 17:00:00"),
    ("2025-07-15T12:51:00", "2025-07-15 17:00:00"),
])
def test_the_offset_is_constant_across_seasons(spark, tmp_path, staging_schema,
                                              observed, expected_hour):
    staged = _staged(spark, tmp_path, staging_schema, f"{observed},FM-15,0.00,50,60\n")
    assert _hours(staged) == [expected_hour]


def test_an_hour_that_does_not_exist_locally_is_still_reported(spark, tmp_path, staging_schema):
    # NOAA really does report 02:51 on a spring-forward date, which is why the source cannot
    # be read as local wall-clock time.
    staged = _staged(spark, tmp_path, staging_schema,
                     "2025-03-09T02:51:00,FM-15,0.00,40,50\n")
    assert _hours(staged) == ["2025-03-09 07:00:00"]


def test_dry_trace_and_absent_precipitation_stay_distinguishable(spark, tmp_path, staging_schema):
    staged = _staged(
        spark, tmp_path, staging_schema,
        "2025-01-15T01:51:00,FM-15,0.00,40,50\n",   # measured dry
        "2025-01-15T02:51:00,FM-15,T,40,50\n",      # trace
        "2025-01-15T03:51:00,FM-15,,40,50\n",       # no reading
        "2025-01-15T04:51:00,FM-15,0.25,40,50\n",   # measured rain
    )
    rows = {r["precip_raw"]: r["precip_in"] for r in staged.collect()}
    assert rows["0.00"] == 0.0
    assert rows["T"] == 0.0        # numerically zero, but not a dry hour
    assert rows[""] is None        # unmeasured, not zero
    assert rows["0.25"] == 0.25


def test_special_reports_are_excluded(spark, tmp_path, staging_schema):
    # FM-16 is a special report and usually carries no hourly accumulation.
    staged = _staged(
        spark, tmp_path, staging_schema,
        "2025-01-15T12:07:00,FM-16,,40,50\n",
        "2025-01-15T12:51:00,FM-15,0.04,40,50\n",
        "2025-01-15T13:00:00,SOD  ,,40,50\n",
    )
    assert staged.count() == 1
    assert staged.collect()[0]["precip_raw"] == "0.04"


def test_one_row_per_hour_when_the_source_repeats_one(spark, tmp_path, staging_schema):
    staged = _staged(
        spark, tmp_path, staging_schema,
        "2025-01-15T12:51:00,FM-15,0.01,40,50\n",
        "2025-01-15T12:59:00,FM-15,0.02,40,50\n",
    )
    assert staged.count() == 1
    # Last by source time, deterministically.
    assert staged.collect()[0]["precip_raw"] == "0.02"


def test_a_suspect_value_keeps_its_measurement(spark, tmp_path, staging_schema):
    # NOAA suffixes a doubtful reading with "s". Coercing it away deleted real rainfall and
    # left the hour indistinguishable from one the gauge never reported.
    staged = _staged(
        spark, tmp_path, staging_schema,
        "2025-01-15T01:51:00,FM-15,0.08s,40,50\n",
        "2025-01-15T02:51:00,FM-15,0.08,40,50\n",
    )
    rows = {r["precip_raw"]: (r["precip_in"], r["precip_suspect"]) for r in staged.collect()}
    assert rows["0.08s"] == (0.08, True)
    assert rows["0.08"] == (0.08, False)


def test_a_value_that_is_not_a_number_stays_unmeasured(spark, tmp_path, staging_schema):
    staged = _staged(spark, tmp_path, staging_schema, "2025-01-15T01:51:00,FM-15,??,40,50\n")
    row = staged.collect()[0]
    assert row["precip_in"] is None
    assert row["precip_raw"] == "??"      # kept, so the hour is not mistaken for dry


def test_a_station_year_with_no_routine_observations_fails(spark, tmp_path, staging_schema):
    with pytest.raises(StagingError, match="no FM-15"):
        _staged(spark, tmp_path, staging_schema, "2025-01-15T12:07:00,FM-16,,40,50\n")


def test_a_file_missing_a_required_column_fails(spark, tmp_path, staging_schema):
    path = tmp_path / "short.csv"
    path.write_text("DATE,REPORT_TYPE,HourlyDryBulbTemperature,HourlyRelativeHumidity\n")
    with pytest.raises(StagingError, match="HourlyPrecipitation"):
        stage_weather(spark, "central_park", "2025", path, "chk", staging_schema,
                      offset_hours=LST_OFFSET)


def test_rerunning_a_station_year_replaces_only_it(spark, tmp_path, staging_schema):
    stage_weather(spark, "central_park", "2025",
                  _csv(tmp_path, "2025-01-15T12:51:00,FM-15,0.00,40,50\n", name="a.csv"),
                  "chk", staging_schema, offset_hours=LST_OFFSET)
    stage_weather(spark, "jfk", "2025",
                  _csv(tmp_path, "2025-01-15T12:51:00,FM-15,0.10,40,50\n", name="b.csv"),
                  "chk", staging_schema, offset_hours=LST_OFFSET)
    stage_weather(spark, "central_park", "2025",
                  _csv(tmp_path, "2025-01-15T12:51:00,FM-15,0.00,40,50\n",
                       "2025-01-15T13:51:00,FM-15,0.00,40,50\n", name="c.csv"),
                  "chk", staging_schema, offset_hours=LST_OFFSET)

    staged = spark.table(f"{staging_schema}.{WEATHER_TABLE}")
    counts = {r["station_key"]: r["count"] for r in
              staged.groupBy("station_key").count().collect()}
    # Every station shares the year, so a year-only predicate would have deleted jfk.
    assert counts == {"central_park": 2, "jfk": 1}
