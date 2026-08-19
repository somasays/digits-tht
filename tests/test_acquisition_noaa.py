import csv
import hashlib
import io
import json

import pytest

from pipeline.acquisition.noaa import (
    AcquisitionError,
    acquire_station_years,
)

HEADER = ["STATION", "DATE", "REPORT_TYPE", "HourlyPrecipitation", "HourlyDryBulbTemperature"]
ROW = ["72505394728", "2025-01-01T00:51:00", "FM-15", "0.00", "32"]
STATIONS = {"central_park": "72505394728"}


def _csv_bytes(rows: list[list[str]], header: list[str] = HEADER) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _acquire(raw_root, url, stations=None, years=("2025",)):
    stations = stations or STATIONS
    urls = {(key, year): url for key in stations for year in years}
    return acquire_station_years(stations, list(years), raw_root, urls=urls)


def test_acquire_success_promotes_and_matches_fresh_checksum(server, raw_root):
    body = _csv_bytes([ROW])
    url = server.serve("/cp.csv", body, content_type="text/csv")

    result = _acquire(raw_root, url)[0]

    assert result.disposition == "acquired"
    assert result.raw_path.read_bytes() == body
    assert hashlib.sha256(result.raw_path.read_bytes()).hexdigest() == result.checksum_sha256
    assert result.row_count == 1

    manifest = json.loads((result.raw_path.parent / "manifest.json").read_text())
    assert manifest["station_id"] == "72505394728"
    assert manifest["delivery_format"] == "csv"
    assert manifest["year"] == "2025"


def test_multiple_stations_are_acquired_together(server, raw_root):
    body = _csv_bytes([ROW])
    url = server.serve("/lcd.csv", body, content_type="text/csv")
    stations = {"central_park": "72505394728", "laguardia": "72503014732"}

    results = _acquire(raw_root, url, stations=stations)

    assert len(results) == 2
    assert (raw_root / "noaa" / "lcd" / "central_park" / "2025").exists()
    assert (raw_root / "noaa" / "lcd" / "laguardia" / "2025").exists()


def test_missing_required_column_blocks_promotion_with_actionable_error(server, raw_root):
    body = _csv_bytes([["72505394728", "2025-01-01T00:51:00", "FM-15"]],
                      header=["STATION", "DATE", "REPORT_TYPE"])
    url = server.serve("/cp.csv", body, content_type="text/csv")

    with pytest.raises(AcquisitionError, match="HourlyPrecipitation"):
        _acquire(raw_root, url)

    noaa_dir = raw_root / "noaa"
    assert not noaa_dir.exists() or not list(noaa_dir.glob("**/manifest.json"))


def test_unexpected_content_type_blocks_promotion(server, raw_root):
    body = _csv_bytes([ROW])
    url = server.serve("/cp.csv", body, content_type="text/html")

    with pytest.raises(AcquisitionError, match="content type"):
        _acquire(raw_root, url)

    noaa_dir = raw_root / "noaa"
    assert not noaa_dir.exists() or not list(noaa_dir.glob("**/manifest.json"))


def test_truncated_download_is_rejected_and_leaves_nothing_visible(server, raw_root):
    body = _csv_bytes([ROW] * 50)
    url = server.serve(
        "/cp.csv", body[: len(body) // 2],
        declared_length=len(body), content_type="text/csv",
    )

    with pytest.raises(AcquisitionError, match="download failed"):
        _acquire(raw_root, url)

    noaa_dir = raw_root / "noaa"
    assert not noaa_dir.exists() or not list(noaa_dir.glob("**/manifest.json"))


def test_repeated_acquisition_is_idempotent_reuse(server, raw_root):
    body = _csv_bytes([ROW])
    url = server.serve("/cp.csv", body, content_type="text/csv")

    first = _acquire(raw_root, url)[0]
    second = _acquire(raw_root, url)[0]

    assert first.disposition == "acquired"
    assert second.disposition == "reused"
    assert first.checksum_sha256 == second.checksum_sha256
    assert len(list((raw_root / "noaa" / "lcd" / "central_park" / "2025").iterdir())) == 1


def test_revised_delivery_creates_new_version_without_removing_old(server, raw_root):
    body_v1 = _csv_bytes([ROW])
    body_v2 = _csv_bytes([ROW, ["72505394728", "2025-01-01T01:51:00", "FM-15", "0.01", "31"]])

    url = server.serve("/cp.csv", body_v1, content_type="text/csv")
    first = _acquire(raw_root, url)[0]

    server.serve("/cp.csv", body_v2, content_type="text/csv")
    second = _acquire(raw_root, url)[0]

    assert first.checksum_sha256 != second.checksum_sha256
    assert first.raw_path.read_bytes() == body_v1
    assert second.raw_path.read_bytes() == body_v2
    assert len(list((raw_root / "noaa" / "lcd" / "central_park" / "2025").iterdir())) == 2

