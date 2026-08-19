import hashlib
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.acquisition.tlc import (
    AcquisitionError,
    acquire_periods,
)

PARQUET_CONTENT_TYPE = "application/octet-stream"


def _parquet_bytes(columns: dict) -> bytes:
    table = pa.table(columns)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_acquire_success_promotes_and_matches_fresh_checksum(server, raw_root):
    body = _parquet_bytes({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    url = server.serve("/f.parquet", body, content_type=PARQUET_CONTENT_TYPE)

    result = acquire_periods(["2025-01"], raw_root, urls={"2025-01": url})[0]

    assert result.disposition == "acquired"
    assert result.raw_path.read_bytes() == body
    assert hashlib.sha256(result.raw_path.read_bytes()).hexdigest() == result.checksum_sha256

    parquet_file = pq.ParquetFile(result.raw_path)
    assert parquet_file.metadata.num_rows == result.row_count


def test_multiple_periods_are_acquired_together_in_order(server, raw_root):
    body_jan = _parquet_bytes({"a": [1]})
    body_feb = _parquet_bytes({"a": [2, 3]})
    jan_url = server.serve("/jan.parquet", body_jan, content_type=PARQUET_CONTENT_TYPE)
    feb_url = server.serve("/feb.parquet", body_feb, content_type=PARQUET_CONTENT_TYPE)

    results = acquire_periods(
        ["2025-01", "2025-02"], raw_root,
        urls={"2025-01": jan_url, "2025-02": feb_url},
    )

    assert [r.row_count for r in results] == [1, 2]
    assert results[0].raw_path.read_bytes() == body_jan
    assert results[1].raw_path.read_bytes() == body_feb
    assert (raw_root / "tlc" / "yellow" / "2025-01").exists()
    assert (raw_root / "tlc" / "yellow" / "2025-02").exists()


def test_truncated_download_is_rejected_and_leaves_nothing_visible(server, raw_root):
    body = _parquet_bytes({"a": [1, 2, 3]})
    url = server.serve(
        "/f.parquet", body[: len(body) // 2],
        declared_length=len(body), content_type=PARQUET_CONTENT_TYPE,
    )

    with pytest.raises(AcquisitionError, match="download failed"):
        acquire_periods(["2025-01"], raw_root, urls={"2025-01": url})

    tlc_dir = raw_root / "tlc"
    assert not tlc_dir.exists() or not list(tlc_dir.glob("**/manifest.json"))


def test_corrupt_download_is_rejected_and_leaves_nothing_visible(server, raw_root):
    garbage = b"not a parquet file, just plain bytes" * 10
    url = server.serve("/f.parquet", garbage, content_type=PARQUET_CONTENT_TYPE)

    with pytest.raises(AcquisitionError, match="not valid parquet"):
        acquire_periods(["2025-01"], raw_root, urls={"2025-01": url})

    tlc_dir = raw_root / "tlc"
    assert not tlc_dir.exists() or not list(tlc_dir.glob("**/manifest.json"))


def test_repeated_acquisition_is_idempotent_reuse(server, raw_root):
    body = _parquet_bytes({"a": [1, 2, 3]})
    url = server.serve("/f.parquet", body, content_type=PARQUET_CONTENT_TYPE)

    first = acquire_periods(["2025-01"], raw_root, urls={"2025-01": url})[0]
    second = acquire_periods(["2025-01"], raw_root, urls={"2025-01": url})[0]

    assert first.disposition == "acquired"
    assert second.disposition == "reused"
    assert first.checksum_sha256 == second.checksum_sha256
    assert len(list((raw_root / "tlc" / "yellow" / "2025-01").iterdir())) == 1


def test_changed_bytes_creates_new_version_without_removing_old(server, raw_root):
    body_a = _parquet_bytes({"a": [1, 2, 3]})
    body_b = _parquet_bytes({"a": [4, 5, 6, 7]})

    url = server.serve("/f.parquet", body_a, content_type=PARQUET_CONTENT_TYPE)
    first = acquire_periods(["2025-01"], raw_root, urls={"2025-01": url})[0]

    server.serve("/f.parquet", body_b, content_type=PARQUET_CONTENT_TYPE)
    second = acquire_periods(["2025-01"], raw_root, urls={"2025-01": url})[0]

    assert first.checksum_sha256 != second.checksum_sha256
    assert first.raw_path.read_bytes() == body_a
    assert second.raw_path.read_bytes() == body_b
    assert len(list((raw_root / "tlc" / "yellow" / "2025-01").iterdir())) == 2

