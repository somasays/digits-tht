from __future__ import annotations

import json
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import requests
from pypdl import Pypdl

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_MAX_CONCURRENT = 4
DEFAULT_HEAD_TIMEOUT_SECONDS = 15.0


class AcquisitionError(RuntimeError):
    """Raised when a source object cannot be acquired, or fails content validation."""


@dataclass(frozen=True)
class FileSpec:
    """One file to acquire: where it comes from, where it lands, and how it is validated.

    `destination_prefix` is the source-specific path below which the common acquisition
    code creates a checksum-addressed directory.
    `validate` receives the downloaded path and the response content type, and returns the
    row count or raises AcquisitionError.
    """
    source_url: str
    destination_prefix: Path
    validate: Callable[[Path, str | None], int]
    manifest_extra: dict


@dataclass(frozen=True)
class AcquiredFile:
    checksum_sha256: str
    disposition: str  # "acquired" | "reused"
    raw_path: Path
    file_size_bytes: int
    row_count: int


def acquire_files(
    specs: list[FileSpec],
    raw_root: Path,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
) -> list[AcquiredFile]:
    """Download every spec concurrently, then validate and promote each one.

    Results are returned in the order the specs were given. Downloading is the slow,
    network-bound part and runs concurrently under a single pypdl instance; validation and
    promotion are local and run per file afterwards.
    """
    if not specs:
        return []

    raw_root = Path(raw_root)
    tmp_root = raw_root / "tmp" / uuid.uuid4().hex
    tmp_root.mkdir(parents=True, exist_ok=True)

    try:
        metadata = [_read_source_metadata(spec.source_url) for spec in specs]
        tmp_dirs = [tmp_root / str(index) for index in range(len(specs))]
        for tmp_dir in tmp_dirs:
            tmp_dir.mkdir(parents=True, exist_ok=True)
        dest_files = [
            tmp_dir / spec.source_url.rsplit("/", 1)[-1]
            for tmp_dir, spec in zip(tmp_dirs, specs)
        ]

        checksums = _download_all(
            [spec.source_url for spec in specs], dest_files,
            max_attempts=max_attempts, max_concurrent=max_concurrent,
        )

        results = []
        for spec, (content_type, expected_bytes), tmp_dir, dest_file, checksum in zip(
            specs, metadata, tmp_dirs, dest_files, checksums
        ):
            results.append(
                _promote_one(spec, tmp_dir, dest_file, checksum, content_type, expected_bytes)
            )
        return results
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def _read_source_metadata(url: str) -> tuple[str | None, int | None]:
    """Content type and declared length. pypdl exposes no response headers, and the content
    type is part of what acquisition validates."""
    try:
        head = requests.head(url, timeout=DEFAULT_HEAD_TIMEOUT_SECONDS, allow_redirects=True)
        head.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise AcquisitionError(f"could not read source metadata for {url}: {exc}") from exc

    declared_length = head.headers.get("Content-Length")
    return head.headers.get("Content-Type"), int(declared_length) if declared_length else None


def _download_all(
    urls: list[str],
    dest_files: list[Path],
    *,
    max_attempts: int,
    max_concurrent: int,
) -> list[str]:
    """Download every url concurrently under one pypdl instance; return checksums in order."""
    downloader = Pypdl(max_concurrent=min(max_concurrent, len(urls)))
    result = downloader.start(
        tasks=[
            {"url": url, "file_path": str(dest_file)}
            for url, dest_file in zip(urls, dest_files)
        ],
        retries=max_attempts - 1,
        hash_algorithms="sha256",
        block=True,
        display=False,
    )
    if not result or len(result) != len(urls):
        failed = downloader.failed or [u for u in urls]
        raise AcquisitionError(
            f"download failed after {max_attempts} attempts for: {', '.join(failed)}"
        )

    by_url = {url: validator for url, validator in result}
    missing = [url for url in urls if url not in by_url]
    if missing:
        raise AcquisitionError(
            f"download failed after {max_attempts} attempts for: {', '.join(missing)}"
        )
    return [by_url[url].get_hash("sha256") for url in urls]


def _promote_one(
    spec: FileSpec,
    tmp_dir: Path,
    dest_file: Path,
    checksum: str,
    content_type: str | None,
    expected_bytes: int | None,
) -> AcquiredFile:
    file_size = dest_file.stat().st_size
    if expected_bytes is not None and file_size != expected_bytes:
        raise AcquisitionError(
            f"incomplete download for {spec.source_url}: "
            f"expected {expected_bytes} bytes, got {file_size}"
        )

    row_count = spec.validate(dest_file, content_type)

    final_dir = spec.destination_prefix / checksum
    manifest = {
        "source_url": spec.source_url,
        **spec.manifest_extra,
        "checksum_sha256": checksum,
        "file_size_bytes": file_size,
        "row_count": row_count,
        "retrieved_at": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }
    (tmp_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    disposition = _promote_or_reuse(tmp_dir, final_dir)
    return AcquiredFile(
        checksum_sha256=checksum,
        disposition=disposition,
        raw_path=final_dir / dest_file.name,
        file_size_bytes=file_size,
        row_count=row_count,
    )


def _promote_or_reuse(tmp_dir: Path, final_dir: Path) -> str:
    """Move tmp_dir to its checksum-addressed location, or discard it if that location already
    holds these bytes. The rename is atomic because tmp_dir sits under the same raw root."""
    if final_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return "reused"
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(tmp_dir, final_dir)
        return "acquired"
    except OSError:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return "reused"
