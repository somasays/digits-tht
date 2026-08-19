"""Local HTTP server for acquisition tests.

pypdl downloads over aiohttp, which the `responses` library cannot intercept, so tests
serve real bytes over a loopback socket instead. The server never advertises
`Accept-Ranges`, which keeps pypdl on its single-segment path: downloads are then
deterministic, and the truncation checks in pipeline.acquisition.download are what the
interrupted-download tests actually exercise.
"""
from __future__ import annotations

import http.server
import re
import threading
from collections import Counter

import os

import pytest


class FixtureServer:
    def __init__(self):
        self.routes = {}
        self.request_counts = Counter()
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *args):
                pass

            def _route(self):
                return outer.routes.get(self.path)

            def do_HEAD(self):
                route = self._route()
                if route is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                body, declared_length, content_type = route
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header(
                    "Content-Length",
                    str(declared_length if declared_length is not None else len(body)),
                )
                self.end_headers()

            def do_GET(self):
                outer.request_counts[self.path] += 1
                route = self._route()
                if route is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                body, declared_length, content_type = route
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header(
                    "Content-Length",
                    str(declared_length if declared_length is not None else len(body)),
                )
                self.end_headers()
                self.wfile.write(body)

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def serve(self, path: str, body: bytes, *, declared_length: int | None = None,
              content_type: str = "application/octet-stream") -> str:
        """Register a route. declared_length larger than body simulates a truncated transfer."""
        self.routes[path] = (body, declared_length, content_type)
        return self.base_url + path

    def shutdown(self) -> None:
        self.httpd.shutdown()
        self.thread.join()


@pytest.fixture
def server():
    fixture = FixtureServer()
    yield fixture
    fixture.shutdown()


@pytest.fixture
def raw_root(tmp_path):
    return tmp_path / "raw"


def pytest_collection_modifyitems(config, items):
    """Skip the Spark tier when no JVM is available. `make test` exports JAVA_HOME."""
    if os.environ.get("JAVA_HOME"):
        return
    skip = pytest.mark.skip(reason="no JVM; run via `make test` or set JAVA_HOME")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def staging_schema(spark, request):
    """A database per test: the Spark session is shared, so tables would otherwise collide."""
    name = "staging_" + re.sub(r"\W", "_", request.node.name)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")
    yield name
    spark.sql(f"DROP DATABASE {name} CASCADE")


@pytest.fixture(scope="session")
def spark():
    """A local session for the Spark tier. Production takes the runtime's session instead."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("pipeline-tests")
        # Set explicitly: inheriting it shifts naive source timestamps by the host's offset,
        # producing plausible instants and false flags (design doc 8.3).
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
