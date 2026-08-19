"""Local Spark session.

Production takes the runtime's session (see cli). This builds one for the local
demonstration and the test suite, with the same settings dbt's session-mode target
declares in profiles.yml -- both processes must agree, or they see different
warehouses and different timestamps.
"""
from __future__ import annotations

import os
import sys

DELTA = "io.delta:delta-spark_2.12:3.2.1"
KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"

WAREHOUSE = "var/warehouse"

# Local mode runs the executor inside the driver, so this is the only heap there is. The
# default is about 1g, which reads the fast demonstration fine and dies on the full one:
# the streaming sink writes one file per Kafka partition, so eight million records land in
# three files of ~470MB, and the vectorized Parquet reader allocates a buffer per row
# group. Measured on a 24GB machine; raise it if the replay grows.
DRIVER_MEMORY = "4g"


def local_session(app_name: str = "pipeline", packages: tuple[str, ...] = (DELTA,)):
    """A Delta-enabled local session sharing dbt's metastore and warehouse.

    `packages` is a parameter rather than a constant because the Kafka connector is
    only needed by `fleet ingest`. Baking it in would make the test suite resolve it
    from Maven on a cold run, and the suite is deliberately network-free.
    """
    from pyspark.sql import SparkSession

    # Pin both ends to this interpreter. Spark launches workers with whatever `python3`
    # is on PATH, which on this machine is a different minor version from the venv and
    # fails every mapInPandas with PYTHON_VERSION_MISMATCH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        # Explicit: inheriting the host zone shifts naive source timestamps and
        # produces plausible instants with false DST flags (design doc 8.3).
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.driver.memory", DRIVER_MEMORY)
        # Relative, so no workspace path is committed and both processes resolve it
        # against the repository root.
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        # dbt-spark's session method calls enableHiveSupport(); without it here the
        # two processes use different catalogs and cannot see each other's tables.
        .enableHiveSupport()
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session
