from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from delta import configure_spark_with_delta_pip
    from pyspark.errors.exceptions.base import PySparkRuntimeError
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # Delta's default of 50 snapshot partitions makes tiny test tables slow
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except PySparkRuntimeError as e:
        pytest.skip(f"No usable Java runtime for local Spark: {e}")
    yield session
    session.stop()


@pytest.fixture
def raw_row():
    """Factory for one raw UNSW-NB15 row (tuple in FULL_SCHEMA_FIELDS order)."""
    from src.utils.schema import FULL_SCHEMA_FIELDS

    defaults = {"string": "x", "double": 0.5, "long": 1}
    base = {name: defaults[t] for name, t in FULL_SCHEMA_FIELDS}
    base.update(
        srcip="10.0.0.1",
        sport="1234",
        dstip="10.0.0.2",
        dsport="80",
        proto="tcp",
        state="FIN",
        service="http",
        ct_ftp_cmd="0",
        attack_cat=None,
        label=0,
    )

    def make(**overrides):
        row = {**base, **overrides}
        return tuple(row[name] for name, _ in FULL_SCHEMA_FIELDS)

    return make
