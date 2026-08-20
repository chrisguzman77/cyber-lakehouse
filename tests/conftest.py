from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.errors.exceptions.base import PySparkRuntimeError
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except PySparkRuntimeError as e:
        pytest.skip(f"No usable Java runtime for local Spark: {e}")
    yield session
    session.stop()
