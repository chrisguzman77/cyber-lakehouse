from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = "cyber-lakehouse"
    master: str = "local[*]"
    shuffle_partitions: int = 8


def build_spark_submit_args(delta_version: str = "3.3.1") -> list[str]:
    """
    Build the Spark submit arguments needed to enable Delta Lake.
    We'll pass these to spark-submit in docker exec.
    """
    pkg = f"io.delta:delta-spark_2.12:{delta_version}"
    return [
        "--packages",
        pkg,
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf",
        "spark.sql.sources.partitionOverwriteMode=dynamic",
        "--conf",
        "spark.sql.shuffle.partitions=8",
    ]


def env_or_default(name: str, default: str) -> str:
    return os.environ.get(name, default)
