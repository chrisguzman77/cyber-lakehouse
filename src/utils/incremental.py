"""Incremental (streaming + MERGE) helpers shared by the local jobs and the
Databricks notebooks.

Every layer below bronze is insert-only keyed by flow_hash: the first copy of a
flow wins, so re-running a job or re-delivering a file never writes duplicates.
Tables are created with Change Data Feed enabled so the next layer can stream
just the newly inserted rows. Targets are addressed by path (local) or by
table name (Unity Catalog).
"""

from __future__ import annotations

from collections.abc import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

CDF_META_COLS = ("_change_type", "_commit_version", "_commit_timestamp")


def first_per_key(
    df: DataFrame,
    key: str = "flow_hash",
    order_cols: tuple[str, ...] = ("ingest_ts", "source_file"),
) -> DataFrame:
    """Keep one row per key: the earliest by order_cols. Needed before an
    insert-only MERGE, which would otherwise insert in-batch duplicates."""
    w = Window.partitionBy(key).orderBy(*[F.col(c).asc() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def ensure_delta_table(
    spark: SparkSession, schema: StructType, *, path: str | None = None, name: str | None = None
) -> DeltaTable:
    """Create the target (with Change Data Feed on) if missing and return it."""
    builder = (
        DeltaTable.createIfNotExists(spark)
        .addColumns(schema)
        .property("delta.enableChangeDataFeed", "true")
    )
    if name:
        builder.tableName(name).execute()
        return DeltaTable.forName(spark, name)
    builder.location(path).execute()
    return DeltaTable.forPath(spark, path)


def insert_new_keys(
    spark: SparkSession,
    batch_df: DataFrame,
    key: str = "flow_hash",
    *,
    path: str | None = None,
    name: str | None = None,
) -> None:
    """MERGE batch_df into the target, inserting only keys it doesn't hold yet.
    batch_df must already be unique on key (see first_per_key)."""
    target = ensure_delta_table(spark, batch_df.schema, path=path, name=name)
    (
        target.alias("t")
        .merge(batch_df.alias("s"), f"t.{key} = s.{key}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def cdf_inserts(changes: DataFrame) -> DataFrame:
    """Rows inserted upstream, from a readChangeFeed DataFrame, minus CDF metadata."""
    return changes.filter(F.col("_change_type") == "insert").drop(*CDF_META_COLS)


def append_stream(
    stream_df: DataFrame, checkpoint: str, *, path: str | None = None, name: str | None = None
) -> None:
    """Append a stream to a Delta target, processing everything available, then stop."""
    writer = (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
    )
    query = writer.toTable(name) if name else writer.start(path)
    query.awaitTermination()


def run_insert_stream(
    stream_df: DataFrame,
    checkpoint: str,
    key: str = "flow_hash",
    *,
    batch_fn: Callable[[DataFrame], DataFrame] | None = None,
    path: str | None = None,
    name: str | None = None,
) -> None:
    """Stream into a Delta target via foreachBatch + insert_new_keys, processing
    everything available, then stop. batch_fn transforms each micro-batch first."""

    def write_batch(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_fn is not None:
            batch_df = batch_fn(batch_df)
        insert_new_keys(batch_df.sparkSession, batch_df, key, path=path, name=name)

    (
        stream_df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
