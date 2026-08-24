# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Clean Silver
# MAGIC Normalizes labels/attack categories, fixes CSV quirks, dedupes flows by a
# MAGIC SHA-256 flow key, and writes `cyber_lakehouse.silver.traffic_clean`.
# MAGIC Mirrors `src/jobs/02_clean_silver.py`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "cyber_lakehouse.bronze.traffic"
SILVER_TABLE = "cyber_lakehouse.silver.traffic_clean"

NULLABLE_COUNT_COLS = ["ct_flw_http_mthd", "is_ftp_login"]
FLOW_KEY_COLS = ["srcip", "sport", "dstip", "dsport", "proto", "stime", "ltime", "sbytes", "dbytes"]
ATTACK_CAT_FIXES = {"backdoors": "backdoor"}

# COMMAND ----------

df = spark.table(BRONZE_TABLE)  # noqa: F821

df = df.withColumn("label_int", F.col("label").cast("int"))

cat = F.lower(F.trim(F.col("attack_cat")))
for raw, fixed in ATTACK_CAT_FIXES.items():
    cat = F.when(cat == raw, F.lit(fixed)).otherwise(cat)
df = df.withColumn(
    "attack_cat_norm",
    F.when(F.col("attack_cat").isNull() | (F.trim(F.col("attack_cat")) == ""), "normal").otherwise(
        cat
    ),
)

# try_cast: blank strings must become NULL->0, and plain cast throws under
# ANSI mode (default on Databricks serverless)
df = df.withColumn(
    "ct_ftp_cmd", F.coalesce(F.expr("try_cast(trim(ct_ftp_cmd) AS BIGINT)"), F.lit(0))
)
for c in NULLABLE_COUNT_COLS:
    df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

df = df.filter(F.col("label_int").isNotNull()).filter(F.col("dur") >= 0)

df = df.withColumn(
    "flow_hash",
    F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in FLOW_KEY_COLS]), 256),
)
w = F.row_number().over(Window.partitionBy("flow_hash").orderBy(F.col("ingest_ts").desc()))
df = df.withColumn("rn", w).filter(F.col("rn") == 1).drop("rn")

df = df.withColumn("is_attack", (F.col("label_int") == F.lit(1)).cast("int"))

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    SILVER_TABLE
)

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"SELECT attack_cat_norm, COUNT(*) AS rows FROM {SILVER_TABLE} "
        "GROUP BY attack_cat_norm ORDER BY rows DESC"
    )
)
