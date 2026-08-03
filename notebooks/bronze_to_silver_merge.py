# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: generic MERGE
# MAGIC
# MAGIC Reads the same `ctl.SourceRegistry` the ADF pipelines use, so this notebook handles every
# MAGIC source without modification. Adding a table to the registry adds it here too.
# MAGIC
# MAGIC The MERGE is **idempotent** — re-running it after a partial failure produces the same
# MAGIC Silver state rather than duplicating rows.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

dbutils.widgets.text("source_system", "SalesDB")
dbutils.widgets.text("ingest_date", "")

SOURCE_SYSTEM = dbutils.widgets.get("source_system")
INGEST_DATE = dbutils.widgets.get("ingest_date") or None

LAKE = "abfss://lakehouse@yourstorageaccount.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the registry
# MAGIC
# MAGIC JDBC credentials come from a Key Vault-backed secret scope, same as the streaming project.

# COMMAND ----------

jdbc_url = (
    "jdbc:sqlserver://yourserver.database.windows.net:1433;"
    "database=controldb;encrypt=true;trustServerCertificate=false;"
)

registry = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("query", f"""
        SELECT SourceId, SourceSchema, SourceTable, LoadType,
               PrimaryKeyColumns, TargetContainer, TargetPath
        FROM ctl.SourceRegistry
        WHERE IsActive = 1 AND SourceSystem = '{SOURCE_SYSTEM}'
    """)
    .option("user", dbutils.secrets.get("kv-scope", "sql-user"))
    .option("password", dbutils.secrets.get("kv-scope", "sql-password"))
    .load()
    .collect()
)

print(f"{len(registry)} active sources for {SOURCE_SYSTEM}")

# COMMAND ----------

def merge_to_silver(row):
    """Upsert one Bronze dataset into its Silver Delta table."""
    table_name = row["SourceTable"].lower()
    pk_cols = [c.strip() for c in (row["PrimaryKeyColumns"] or "").split(",") if c.strip()]

    if not pk_cols:
        raise ValueError(f"{table_name}: PrimaryKeyColumns is empty — cannot MERGE without a key")

    bronze_path = f"{LAKE}/bronze/{row['TargetPath']}"
    silver_path = f"{LAKE}/silver/{table_name}"

    reader = spark.read.format("parquet")
    if INGEST_DATE:
        bronze_path = f"{bronze_path}/ingest_date={INGEST_DATE}"

    src = reader.load(bronze_path)

    # Bronze can hold several versions of the same key across incremental runs.
    # Keep only the newest per key before merging, otherwise MERGE throws on
    # multiple source rows matching one target row.
    if "ModifiedDate" in src.columns:
        order_col = F.col("ModifiedDate").desc()
    else:
        order_col = F.col("_ingested_at").desc()

    dedup_window = Window.partitionBy(*pk_cols).orderBy(order_col)

    src = (
        src.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )

    if not DeltaTable.isDeltaTable(spark, silver_path):
        src.write.format("delta").save(silver_path)
        print(f"  {table_name}: created Silver with {src.count()} rows")
        return

    tgt = DeltaTable.forPath(spark, silver_path)
    condition = " AND ".join([f"t.`{c}` = s.`{c}`" for c in pk_cols])

    (
        tgt.alias("t")
        .merge(src.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"  {table_name}: merged on {pk_cols}")


# COMMAND ----------

failures = []

for row in registry:
    try:
        merge_to_silver(row)
    except Exception as exc:  # noqa: BLE001 — collect and report, do not abort the batch
        failures.append((row["SourceTable"], str(exc)))
        print(f"  FAILED {row['SourceTable']}: {exc}")

if failures:
    raise RuntimeError(f"{len(failures)} source(s) failed: {[f[0] for f in failures]}")

print("All sources merged successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance
# MAGIC
# MAGIC MERGE rewrites files. Compact and reclaim on a schedule.

# COMMAND ----------

for row in registry:
    path = f"{LAKE}/silver/{row['SourceTable'].lower()}"
    if DeltaTable.isDeltaTable(spark, path):
        spark.sql(f"OPTIMIZE delta.`{path}`")
        spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")
