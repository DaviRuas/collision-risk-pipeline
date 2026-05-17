# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC Reads the SAAQ collision table from Unity Catalog, maps columns to the
# MAGIC bronze naming convention, and writes a Delta table.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/lalitaru123@gmail.com/collision-risk-pipeline/src")

from pyspark.sql import functions as F

# COMMAND ----------

# --------------------------------------------------------------------------- #
# Configuration                                                                #
# --------------------------------------------------------------------------- #
SOURCE_TABLE = "workspace.default.collisions_routieres"
BRONZE_DELTA  = "/delta/collisions_bronze"

# COMMAND ----------

# --------------------------------------------------------------------------- #
# 1. Read from Unity Catalog                                                   #
# --------------------------------------------------------------------------- #
raw_df = spark.read.table(SOURCE_TABLE)

print(f"Raw row count: {raw_df.count():,}")

# COMMAND ----------

# --------------------------------------------------------------------------- #
# 2. Map to bronze schema                                                      #
# --------------------------------------------------------------------------- #
bronze_df = (
    raw_df
    .withColumnRenamed("NO_SEQ_COLL",   "accident_id")
    .withColumnRenamed("DT_ACCDN",      "accident_date")   # already DateType
    .withColumnRenamed("LOC_LAT",       "latitude")
    .withColumnRenamed("LOC_LONG",      "longitude")
    .withColumnRenamed("GRAVITE",       "severity")         # already string
    .withColumn("road_condition",    F.col("CD_ETAT_SURFC").cast("string"))
    .withColumn("weather_condition", F.col("CD_COND_METEO").cast("string"))
    .withColumnRenamed("MRC",           "municipality")
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "accident_id",
        "accident_date",
        "latitude",
        "longitude",
        "severity",
        "road_condition",
        "weather_condition",
        "municipality",
        "ingested_at",
    )
)

# COMMAND ----------

# --------------------------------------------------------------------------- #
# 3. Write Delta table                                                         #
# --------------------------------------------------------------------------- #
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "false")
    .save(BRONZE_DELTA)
)

print(f"Written to {BRONZE_DELTA}")

# COMMAND ----------

# --------------------------------------------------------------------------- #
# 4. Validation                                                                #
# --------------------------------------------------------------------------- #
validation_df = spark.read.format("delta").load(BRONZE_DELTA)

print(f"Bronze row count : {validation_df.count():,}")
validation_df.printSchema()
display(validation_df.limit(5))
