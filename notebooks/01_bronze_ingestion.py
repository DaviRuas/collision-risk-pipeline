# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC Reads raw SAAQ collision CSVs from DBFS, applies an explicit schema,
# MAGIC renames columns to the bronze naming convention, and writes a Delta table.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/<your-repo>/src")   # adjust to your Databricks repo path

from pyspark.sql import functions as F
from src.schema import SAAQ_RAW_SCHEMA

# COMMAND ----------

# --------------------------------------------------------------------------- #
# Configuration — edit the path after uploading files to DBFS                 #
# --------------------------------------------------------------------------- #
SAAQ_DBFS_PATH  = "/dbfs/FileStore/saaq/"         # folder containing *.csv files
BRONZE_DELTA    = "/delta/collisions_bronze"
DATE_FORMAT     = "yyyy-MM-dd"                     # adjust if SAAQ uses a different format

# COMMAND ----------

# --------------------------------------------------------------------------- #
# 1. Read raw CSVs with explicit schema                                        #
# --------------------------------------------------------------------------- #
raw_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")          # bad rows land in _corrupt_record
    .option("encoding", "UTF-8")
    .schema(SAAQ_RAW_SCHEMA)
    .csv(SAAQ_DBFS_PATH)
)

print(f"Raw row count: {raw_df.count():,}")

# COMMAND ----------

# --------------------------------------------------------------------------- #
# 2. Rename columns + parse date + add ingested_at                            #
# --------------------------------------------------------------------------- #
bronze_df = (
    raw_df
    .withColumnRenamed("NO_RAPPORT",    "accident_id")
    .withColumn("accident_date", F.to_date(F.col("DT_ACCDN"), DATE_FORMAT))
    .drop("DT_ACCDN")
    .withColumnRenamed("LOC_LAT",       "latitude")
    .withColumnRenamed("LOC_LONG",      "longitude")
    .withColumnRenamed("GRAVITE",       "severity")
    .withColumnRenamed("CD_COND_ROUTE", "road_condition")
    .withColumnRenamed("CD_METEO",      "weather_condition")
    .withColumnRenamed("MUN_NM",        "municipality")
    .drop("HR_ACCDN")                    # kept in raw; not in bronze schema
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
print(f"Bronze schema    :")
validation_df.printSchema()

display(validation_df.limit(5))
