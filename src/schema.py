from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, DateType, TimestampType,
)

# Raw SAAQ CSV column names (French headers as published by Quebec open data)
SAAQ_RAW_SCHEMA = StructType([
    StructField("NO_RAPPORT",    StringType(),  nullable=True),
    StructField("DT_ACCDN",      StringType(),  nullable=True),  # "YYYY-MM-DD"
    StructField("HR_ACCDN",      StringType(),  nullable=True),  # "HH:MM"
    StructField("LOC_LAT",       DoubleType(),  nullable=True),
    StructField("LOC_LONG",      DoubleType(),  nullable=True),
    StructField("GRAVITE",       IntegerType(), nullable=True),  # 1=fatal 2=serious 3=minor 4=PDO
    StructField("CD_COND_ROUTE", StringType(),  nullable=True),
    StructField("CD_METEO",      StringType(),  nullable=True),
    StructField("MUN_NM",        StringType(),  nullable=True),
])

# Bronze: mapped & renamed columns written to Delta
BRONZE_SCHEMA = StructType([
    StructField("accident_id",        StringType(),    nullable=True),
    StructField("accident_date",      DateType(),      nullable=True),
    StructField("latitude",           DoubleType(),    nullable=True),
    StructField("longitude",          DoubleType(),    nullable=True),
    StructField("severity",           IntegerType(),   nullable=True),
    StructField("road_condition",     StringType(),    nullable=True),
    StructField("weather_condition",  StringType(),    nullable=True),
    StructField("municipality",       StringType(),    nullable=True),
    StructField("ingested_at",        TimestampType(), nullable=False),
])

# Silver: feature-engineered corridor aggregates
SILVER_SCHEMA = StructType([
    StructField("corridor_id",      StringType(),  nullable=False),
    StructField("year_month",       StringType(),  nullable=False),  # "YYYY-MM"
    StructField("season",           StringType(),  nullable=True),   # winter/spring/summer/fall
    StructField("time_of_day",      StringType(),  nullable=True),   # morning/afternoon/evening/night
    StructField("weather_zone",     StringType(),  nullable=True),
    StructField("collision_count",  LongType(),    nullable=False),
    StructField("severity_score",   DoubleType(),  nullable=True),
    StructField("avg_temp_c",       DoubleType(),  nullable=True),
    StructField("precip_mm",        DoubleType(),  nullable=True),
])

# Gold: model-scored corridors ready for visualisation
GOLD_SCHEMA = StructType([
    StructField("corridor_id",   StringType(), nullable=False),
    StructField("year_month",    StringType(), nullable=False),
    StructField("risk_score",    DoubleType(), nullable=True),
    StructField("risk_tier",     StringType(), nullable=True),   # low / medium / high
    StructField("centroid_lat",  DoubleType(), nullable=True),
    StructField("centroid_lon",  DoubleType(), nullable=True),
])
