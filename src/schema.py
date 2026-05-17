from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType,
    DoubleType, DateType, TimestampType,
)

# Actual Unity Catalog table schema: workspace.default.collisions_routieres
# Columns used in bronze ingestion (subset of the full 67-column table)
SAAQ_RAW_SCHEMA = StructType([
    StructField("NO_SEQ_COLL",          StringType(),  nullable=True),
    StructField("DT_ACCDN",             DateType(),    nullable=True),
    StructField("HEURE_ACCDN",          StringType(),  nullable=True),
    StructField("LOC_LAT",              DoubleType(),  nullable=True),
    StructField("LOC_LONG",             DoubleType(),  nullable=True),
    StructField("GRAVITE",              StringType(),  nullable=True),  # e.g. "Mortel", "Blessé grave"
    StructField("CD_ETAT_SURFC",        LongType(),    nullable=True),  # surface condition code
    StructField("CD_COND_METEO",        LongType(),    nullable=True),  # weather condition code
    StructField("MRC",                  StringType(),  nullable=True),  # municipal regional county
    StructField("REG_ADM",              StringType(),  nullable=True),
    StructField("NB_MORTS",             LongType(),    nullable=True),
    StructField("NB_BLESSES_GRAVES",    LongType(),    nullable=True),
    StructField("NB_BLESSES_LEGERS",    LongType(),    nullable=True),
    StructField("NB_VICTIMES_TOTAL",    LongType(),    nullable=True),
    StructField("AN",                   LongType(),    nullable=True),
])

# Bronze: mapped & renamed columns written to Delta
BRONZE_SCHEMA = StructType([
    StructField("accident_id",        StringType(),    nullable=True),
    StructField("accident_date",      DateType(),      nullable=True),
    StructField("latitude",           DoubleType(),    nullable=True),
    StructField("longitude",          DoubleType(),    nullable=True),
    StructField("severity",           StringType(),    nullable=True),
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
