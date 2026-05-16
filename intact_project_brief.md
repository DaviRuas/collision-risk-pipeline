# Road collision risk scoring pipeline

Portfolio project targeting **Intact Financial** (confirmed stack: Databricks · PySpark · AWS · Snowflake · Python).

---

## Data sources
| Source | What | URL |
|---|---|---|
| SAAQ open data | Quebec collision records | https://www.donneesquebec.ca/recherche/dataset/rapports-d-accident |
| Open-Meteo API | Historical weather, no key needed | https://open-meteo.com/en/docs/historical-weather-api |
| Statistics Canada | FSA boundary shapefiles | https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/index2021-eng.cfm |

## Platform
**Databricks Community Edition** — https://community.cloud.databricks.com
Delta Lake · PySpark notebooks · MLflow (built-in) · Databricks SQL

---

## Pipeline
```
SAAQ CSV + Open-Meteo API
        ↓
  Delta BRONZE       raw collisions + weather joined
        ↓
  Delta SILVER       corridor_id · season · time_of_day · weather_zone · severity_score
        ↓
  Delta GOLD         logistic regression risk scores per corridor (MLflow tracked)
        ↓
  Folium choropleth  HTML map · Databricks SQL dashboard
```

---

## Schemas

**Bronze**
```
accident_id, accident_date, latitude, longitude,
severity, road_condition, weather_condition,
municipality, ingested_at
```

**Silver**
```
corridor_id, year_month, season, time_of_day,
weather_zone, collision_count, severity_score,
avg_temp_c, precip_mm
```

**Gold**
```
corridor_id, year_month, risk_score,
risk_tier (low/medium/high), centroid_lat, centroid_lon
```

---

## Folder structure
```
collision-risk-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_features.py
│   ├── 03_model_training.py
│   └── 04_visualisation.py
├── src/
│   ├── weather_client.py
│   ├── schema.py
│   └── features.py
├── tests/
├── output/
└── requirements.txt
```

## Libraries
```
pyspark delta-spark mlflow scikit-learn folium requests pandas geopandas pytest
```
