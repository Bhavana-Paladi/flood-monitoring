## Project Overview

This project implements flood monitoring data pipelines using PySpark, Google Cloud Storage, and BigQuery.

### Batch Pipeline: Station Baseline + Daily Stats

**Source data (GCS):**
- `gs://flood-monitoring-bhavana-eu/raw/archive/readings-full-YYYY-MM-DD.csv`
  - Near real-time water level readings from the UK Environment Agency flood monitoring API (archive).
- `gs://flood-monitoring-bhavana-eu/raw/stations/stations_level.csv`
  - Metadata for all level stations (location, river, catchment).

**Processing (PySpark script `spark/batch_baseline_stations.py`):**
1. Read archive CSVs and station CSV from GCS.
2. Filter to water level measurements (`parameter = 'level'`) and clean malformed values.
3. Aggregate historical readings per station to compute:
   - mean level, min level, max level, std deviation.
4. Aggregate per station per day to compute:
   - number of readings per day,
   - daily mean level,
   - daily standard deviation.
5. Write curated results back to GCS as Parquet:
   - `curated/batch_station_baseline/`
   - `curated/batch_station_daily/`

**Load into BigQuery:**
- `flood_monitoring.station_baseline` ← `curated/batch_station_baseline/*.parquet`
- `flood_monitoring.station_daily`   ← `curated/batch_station_daily/*.parquet`

These two tables are used as inputs for the dashboard and for the streaming alert pipeline.
