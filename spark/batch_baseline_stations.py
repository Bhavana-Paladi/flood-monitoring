from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------
# CONFIG: ALL PATHS SET TO ABSOLUTE FOR SPARK RELIABILITY
# ---------------------------------------------------------

# GCS bucket (used later — not needed for VM runs)
BUCKET_NAME = "flood-monitoring-bhavana-eu"

# =======================
# RAW / INPUT DATA PATHS
# =======================

# Absolute path to historical CSVs
ARCHIVE_PATH = "/home/b_paladi/flood-monitoring/data/archive"

# Absolute path to stations metadata
STATIONS_PATH = "/home/b_paladi/flood-monitoring/data/stations/stations_level.csv"

# =======================
# CURATED OUTPUT PATHS
# =======================

# Where the batch baseline parquet will be written
OUTPUT_BASELINE_LOCAL = "/home/b_paladi/flood-monitoring/data/curated/batch_station_baseline"

# Where the daily aggregated parquet will be written (optional job)
OUTPUT_DAILY_LOCAL = "/home/b_paladi/flood-monitoring/data/curated/batch_station_daily"



def main():
    # 1) START SPARK SESSION (ENTRY POINT OF PIPELINE)
    spark = (
        SparkSession.builder
        .appName("FloodBatchBaselineStations")
        .getOrCreate()
    )

    # 2) READ RAW DATA (RAW ZONE -> DATAFRAMES)
    # Historic readings
    readings_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ARCHIVE_PATH)
    )

    # Station metadata (locations, river, etc.)
    stations_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(STATIONS_PATH)
    )

    # 3) BASIC COLUMN SELECTION & FILTERING
    readings = (
        readings_raw
        .select("dateTime", "measure", "value")
        .where(F.col("value").isNotNull())
    )

    # 4) CLEAN VALUE COLUMN (STRING -> NUMERIC)
    # Many rows look like "3.488|3.486". We take the first number.
    readings = readings.withColumn(
        "value_str",
        F.regexp_extract("value", r"(-?\d+(\.\d+)?)", 0)
    )

    # Empty string -> NULL, then cast to double
    readings = readings.withColumn(
        "value_clean",
        F.when(F.col("value_str") == "", None).otherwise(F.col("value_str")).cast("double")
    )

    # Drop rows which still don't parse and tidy columns
    readings = (
        readings
        .where(F.col("value_clean").isNotNull())
        .drop("value", "value_str")
        .withColumnRenamed("value_clean", "value")
    )

    # 5) DERIVE STATION REFERENCE AND DATE
    # stationReference is embedded in the measure URI, e.g.
    # http://.../id/measures/1491TH-level-stage-i-15_min-mASD
    readings = readings.withColumn(
        "stationReference",
        F.regexp_extract("measure", r"/id/measures/([0-9A-Z]+)-", 1)
    )

    readings = readings.withColumn(
        "reading_date",
        F.to_date("dateTime")
    )

    # 6) DAILY STATS PER STATION
    # This is like "daily fact table" used for seasonality analysis.
    station_daily_stats = (
        readings
        .groupBy("stationReference", "reading_date")
        .agg(
            F.count("*").alias("n_readings_day"),
            F.avg("value").alias("mean_level_day"),
            F.stddev("value").alias("std_level_day"),
        )
    )

    # 7) LONG-TERM BASELINE PER STATION
    # This is what the streaming pipeline will use as reference.
    station_baseline = (
        readings
        .groupBy("stationReference")
        .agg(
            F.count("*").alias("n_readings_total"),
            F.avg("value").alias("mean_level_total"),
            F.stddev("value").alias("std_level_total"),
            F.expr("percentile_approx(value, 0.05)").alias("p05_level"),
            F.expr("percentile_approx(value, 0.50)").alias("median_level"),
            F.expr("percentile_approx(value, 0.95)").alias("p95_level"),
        )
    )

    # 8) ENRICH BASELINE WITH STATION METADATA
    stations_clean = stations_raw.select(
        "stationReference",
        "label",
        "riverName",
        "catchmentName",
        "town",
        "lat",
        "long",
        "status",
    )

    baseline_enriched = (
        station_baseline
        .join(stations_clean, on="stationReference", how="left")
    )

    # 9) DERIVE ALERT THRESHOLDS FROM BASELINE STATS
    baseline_enriched = (
        baseline_enriched
        .withColumn(
            "alert_threshold_high",
            F.col("p95_level")
        )
        .withColumn(
            "alert_threshold_very_high",
            F.col("mean_level_total") + 2 * F.col("std_level_total")
        )
    )

    # 10) WRITE CURATED OUTPUTS (CURATED ZONE)
    # Station-level baseline with metadata + thresholds
    (
        baseline_enriched
        .write
        .mode("overwrite")
        .parquet(OUTPUT_BASELINE_LOCAL)
    )

    # Station/day statistics
    (
        station_daily_stats
        .write
        .mode("overwrite")
        .parquet(OUTPUT_DAILY_LOCAL)
    )

    spark.stop()


if __name__ == "__main__":
    main()

