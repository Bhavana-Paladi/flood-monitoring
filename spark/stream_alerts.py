import json
from pyspark.sql import SparkSession, functions as F, types as T


# ---------- Spark session with Kafka connector ----------

def create_spark():
    """
    Create a SparkSession with the Kafka connector jar.
    """
    return (
        SparkSession.builder
        .appName("flood-stream-alerts")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )


# ---------- Schemas and paths ----------

READINGS_SCHEMA = T.StructType(
    [
        T.StructField("stationReference", T.StringType(), True),
        T.StructField("dateTime", T.TimestampType(), True),
        T.StructField("value", T.DoubleType(), True),
        T.StructField("measure", T.StringType(), True),
        T.StructField("stationName", T.StringType(), True),
        T.StructField("riverName", T.StringType(), True),
        T.StructField("catchmentName", T.StringType(), True),
        T.StructField("town", T.StringType(), True),
        T.StructField("lat", T.DoubleType(), True),
        T.StructField("long", T.DoubleType(), True),
    ]
)

BASELINE_PARQUET = "data/curated/batch_station_baseline"
OUTPUT_PARQUET = "data/curated/stream_alerts"
CHECKPOINT_DIR = "data/checkpoints/stream_alerts"


# ---------- Main streaming pipeline ----------

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_bootstrap = "localhost:9092"
    topic = "flood_readings"

    # 1) Read JSON records from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    readings = (
        kafka_df
        .select(F.col("value").cast("string").alias("value_str"))
        .select(F.from_json("value_str", READINGS_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("stationReference").isNotNull() & F.col("dateTime").isNotNull())
    )

    # 2) Windowed statistics per station
    windowed = (
        readings
        .withWatermark("dateTime", "10 minutes")
        .groupBy(
            "stationReference",
            F.window("dateTime", "10 minutes", "5 minutes"),
        )
        .agg(
            F.count("*").alias("n_readings_window"),
            F.avg("value").alias("mean_level_window"),
            F.stddev("value").alias("std_level_window"),
        )
    )

    # 3) Load static baseline from batch pipeline
    baseline = (
        spark.read.parquet(BASELINE_PARQUET)
        .select(
            "stationReference",
            "mean_level_total",
            "std_level_total",
            "alert_threshold_high",
            "alert_threshold_very_high",
            "riverName",
            "catchmentName",
            "town",
        )
        .dropDuplicates(["stationReference"])
    )

    # 4) Join streaming stats with baseline and create alert level
    joined = windowed.join(baseline, on="stationReference", how="left")

    alerts = (
        joined
        .withColumn(
            "z_score",
            (F.col("mean_level_window") - F.col("mean_level_total"))
            / F.col("std_level_total")
        )
        .withColumn(
            "alert_level",
            F.when(
                F.col("mean_level_window") >= F.col("alert_threshold_very_high"),
                F.lit("RED"),
            )
            .when(
                F.col("mean_level_window") >= F.col("alert_threshold_high"),
                F.lit("AMBER"),
            )
            .otherwise(F.lit("NORMAL"))
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .select(
            "stationReference",
            "window_start",
            "window_end",
            "n_readings_window",
            "mean_level_window",
            "std_level_window",
            "mean_level_total",
            "std_level_total",
            "z_score",
            "alert_level",
            "alert_threshold_high",
            "alert_threshold_very_high",
            "riverName",
            "catchmentName",
            "town",
        )
    )

    # 5) Write streaming alerts to curated parquet folder
    query = (
        alerts.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", OUTPUT_PARQUET)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
