"""
Gold layer: Silver -> business-ready aggregates + ML feature tables.

Produces two Gold tables:
  1. gold.restaurant_ops_metrics  -- rolling order volume / avg value / cancel
     rate per restaurant, refreshed on a micro-batch schedule (BI-facing).
  2. gold.eta_model_features      -- point-in-time feature set keyed by
     order_id for the ETA prediction model (driver load, restaurant
     backlog, historical on-time rate).

This is a batch job intended to be triggered by Airflow every N minutes
rather than run continuously, since Gold aggregates don't need
sub-minute freshness.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LAKE_ROOT = os.environ.get("LAKE_ROOT", "s3a://lakehouse")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-aggregate")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_restaurant_ops_metrics(spark: SparkSession):
    silver = spark.read.format("delta").load(f"{LAKE_ROOT}/silver/orders")

    metrics = (
        silver.groupBy("restaurant_id", F.window("event_ts", "1 hour"))
        .agg(
            F.count("order_id").alias("order_count"),
            F.avg("order_value_usd").alias("avg_order_value"),
            F.sum(F.when(F.col("status") == "CANCELED", 1).otherwise(0)).alias("canceled_count"),
            F.avg("eta_minutes").alias("avg_eta_minutes"),
        )
        .withColumn(
            "cancel_rate", F.col("canceled_count") / F.col("order_count")
        )
        .select(
            "restaurant_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "order_count", "avg_order_value", "canceled_count", "cancel_rate", "avg_eta_minutes",
        )
    )

    (
        metrics.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("window_start")
        .save(f"{LAKE_ROOT}/gold/restaurant_ops_metrics")
    )


def build_eta_model_features(spark: SparkSession):
    silver = spark.read.format("delta").load(f"{LAKE_ROOT}/silver/orders")

    driver_load = (
        silver.filter(F.col("status").isin("ACCEPTED", "PICKED_UP"))
        .groupBy("driver_id")
        .agg(F.count("order_id").alias("driver_active_orders"))
    )

    restaurant_backlog = (
        silver.filter(F.col("status").isin("CREATED", "ACCEPTED"))
        .groupBy("restaurant_id")
        .agg(F.count("order_id").alias("restaurant_open_orders"))
    )

    w = Window.partitionBy("restaurant_id").orderBy("event_ts")
    restaurant_history = (
        silver.filter(F.col("status") == "DELIVERED")
        .withColumn("on_time", (F.col("eta_minutes") <= 45).cast("int"))
        .withColumn(
            "restaurant_on_time_rate_rolling",
            F.avg("on_time").over(w.rowsBetween(-49, -1)),
        )
        .select("order_id", "restaurant_id", "restaurant_on_time_rate_rolling")
    )

    features = (
        silver.select("order_id", "restaurant_id", "driver_id", "eta_minutes", "event_ts")
        .join(driver_load, "driver_id", "left")
        .join(restaurant_backlog, "restaurant_id", "left")
        .join(restaurant_history, ["order_id", "restaurant_id"], "left")
        .na.fill({"driver_active_orders": 0, "restaurant_open_orders": 0, "restaurant_on_time_rate_rolling": 0.8})
    )

    (
        features.write.format("delta")
        .mode("overwrite")
        .save(f"{LAKE_ROOT}/gold/eta_model_features")
    )


def main():
    spark = get_spark()
    build_restaurant_ops_metrics(spark)
    build_eta_model_features(spark)
    spark.stop()


if __name__ == "__main__":
    main()
