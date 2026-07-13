"""
Bronze layer: Kafka -> raw Delta table.

Reads the raw JSON off Kafka as-is (no cleaning, no dropping bad rows) and
lands it in Delta with an ingestion timestamp + the original Kafka
metadata. This is the immutable "single source of truth" layer — Silver
and Gold can always be rebuilt from Bronze.

Run:
    spark-submit --packages io.delta:delta-spark_2.12:3.1.0,\
      org.apache.spark.kafka:spark-sql-kafka-0-10_2.12:3.5.0 \
      01_bronze_ingest.py
"""
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from schemas.order_event_schema import ORDER_EVENT_SCHEMA

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
LAKE_ROOT = os.environ.get("LAKE_ROOT", "s3a://lakehouse")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "s3a://lakehouse/_checkpoints")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze-ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("S3_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("S3_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def main():
    spark = get_spark()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze = raw.select(
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), ORDER_EVENT_SCHEMA).alias("payload"),
        col("value").cast("string").alias("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
    ).select(
        "kafka_key", "payload.*", "raw_value", "topic", "partition", "offset", "kafka_ts"
    ).withColumn("_ingested_at", current_timestamp())

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_orders")
        .trigger(processingTime="10 seconds")
        .start(f"{LAKE_ROOT}/bronze/orders")
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
