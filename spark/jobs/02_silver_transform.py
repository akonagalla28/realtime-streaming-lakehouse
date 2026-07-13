"""
Silver layer: Bronze -> cleaned, deduped, schema-enforced Delta table.

Applies the data-quality rules that make this table trustworthy for
downstream consumers:
  - drop rows with null order_id / customer_id
  - drop duplicate order_id + status combinations (idempotent re-reads)
  - parse + validate event_ts, drop unparsable rows into a quarantine table
  - clip negative order_value_usd into a rejects table instead of silently
    coercing it

Run as a streaming job (foreachBatch) so Silver stays continuously in
sync with Bronze using Delta's MERGE for upserts.
"""
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

LAKE_ROOT = os.environ.get("LAKE_ROOT", "s3a://lakehouse")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "s3a://lakehouse/_checkpoints")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def clean_batch(batch_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (clean_df, quarantine_df)."""
    parsed = batch_df.withColumn("event_ts_parsed", to_timestamp(col("event_ts")))

    # quarantine: missing keys, unparsable timestamps, negative $ values
    is_bad = (
        col("order_id").isNull()
        | col("customer_id").isNull()
        | col("event_ts_parsed").isNull()
        | (col("order_value_usd") < 0)
    )

    quarantine_df = parsed.filter(is_bad).withColumn("_reject_reason",
        col("order_id").isNull().cast("string"))
    clean_df = parsed.filter(~is_bad)

    # dedupe on (order_id, status) keeping latest ingestion
    w = Window.partitionBy("order_id", "status").orderBy(col("_ingested_at").desc())
    clean_df = (
        clean_df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn", "event_ts")
        .withColumnRenamed("event_ts_parsed", "event_ts")
    )
    return clean_df, quarantine_df


def upsert_to_delta(spark: SparkSession, df: DataFrame, path: str, merge_keys: list[str]):
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return

    target = DeltaTable.forPath(spark, path)
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    (
        target.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(batch_df: DataFrame, batch_id: int):
    spark = batch_df.sparkSession
    clean_df, quarantine_df = clean_batch(batch_df)

    if clean_df.count() > 0:
        upsert_to_delta(spark, clean_df, f"{LAKE_ROOT}/silver/orders", ["order_id", "status"])
    if quarantine_df.count() > 0:
        quarantine_df.write.format("delta").mode("append").save(f"{LAKE_ROOT}/quarantine/orders")


def main():
    spark = get_spark()
    bronze_stream = spark.readStream.format("delta").load(f"{LAKE_ROOT}/bronze/orders")

    query = (
        bronze_stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_orders")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
