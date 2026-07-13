from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

ORDER_EVENT_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("order_value_usd", DoubleType(), True),
    StructField("eta_minutes", IntegerType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("event_ts", StringType(), True),
    StructField("source", StringType(), True),
])
