from pyspark.sql.types import(
    StructType,
    StructField,
    StringType,
    TimestampType,
    DecimalType,
)

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", TimestampType(), False),
    StructField("order_status", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("shipping_address", StringType(), False),
    StructField("shipping_city", StringType(), False),
    StructField("shipping_state", StringType(), True),
    StructField("shipping_postal_code", StringType(), True),
    StructField("shipping_country", StringType(), False),
    StructField("total_amount", DecimalType(12, 2), False),
    StructField("currency", StringType(), False),
    StructField("update_date", TimestampType(), False),
])