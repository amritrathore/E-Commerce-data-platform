from pyspark.sql.types import(
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType,
)

product_prices_schema = StructType([
    StructField("price_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("price", DecimalType(12, 2), False),
    StructField("currency", StringType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("updated_date", TimestampType(), False),
])