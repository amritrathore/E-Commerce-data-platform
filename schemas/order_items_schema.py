from pyspark.sql.types import(
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
)

order_items_schema = StructType([
    StructField("order_item_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price_id", StringType(), False),
    StructField("discount_percent", IntegerType(), False),
    StructField("tax_percent", IntegerType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("updated_date", TimestampType(), False),
])