from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType
)

product_schema = StructType([
StructField("product_id", StringType() ,False),
StructField("sku", StringType() ,False),
StructField("product_name", StringType() ,False),
StructField("brand", StringType() ,True),
StructField("description", StringType() ,True),
StructField("listing_date", TimestampType() ,False),
StructField("update_date", TimestampType() ,False),
StructField("is_active", BooleanType() ,False),
StructField("category_id", StringType() ,False),
StructField("sub_category_id", StringType() ,False),
])