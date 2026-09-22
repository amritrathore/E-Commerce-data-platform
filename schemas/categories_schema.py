from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    TimestampType
)

categories_schema = StructType([
    StructField("category_id", StringType(), False),
    StructField("category_name", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("updated_date", TimestampType(), False),
])