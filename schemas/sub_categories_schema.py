from pyspark.sql.types import(
    StructType,
    StructField,
    StringType,
    TimestampType,
    BooleanType
)

sub_categories_schema = StructType([
    StructField("sub_category_id", StringType(), False),
    StructField("category_id", StringType(), False),
    StructField("sub_category_name", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("updated_date", TimestampType(), False),
])