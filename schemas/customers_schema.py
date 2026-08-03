from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType


customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("address", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("signup_datetime", TimestampType(), False),
    StructField("email", StringType(), False),
    StructField("phone_number", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", TimestampType(), True),
    StructField("last_modified_date", TimestampType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), False),
])