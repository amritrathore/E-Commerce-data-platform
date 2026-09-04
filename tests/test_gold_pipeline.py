from datetime import datetime

from core.spark_session_manager import SparkSessionManager
from pipeline.gold.gold_pipeline import GoldPipeline

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    BooleanType,
)


class FakeReader:

    def __init__(self, df):
        self.df = df

    def read(self, dataset_name):
        return self.df
    

class FakeMissingReader:

    def read(self, dataset_name):
        raise Exception("Gold data not found")


class FakeWriter:

    def __init__(self):
        self.written_df = None
        self.dataset_name = None
        self.layer = None

    def write(self, df, dataset_name, layer):
        self.written_df = df
        self.dataset_name = dataset_name
        self.layer = layer


def test_gold_pipeline_transforms_and_writes_customer_dimension():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "100",
            "Priya",
            "Yadav",
            "Jaipur",
            "Rajasthan",
            "190, Shastri Nagar",
            "Home",
            datetime(2023, 5, 23, 23, 6),
            "priya.yadav100@example.com",
            "9193349856",
            "Female",
            datetime(1988, 1, 1),
            datetime(2024, 2, 26, 23, 6),
            True,
            "302001",
            "India",
        )
    ]

    columns = [
        "customer_id",
        "first_name",
        "last_name",
        "city",
        "state",
        "address",
        "address_type",
        "signup_datetime",
        "email",
        "phone_number",
        "gender",
        "date_of_birth",
        "last_modified_date",
        "is_active",
        "postal_code",
        "country",
    ]

    silver_df = spark.createDataFrame(
        data,
        columns
    )

    reader = FakeReader(silver_df)
    gold_reader = FakeMissingReader()
    writer = FakeWriter()

    pipeline = GoldPipeline(
        reader=reader,
        gold_reader=gold_reader,
        writer=writer,
    )

    pipeline.run("customers")

    assert writer.dataset_name == "customers"
    assert writer.layer == "gold"
    assert writer.written_df is not None

    row = writer.written_df.collect()[0]

    assert row.customer_id == "100"
    assert row.customer_key is not None
    assert row.is_current is True
    assert row.effective_from is not None
    assert row.effective_to is None



def test_gold_pipeline_scd2_type():

    spark = SparkSessionManager.get_session()

    silver_columns = [
        "customer_id",
        "first_name",
        "last_name",
        "city",
        "state",
        "address",
        "address_type",
        "signup_datetime",
        "email",
        "phone_number",
        "gender",
        "date_of_birth",
        "last_modified_date",
        "is_active",
        "postal_code",
        "country",
    ]

    gold_schema = StructType([
        StructField("customer_key", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("date_of_birth", TimestampType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("address", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("signup_datetime", TimestampType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
    ])

    incoming_data = [
        (
            "100",
            "Priya",
            "Yadav",
            "Jaipur",
            "Rajasthan",
            "190, Shastri Nagar",
            "Home",
            datetime(2023, 5, 23, 23, 6),
            "priya.yadav100@example.com",
            "9193349856",
            "Female",
            datetime(1988, 1, 1),
            datetime(2024, 2, 26, 23, 6),
            True,
            "302001",
            "India",
        )
    ]

    existing_data = [
        (
            "old-customer-key",
            "100",
            "Priya",
            "Yadav",
            "Female",
            datetime(1988, 1, 1),
            "Agra",
            "Rajasthan",
            "302001",
            "India",
            "190, Shastri Nagar",
            "Home",
            datetime(2023, 5, 23, 23, 6),
            True,
            datetime(2026, 9, 1, 10, 0),
            None,
            True,
        )
    ]

    incoming_df = spark.createDataFrame(
            incoming_data,
            silver_columns
        )

    existing_df =  spark.createDataFrame(
            existing_data,
            gold_schema
        )

    reader = FakeReader(incoming_df)
    gold_reader = FakeReader(existing_df)
    writer = FakeWriter()
    
    pipeline = GoldPipeline(
            reader=reader,
            gold_reader=gold_reader,
            writer=writer,
        )

    pipeline.run(
        dataset_name="customers"
    )

    result_df = writer.written_df

    rows = result_df.collect()

    assert len(rows) == 2

    old_row = next(
        row
        for row in rows
        if row.customer_key == "old-customer-key"
    )

    new_row = next(
        row
        for row in rows
        if row.customer_key != "old-customer-key"
    )

    # Old version should be expired
    assert old_row.customer_id == "100"
    assert old_row.city == "Agra"
    assert old_row.is_current is False
    assert old_row.effective_to is not None

    # New version should become current
    assert new_row.customer_id == "100"
    assert new_row.city == "Jaipur"
    assert new_row.is_current is True
    assert new_row.effective_to is None

    # Changed version should have a new surrogate key
    assert new_row.customer_key != old_row.customer_key

