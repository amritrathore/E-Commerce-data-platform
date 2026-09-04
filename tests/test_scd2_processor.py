from datetime import datetime

from core.spark_session_manager import SparkSessionManager
from framework.gold.scd2_processor import SCD2Processor

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    BooleanType,
)

scd_schema = StructType([
    StructField("customer_key", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("effective_from", TimestampType(), False),
    StructField("effective_to", TimestampType(), True),
    StructField("is_current", BooleanType(), False),
])


def test_scd2_initial_load_returns_incoming_dataframe():

    spark = SparkSessionManager.get_session()

    incoming_df = spark.createDataFrame(
        [
            (
                "key-100",
                "100",
                "Priya",
                True,
            )
        ],
        [
            "customer_key",
            "customer_id",
            "first_name",
            "is_current",
        ],
    )

    processor = SCD2Processor()

    result_df = processor.process(
        incoming_df=incoming_df,
        existing_df=None,
        business_key="customer_id",
    )

    rows = result_df.collect()

    assert len(rows) == 1
    assert rows[0].customer_id == "100"


def test_scd2_inserts_new_customer():
    spark = SparkSessionManager.get_session()

    processor = SCD2Processor()


    existing_df = spark.createDataFrame(
        [
            (
                "key-100",
                "100",
                "Priya",
                datetime(2026, 9, 1, 10, 0, 0),
                None,
                True,
            )
        ],
        schema=scd_schema,
    )

    incoming_df = spark.createDataFrame(
        [
            (
                "key-100",
                "100",
                "Priya",
                datetime(2026, 9, 1, 10, 0, 0),
                None,
                True,
            ),
            (
                "key-101",
                "101",
                "Garima",
                datetime(2026, 9, 1, 11, 0, 0),
                None,
                True,
            ),
        ],
       schema=scd_schema,
    )

    result_df = processor.process(
        incoming_df=incoming_df,
        existing_df=existing_df,
        business_key="customer_id",
    )

    rows = result_df.collect()

    assert len(rows) == 2

    rows_by_customer = {
        row.customer_id: row
        for row in rows
    }

    assert "100" in rows_by_customer
    assert "101" in rows_by_customer

    assert rows_by_customer["101"].is_current is True


def test_scd2_keeps_unchanged_customer():

    spark = SparkSessionManager.get_session()

    processor = SCD2Processor()

    existing_df = spark.createDataFrame(
        [
            (
                "key-100",
                "100",
                "Priya",
                datetime(2026, 9, 1, 10, 0, 0),
                None,
                True,
            )
        ],
        schema=scd_schema,
    )

    incoming_df = spark.createDataFrame(
        [
            (
                "key-100",
                "100",
                "Priya",
                datetime(2026, 9, 1, 11, 0, 0),
                None,
                True,
            )
        ],
        schema=scd_schema,
    )

    result_df = processor.process(
        incoming_df=incoming_df,
        existing_df=existing_df,
        business_key="customer_id",
    )

    rows = result_df.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row.customer_id == "100"
    assert row.customer_key == "key-100"

    assert row.is_current is True
    assert row.effective_to is None


def test_scd2_expires_old_version_and_inserts_new_version():

    spark = SparkSessionManager.get_session()

    processor = SCD2Processor()

    existing_df = spark.createDataFrame(
        [
            (
                "key-old",
                "100",
                "Priya",
                datetime(2026, 9, 1, 10, 0, 0),
                None,
                True,
            )
        ],
        schema=scd_schema,
    )

    incoming_df = spark.createDataFrame(
        [
            (
                "key-new",
                "100",
                "Priya",
                datetime(2026, 9, 1, 11, 0, 0),
                None,
                True,
            )
        ],
        schema=scd_schema,
    )

    result_df = processor.process(
        incoming_df=incoming_df,
        existing_df=existing_df,
        business_key="customer_id",
    )

    rows = result_df.collect()

    assert len(rows) == 2

    old_row = next(
        row
        for row in rows
        if row.customer_key == "key-old"
    )

    new_row = next(
        row
        for row in rows
        if row.customer_key == "key-new"
    )

    # Old version becomes history
    assert old_row.customer_id == "100"
    assert old_row.is_current is False
    assert old_row.effective_to is not None

    # New version becomes current
    assert new_row.customer_id == "100"
    assert new_row.is_current is True
    assert new_row.effective_to is None


def test_scd2_preserves_existing_history():

    spark = SparkSessionManager.get_session()

    processor = SCD2Processor()

    existing_df = spark.createDataFrame(
        [
            (
                "key-v1",
                "100",
                "Priya",
                datetime(2026, 8, 1, 10, 0, 0),
                datetime(2026, 9, 1, 10, 0, 0),
                False,
            ),
            (
                "key-v2",
                "100",
                "Priya",
                datetime(2026, 9, 1, 10, 0, 0),
                None,
                True,
            ),
        ],
        schema=scd_schema,
    )

    incoming_df = spark.createDataFrame(
        [
            (
                "key-v3",
                "100",
                "Priya",
                datetime(2026, 9, 2, 10, 0, 0),
                None,
                True,
            )
        ],
        schema=scd_schema,
    )

    result_df = processor.process(
        incoming_df=incoming_df,
        existing_df=existing_df,
        business_key="customer_id",
    )

    rows = result_df.collect()

    assert len(rows) == 3

    rows_by_key = {
        row.customer_key: row
        for row in rows
    }

    assert rows_by_key["key-v1"].is_current is False
    assert rows_by_key["key-v1"].effective_to is not None

    assert rows_by_key["key-v2"].is_current is False
    assert rows_by_key["key-v2"].effective_to is not None

    assert rows_by_key["key-v3"].is_current is True
    assert rows_by_key["key-v3"].effective_to is None