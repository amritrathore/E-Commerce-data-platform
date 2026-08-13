from pyspark.sql.types import TimestampType

from core.spark_session_manager import SparkSessionManager
from framework.transformation.date_normalizer_transformer import DateNormalizerTransformer


def test_date_normalizer_converts_string_to_timestamp():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", "2026-08-01 10:30:00"),
            ("C002", "2026-08-02 15:45:00"),
        ],
        [
            "customer_id",
            "signup_datetime",
        ],
    )

    transformer = DateNormalizerTransformer(
        columns=["signup_datetime"]
    )

    result = transformer.transform(
        df,
        "customers"
    )

    assert isinstance(
        result.schema["signup_datetime"].dataType,
        TimestampType,
    )


def test_date_normalizer_preserves_null():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", "2026-08-01 10:30:00"),
            ("C002", None),
        ],
        [
            "customer_id",
            "signup_datetime",
        ],
    )

    transformer = DateNormalizerTransformer(
        columns=["signup_datetime"]
    )

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.orderBy("customer_id").collect()

    assert rows[0]["signup_datetime"] is not None
    assert rows[1]["signup_datetime"] is None


def test_date_normalizer_invalid_date_becomes_null():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", "2026-08-01 10:30:00"),
            ("C002", "invalid-date"),
        ],
        [
            "customer_id",
            "signup_datetime",
        ],
    )

    transformer = DateNormalizerTransformer(
        columns=["signup_datetime"]
    )

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.orderBy("customer_id").collect()

    assert rows[0]["signup_datetime"] is not None
    assert rows[1]["signup_datetime"] is None


def test_date_normalizer_ignores_missing_column():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", "Amrit"),
        ],
        [
            "customer_id",
            "first_name",
        ],
    )

    transformer = DateNormalizerTransformer(
        columns=["signup_datetime"]
    )

    result = transformer.transform(
        df,
        "customers"
    )

    assert result.columns == df.columns
    assert result.count() == df.count()