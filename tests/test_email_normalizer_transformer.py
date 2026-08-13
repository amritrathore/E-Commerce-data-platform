import pytest

from core.spark_session_manager import SparkSessionManager
from framework.transformation.email_normalizer_transformer import EmailNormalizerTransformer


def test_email_normalizer_trims_and_lowercases():

    spark = SparkSessionManager.get_session()

    data = [
        ("  AMRIT@Example.COM  ",),
        ("John@Example.COM",),
        ("  SARAH@test.COM",),
    ]

    columns = ["email"]

    df = spark.createDataFrame(
        data,
        columns
    )

    transformer = EmailNormalizerTransformer()

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.collect()

    assert rows[0].email == "amrit@example.com"
    assert rows[1].email == "john@example.com"
    assert rows[2].email == "sarah@test.com"


def test_email_normalizer_preserves_null():

    spark = SparkSessionManager.get_session()

    data = [
        ("  AMRIT@Example.COM  ",),
        (None,),
    ]

    df = spark.createDataFrame(
        data,
        ["email"]
    )

    transformer = EmailNormalizerTransformer()

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.collect()

    assert rows[0].email == "amrit@example.com"
    assert rows[1].email is None


def test_email_normalizer_missing_column():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", "Amrit"),
        ],
        [
            "customer_id",
            "first_name",
        ]
    )

    transformer = EmailNormalizerTransformer()

    with pytest.raises(
        ValueError,
        match="Email column 'email' not found"
    ):
        transformer.transform(
            df,
            "customers"
        )