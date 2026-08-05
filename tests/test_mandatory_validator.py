from framework.validation.mandatory_validator import MandatoryValidator
from core.spark_session_manager import SparkSessionManager
import pytest


def test_mandatory_validator():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit", "amrit@test.com"),
        ("C002", "John", None),
        ("C003", None, "test@test.com"),
        ("C004", "Sarah", "sarah@test.com"),
    ]

    columns = [
        "customer_id",
        "first_name",
        "email"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    validator = MandatoryValidator()

    result = validator.validate(
        df,
        "customers"
    )

    assert result.failed_count == 2

    assert result.valid_df.count() == 2
    assert result.invalid_df.count() == 2



def test_mandatory_validator_rejects_blank_values():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit", "amrit@test.com"),
        ("C002", "   ", "john@test.com"),
        ("C003", "Sarah", ""),
    ]

    columns = [
        "customer_id",
        "first_name",
        "email"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    validator = MandatoryValidator()

    result = validator.validate(
        df,
        "customers"
    )

    assert result.failed_count == 2



def test_mandatory_validator_missing_column():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", "Amrit")
        ],
        [
            "customer_id",
            "first_name"
        ]
    )

    validator = MandatoryValidator()

    with pytest.raises(ValueError):

        validator.validate(
            df,
            "customers"
        )