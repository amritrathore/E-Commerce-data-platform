import pytest

from core.spark_session_manager import SparkSessionManager
from framework.validation.duplicate_validator import DuplicateValidator


def test_duplicate_validator_rejects_duplicate_records():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit"),
        ("C002", "John"),
        ("C003", "Sarah"),
        ("C002", "Mike"),
        ("C004", "David"),
    ]

    columns = [
        "customer_id",
        "first_name",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    validator = DuplicateValidator()

    result = validator.validate(
        df,
        "customers"
    )

    assert result.failed_count == 1

    assert result.valid_df.count() == 4
    assert result.invalid_df.count() == 1


def test_duplicate_validator_keeps_first_occurrence():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit"),
        ("C001", "Amrit Duplicate"),
    ]

    columns = [
        "customer_id",
        "first_name",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    validator = DuplicateValidator()

    result = validator.validate(
        df,
        "customers"
    )

    valid_records = result.valid_df.collect()
    invalid_records = result.invalid_df.collect()

    assert len(valid_records) == 1
    assert len(invalid_records) == 1

    assert valid_records[0]["first_name"] == "Amrit"
    assert invalid_records[0]["first_name"] == "Amrit Duplicate"


def test_duplicate_validator_returns_no_invalid_records_when_unique():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit"),
        ("C002", "John"),
        ("C003", "Sarah"),
    ]

    columns = [
        "customer_id",
        "first_name",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    validator = DuplicateValidator()

    result = validator.validate(
        df,
        "customers"
    )

    assert result.failed_count == 0
    assert result.valid_df.count() == 3
    assert result.invalid_df.count() == 0


def test_duplicate_validator_missing_key_column():

    spark = SparkSessionManager.get_session()

    data = [
        ("Amrit",),
        ("John",),
    ]

    columns = [
        "first_name",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    validator = DuplicateValidator()

    with pytest.raises(ValueError, match="Duplicate key columns not found"):
        validator.validate(
            df,
            "customers"
        )


def test_duplicate_validator_no_keys_configured():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit"),
        ("C001", "Amrit Duplicate"),
    ]

    columns = [
        "customer_id",
        "first_name",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    # Categories currently has no validation.duplicate_keys configuration.
    validator = DuplicateValidator()

    result = validator.validate(
        df,
        "categories"
    )

    assert result.failed_count == 0
    assert result.valid_df.count() == 2
    assert result.invalid_df.count() == 0