from core.spark_session_manager import SparkSessionManager
from framework.validation.mandatory_validator import MandatoryValidator
from framework.validation.duplicate_validator import DuplicateValidator
from framework.validation.validator_engine import ValidatorEngine


def test_validator_engine_runs_multiple_validators():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit", "amrit@test.com"),
        ("C002", "John", "john@test.com"),
        ("C002", "John Duplicate", "duplicate@test.com"),
        ("C003", None, "test@test.com"),
        ("C004", "Sarah", "sarah@test.com"),
    ]

    columns = [
        "customer_id",
        "first_name",
        "email",
    ]

    df = spark.createDataFrame(data, columns)

    engine = ValidatorEngine(
        validators=[
            MandatoryValidator(),
            DuplicateValidator(),
        ]
    )


    result = engine.validate(df, "customers")

    assert result.failed_count == 2
    assert result.valid_df.count() == 3
    assert result.invalid_df.count() == 2


def test_validator_engine_passes_valid_records_to_next_validator():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit", "amrit@test.com"),
        ("C001", None, "missing-name@test.com"),
        ("C002", "John", "john@test.com"),
        ("C003", "Sarah", "sarah@test.com"),
    ]

    columns = [
        "customer_id",
        "first_name",
        "email",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    engine = ValidatorEngine(
        validators=[
            MandatoryValidator(),
            DuplicateValidator(),
        ]
    )

    result = engine.validate(
        df,
        "customers"
    )

    assert result.failed_count == 1
    assert result.valid_df.count() == 3
    assert result.invalid_df.count() == 1


def test_validator_engine_with_no_validators():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit"),
        ("C002", "John"),
    ]

    columns = [
        "customer_id",
        "first_name",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    engine = ValidatorEngine(
        validators=[]
    )

    result = engine.validate(
        df,
        "customers"
    )

    assert result.failed_count == 0
    assert result.valid_df.count() == 2
    assert result.invalid_df.count() == 0


def test_validator_engine_with_single_validator():

    spark = SparkSessionManager.get_session()

    data = [
        ("C001", "Amrit", "amrit@test.com"),
        ("C002", None, "john@test.com"),
        ("C003", "Sarah", "sarah@test.com"),
    ]

    columns = [
        "customer_id",
        "first_name",
        "email",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    engine = ValidatorEngine(
        validators=[
            MandatoryValidator()
        ]
    )

    result = engine.validate(
        df,
        "customers"
    )

    assert result.failed_count == 1
    assert result.valid_df.count() == 2
    assert result.invalid_df.count() == 1
