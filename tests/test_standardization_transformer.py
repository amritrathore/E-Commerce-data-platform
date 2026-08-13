from core.spark_session_manager import SparkSessionManager
from framework.transformation.standardization_transformer import StandardizationTransformer

def test_standardization_transformer_standardizes_values():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", "M"),
            ("C002", "male"),
            ("C003", "F"),
            ("C004", "female"),
        ],
        [
            "customer_id",
            "gender",
        ],
    )

    transformer = StandardizationTransformer(
        mappings={
            "gender": {
                "m": "Male",
                "male": "Male",
                "f": "Female",
                "female": "Female",
            }
        }
    )

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.orderBy("customer_id").collect()

    assert rows[0]["gender"] == "Male"
    assert rows[1]["gender"] == "Male"
    assert rows[2]["gender"] == "Female"
    assert rows[3]["gender"] == "Female"


def test_standardization_transformer_handles_case_and_whitespace():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", " m "),
            ("C002", "MALE"),
            ("C003", " f "),
            ("C004", "FEMALE"),
        ],
        [
            "customer_id",
            "gender",
        ],
    )

    transformer = StandardizationTransformer(
        mappings={
            "gender": {
                "m": "Male",
                "male": "Male",
                "f": "Female",
                "female": "Female",
            }
        }
    )

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.orderBy("customer_id").collect()

    assert rows[0]["gender"] == "Male"
    assert rows[1]["gender"] == "Male"
    assert rows[2]["gender"] == "Female"
    assert rows[3]["gender"] == "Female"


def test_standardization_transformer_preserves_unmapped_values():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", "Unknown"),
            ("C002", "Other"),
        ],
        [
            "customer_id",
            "gender",
        ],
    )

    transformer = StandardizationTransformer(
        mappings={
            "gender": {
                "m": "Male",
                "male": "Male",
                "f": "Female",
                "female": "Female",
            }
        }
    )

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.orderBy("customer_id").collect()

    assert rows[0]["gender"] == "Unknown"
    assert rows[1]["gender"] == "Other"


def test_standardization_transformer_preserves_null():

    spark = SparkSessionManager.get_session()

    df = spark.createDataFrame(
        [
            ("C001", None),
        ],
        schema="customer_id STRING, gender STRING"
    )

    transformer = StandardizationTransformer(
        mappings={
            "gender": {
                "m": "Male",
                "male": "Male",
                "f": "Female",
                "female": "Female",
            }
        }
    )

    result = transformer.transform(
        df,
        "customers"
    )

    row = result.first()

    assert row["gender"] is None


def test_standardization_transformer_ignores_missing_column():

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

    transformer = StandardizationTransformer(
        mappings={
            "gender": {
                "m": "Male",
                "f": "Female",
            }
        }
    )

    result = transformer.transform(
        df,
        "customers"
    )

    assert result.columns == df.columns
    assert result.count() == df.count()