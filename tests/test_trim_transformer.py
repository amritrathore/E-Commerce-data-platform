from core.spark_session_manager import SparkSessionManager

from framework.transformation.trim_transformer import TrimTransformer


def test_trim_transformer_trims_string_columns():

    spark = SparkSessionManager.get_session()

    data = [
        (" C001 ", " Amrit ", " amrit@test.com "),
        (" C002", "John", " john@test.com"),
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

    transformer = TrimTransformer()

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.collect()

    assert rows[0].customer_id == "C001"
    assert rows[0].first_name == "Amrit"
    assert rows[0].email == "amrit@test.com"

    assert rows[1].customer_id == "C002"
    assert rows[1].first_name == "John"
    assert rows[1].email == "john@test.com"


def test_trim_transformer_does_not_modify_numeric_columns():

    spark = SparkSessionManager.get_session()

    data = [
        (" C001 ", 100),
        (" C002 ", 200),
    ]

    columns = [
        "customer_id",
        "quantity",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    transformer = TrimTransformer()

    result = transformer.transform(
        df,
        "customers"
    )

    rows = result.collect()

    assert rows[0].customer_id == "C001"
    assert rows[0].quantity == 100

    assert rows[1].customer_id == "C002"
    assert rows[1].quantity == 200