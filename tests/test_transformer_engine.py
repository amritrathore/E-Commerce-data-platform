from core.spark_session_manager import SparkSessionManager

from framework.transformation.trim_transformer import TrimTransformer
from framework.transformation.transformer_engine import TransformerEngine


def test_transformer_engine_executes_transformers():

    spark = SparkSessionManager.get_session()

    data = [
        (" C001 ", " Amrit "),
        (" C002 ", " John "),
    ]

    columns = [
        "customer_id",
        "first_name",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    engine = TransformerEngine(
        transformers=[
            TrimTransformer()
        ]
    )

    result = engine.transform(
        df,
        "customers"
    )

    rows = result.collect()

    assert rows[0].customer_id == "C001"
    assert rows[0].first_name == "Amrit"

    assert rows[1].customer_id == "C002"
    assert rows[1].first_name == "John"