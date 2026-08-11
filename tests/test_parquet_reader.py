from pathlib import Path
import pytest

from framework.reader.parquet_reader import ParquetReader
from core.spark_session_manager import SparkSessionManager

class TestConfig:

    def __init__(self, dataset):
        self.dataset = dataset


    def get_dataset(self, dataset_name):
        return self.dataset



def test_parquet_reader_reads_bronze_layer(tmp_path):

    spark = SparkSessionManager.get_session()

    source_path = str(
        Path(tmp_path) / "bronze" / "customers"
    )

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

    df.write.mode("overwrite").parquet(
        source_path
    )

    config = TestConfig(
        {
            "bronze": source_path
        }
    )

    reader = ParquetReader(
        layer="bronze",
        config=config,
        spark=spark
    )

    result = reader.read("customers")

    assert result.count() == 2
    assert result.columns == columns 


def test_parquet_reader_reads_configured_layer(tmp_path):

    spark = SparkSessionManager.get_session()

    source_path = str(
        Path(tmp_path) / "silver" / "customers"
    )

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

    df.write.mode("overwrite").parquet(
        source_path
    )

    config = TestConfig(
        {
            "silver": source_path
        }
    )

    reader = ParquetReader(
        layer="silver",
        config=config,
        spark=spark
    )

    result = reader.read("customers")

    assert result.count() == 2


def test_parquet_reader_missing_layer(tmp_path):

    config = TestConfig(
        {
            "bronze": str(tmp_path / "bronze")
        }
    )

    reader = ParquetReader(
        layer="silver",
        config=config,
        spark=SparkSessionManager.get_session()
    )

    with pytest.raises(
        ValueError,
        match="Layer 'silver' not found"
    ):
        reader.read("customers")