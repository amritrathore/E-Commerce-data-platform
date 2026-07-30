import pytest

from framework.writer.parquet_writer import ParquetWriter


def test_invalid_layer():

    writer = ParquetWriter()

    with pytest.raises(ValueError):
        writer.write(
            None,
            "customers",
            "invalid_layer"
        )

def test_customer_bronze_path_exists():

    writer = ParquetWriter()

    dataset = writer.config.get_dataset("customers")

    bronze_path = dataset.get("bronze")

    assert bronze_path == "data/bronze/customers"