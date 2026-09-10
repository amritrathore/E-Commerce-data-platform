from core.spark_session_manager import SparkSessionManager
from framework.reader.csv_reader import CsvReader
from framework.schema.schema_registry import SchemaRegistry
from pyspark.sql.types import StructType


def test_customer_schema_exists():

    schema = SchemaRegistry.get("customer_schema")

    assert schema is not None
    assert isinstance(schema, StructType)


def test_invalid_schema():

    import pytest

    with pytest.raises(ValueError):
        SchemaRegistry.get("invalid_schema")


def test__product_schema_exists():

    schema = SchemaRegistry.get("product_schema")

    assert schema is not None
    assert isinstance(schema, StructType)


def test_product_schema_exists():

    schema = SchemaRegistry.get("product_schema")

    assert schema is not None
    assert len(schema.fields) == 10


def test_product_schema_contains_expected_columns():

    schema = SchemaRegistry.get("product_schema")

    column_names = [
        field.name
        for field in schema.fields
    ]

    assert column_names == [
        "product_id",
        "sku",
        "product_name",
        "brand",
        "description",
        "listing_date",
        "update_date",
        "is_active",
        "category_id",
        "sub_category_id",
    ]


def test_csv_reader_reads_products():

    spark = SparkSessionManager.get_session()

    reader = CsvReader()

    df = reader.read("products")

    assert df is not None

    assert df.columns == [
        "product_id",
        "sku",
        "product_name",
        "brand",
        "description",
        "listing_date",
        "update_date",
        "is_active",
        "category_id",
        "sub_category_id",
    ]

    assert df.count() > 0


def test_products_csv_schema_types():

    reader = CsvReader()

    df = reader.read("products")

    schema_by_name = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    assert schema_by_name["product_id"] == "string"
    assert schema_by_name["sku"] == "string"
    assert schema_by_name["listing_date"] == "timestamp"
    assert schema_by_name["update_date"] == "timestamp"
    assert schema_by_name["is_active"] == "boolean"