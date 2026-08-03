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