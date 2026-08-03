from schemas.customers_schema import customer_schema
from pyspark.sql.types import StructType

class SchemaRegistry:

    _schemas = {
        "customer_schema": customer_schema
    }


    @classmethod
    def get(cls, schema_name: str):

        if schema_name not in cls._schemas:
            raise ValueError(f"Schema '{schema_name}' not found.")

        return cls._schemas[schema_name]


    @classmethod
    def register(cls, name: str, schema: StructType) -> None:
        if name in cls._schemas:
            raise ValueError(f"Schema '{name}' is already registered.")

        cls._schemas[name] = schema