from schemas.categories_schema import categories_schema
from schemas.customer_schema import customer_schema
from schemas.order_items_schema import order_items_schema
from schemas.orders_schema import orders_schema
from schemas.product_prices_schema import product_prices_schema
from schemas.product_schema import product_schema
from schemas.sub_categories_schema import sub_categories_schema
from schemas.wishlist_schema import wishlist_schema
from pyspark.sql.types import StructType

class SchemaRegistry:

    _schemas = {
        "categories_schema": categories_schema,
        "customer_schema": customer_schema,
        "order_items_schema": order_items_schema,
        "orders_schema": orders_schema,
        "product_prices_schema": product_prices_schema,
        "product_schema": product_schema,
        "sub_categories_schema": sub_categories_schema,
        "wishlist_schema": wishlist_schema
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