from core.spark_session_manager import SparkSessionManager
from framework.reader.csv_reader import CsvReader

from schemas.categories_schema import categories_schema
from schemas.customer_schema import customer_schema
from schemas.order_items_schema import order_items_schema
from schemas.orders_schema import orders_schema
from schemas.product_prices_schema import product_prices_schema
from schemas.product_schema import product_schema
from schemas.sub_categories_schema import sub_categories_schema
from schemas.wishlist_schema import wishlist_schema



def test_read_customers_returns_dataframe():

    reader = CsvReader()

    df = reader.read("customers")

    assert df is not None


def test_customer_schema_applied():

    reader = CsvReader()

    df = reader.read("customers")

    assert "customer_id" in df.columns
    assert len(df.schema.fields) == 16


def test_read_categories_returns_dataframe():

    reader = CsvReader()

    df = reader.read("categories")

    assert df is not None


def test_categories_schema_applied():

    reader = CsvReader()

    df = reader.read("categories")

    assert "category_id" in df.columns
    assert len(df.schema.fields) == 5


def test_read_orders_items_returns_dataframe():

    reader = CsvReader()

    df = reader.read("order_items")

    assert df is not None


def test_orders_items_schema_applied():

    reader = CsvReader()

    df = reader.read("order_items")

    assert "order_item_id" in df.columns
    assert len(df.schema.fields) == 9


def test_read_orders_returns_dataframe():

    reader = CsvReader()

    df = reader.read("orders")

    assert df is not None


def test_orders_schema_applied():

    reader = CsvReader()

    df = reader.read("orders")

    assert "order_id" in df.columns
    assert len(df.schema.fields) == 13


def test_read_product_prices_returns_dataframe():

    reader = CsvReader()

    df = reader.read("product_prices")

    assert df is not None


def test_product_prices_schema_applied():

    reader = CsvReader()

    df = reader.read("product_prices")

    assert "price_id" in df.columns
    assert len(df.schema.fields) == 6


def test_read_products_returns_dataframe():

    reader = CsvReader()

    df = reader.read("products")

    assert df is not None


def test_products_schema_applied():

    reader = CsvReader()

    df = reader.read("products")

    assert "product_id" in df.columns
    assert len(df.schema.fields) == 10


def test_read_sub_categories_returns_dataframe():

    reader = CsvReader()

    df = reader.read("sub_categories")

    assert df is not None


def test_sub_categories_schema_applied():

    reader = CsvReader()

    df = reader.read("sub_categories")

    assert "sub_category_id" in df.columns
    assert len(df.schema.fields) == 6


def test_read_wishlist_returns_dataframe():

    reader = CsvReader()

    df = reader.read("wishlist")

    assert df is not None


def test_wishlist_schema_applied():

    reader = CsvReader()

    df = reader.read("wishlist")

    assert "wishlist_id" in df.columns
    assert len(df.schema.fields) == 6


def test_categories_csv_reader():

    reader = CsvReader()

    df = reader.read("categories")

    assert df.count() > 0
    assert_schema_types(df.schema, categories_schema)


def test_customer_csv_reader():
    reader = CsvReader()

    df = reader.read("customers")

    assert df.count() > 0
    assert_schema_types(df.schema, customer_schema)


def test_order_items_csv_reader():
    reader = CsvReader()

    df = reader.read("order_items")

    assert df.count() > 0
    assert_schema_types(df.schema, order_items_schema)


def test_orders_csv_reader():
    reader = CsvReader()

    df = reader.read("orders")

    assert df.count() > 0
    assert_schema_types(df.schema, orders_schema)


def test_product_prices_csv_reader():
    reader = CsvReader()

    df = reader.read("product_prices")

    assert df.count() > 0
    assert_schema_types(df.schema, product_prices_schema)


def test_product_csv_reader():
    reader = CsvReader()

    df = reader.read("products")

    assert df.count() > 0
    assert_schema_types(df.schema, product_schema)


def test_sub_categories_csv_reader():
    reader = CsvReader()

    df = reader.read("sub_categories")

    assert df.count() > 0
    assert_schema_types(df.schema, sub_categories_schema)


def test_wishlist_csv_reader():
    reader = CsvReader()

    df = reader.read("wishlist")

    assert df.count() > 0
    assert_schema_types(df.schema, wishlist_schema)


def assert_schema_types(actual_schema, expected_schema):
    actual = [(field.name, field.dataType) for field in actual_schema.fields]
    expected = [(field.name, field.dataType) for field in expected_schema.fields]

    assert actual == expected