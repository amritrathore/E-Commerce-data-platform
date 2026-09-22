from datetime import datetime
from decimal import Decimal

from core.spark_session_manager import SparkSessionManager
from pipeline.bronze.bronze_pipeline import BronzePipeline

class FakeReader:

    def __init__(self, df):
        self.df = df

    def read(self, dataset_name):
        return self.df

class FakeWriter:

    def __init__(self):
        self.written_df = None
        self.dataset_name = None
        self.layer = None

    def write(self, df, dataset_name, layer):
        self.written_df = df
        self.dataset_name = dataset_name
        self.layer = layer


def test_bronze_pipeline_creation():
    pipeline = BronzePipeline()

    assert pipeline.reader is not None
    assert pipeline.writer is not None


def test_bronze_pipeline_categories():

    spark = SparkSessionManager.get_session()

    categories_df = spark.createDataFrame(
        [
            (
                "CAT01",
                "Electronics",
                True,
                datetime(2024, 1, 1, 9, 00, 00),
                datetime(2026, 8, 20, 10, 30, 00),
            )
        ],
        [
            "category_id",
            "category_name",
            "is_active",
            "created_date",
            "updated_date",
        ],
    )

    reader = FakeReader(categories_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("categories")

    assert writer.dataset_name == "categories"
    assert writer.layer == "bronze"
    assert writer.written_df is not None

    assert writer.written_df.count() == 1
    
    assert "category_id" in writer.written_df.columns
    assert "category_name" in writer.written_df.columns
    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns


def test_bronze_pipeline_customers():

    spark = SparkSessionManager.get_session()

    customers_df = spark.createDataFrame(
        [
            (
                "100",
                "Amrit",
                "Rathore",
                "Jaipur",
                "Rajasthan",
                "12 MG Road",
                "Home",
                datetime(2024, 1, 1, 9, 0, 0),
                "amrit@example.com",
                "9876543210",
                "Male",
                datetime(1990, 5, 10, 0, 0, 0),
                datetime(2026, 8, 20, 10, 30, 0),
                True,
                "302001",
                "India",
            )
        ],
        [
            "customer_id",
            "first_name",
            "last_name",
            "city",
            "state",
            "address",
            "address_type",
            "signup_datetime",
            "email",
            "phone_number",
            "gender",
            "date_of_birth",
            "last_modified_date",
            "is_active",
            "postal_code",
            "country",
        ],
    )

    reader = FakeReader(customers_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("customers")

    assert writer.dataset_name == "customers"
    assert writer.layer == "bronze"
    assert writer.written_df is not None
    assert writer.written_df.count() == 1

    assert "customer_id" in writer.written_df.columns
    assert "email" in writer.written_df.columns

    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns


def test_bronze_pipeline_order_items():

    spark = SparkSessionManager.get_session()

    order_items_df = spark.createDataFrame(
        [
            (
                "OI1001",
                "ORD1001",
                "P1001",
                2,
                "PR5001",
                10,
                18,
                datetime(2026, 7, 1, 10, 5, 0),
                datetime(2026, 7, 1, 11, 0, 0),
            )
        ],
        [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "price_id",
            "discount_percent",
            "tax_percent",
            "created_date",
            "updated_date",
        ],
    )

    reader = FakeReader(order_items_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("order_items")

    assert writer.dataset_name == "order_items"
    assert writer.layer == "bronze"
    assert writer.written_df is not None
    assert writer.written_df.count() == 1

    assert "order_item_id" in writer.written_df.columns
    assert "price_id" in writer.written_df.columns

    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns


def test_bronze_pipeline_orders():

    spark = SparkSessionManager.get_session()

    orders_df = spark.createDataFrame(
        [
            (
                "ORD1001",
                "100",
                datetime(2026, 7, 1, 10, 0, 0),
                "Delivered",
                "UPI",
                "12 MG Road",
                "Jaipur",
                "Rajasthan",
                "302001",
                "India",
                Decimal("2499.00"),
                "INR",
                datetime(2026, 7, 2, 12, 0, 0),
            )
        ],
        [
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "payment_method",
            "shipping_address",
            "shipping_city",
            "shipping_state",
            "shipping_postal_code",
            "shipping_country",
            "total_amount",
            "currency",
            "update_date",
        ],
    )

    reader = FakeReader(orders_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("orders")

    assert writer.dataset_name == "orders"
    assert writer.layer == "bronze"
    assert writer.written_df is not None
    assert writer.written_df.count() == 1

    assert "order_id" in writer.written_df.columns
    assert "customer_id" in writer.written_df.columns

    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns


def test_bronze_pipeline_product_prices():

    spark = SparkSessionManager.get_session()

    product_prices_df = spark.createDataFrame(
        [
            (
                "PR5001",
                "P1001",
                Decimal("799.00"),
                "INR",
                datetime(2024, 1, 1, 9, 0, 0),
                datetime(2026, 8, 20, 10, 0, 0),
            )
        ],
        [
            "price_id",
            "product_id",
            "price",
            "currency",
            "created_date",
            "updated_date",
        ],
    )

    reader = FakeReader(product_prices_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("product_prices")

    assert writer.dataset_name == "product_prices"
    assert writer.layer == "bronze"
    assert writer.written_df is not None
    assert writer.written_df.count() == 1

    assert "price_id" in writer.written_df.columns
    assert "product_id" in writer.written_df.columns

    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns


def test_bronze_pipeline_products():

    spark = SparkSessionManager.get_session()

    product_df = spark.createDataFrame(
        [
            (
                "P1001",
                "LOG-M185",
                "Wireless Mouse",
                "Logitech",
                "Ergonomic wireless mouse",
                datetime(2024, 1, 10, 10, 15),
                datetime(2026, 8, 20, 14, 30),
                True,
                "CAT01",
                "SUBCAT01",
            )
        ],
        [
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
        ],
    )

    reader = FakeReader(product_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("products")

    assert writer.dataset_name == "products"
    assert writer.layer == "bronze"
    assert writer.written_df is not None

    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns


def test_bronze_pipeline_sub_categories():

    spark = SparkSessionManager.get_session()

    sub_categories_df = spark.createDataFrame(
        [
            (
                "SUBCAT01",
                "CAT01",
                "Computer Accessories",
                True,
                datetime(2024, 1, 10, 9, 0, 0),
                datetime(2026, 8, 20, 10, 0, 0),
            )
        ],
        [
            "sub_category_id",
            "category_id",
            "sub_category_name",
            "is_active",
            "created_date",
            "updated_date",
        ],
    )

    reader = FakeReader(sub_categories_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("sub_categories")

    assert writer.dataset_name == "sub_categories"
    assert writer.layer == "bronze"
    assert writer.written_df is not None
    assert writer.written_df.count() == 1

    assert "sub_category_id" in writer.written_df.columns
    assert "category_id" in writer.written_df.columns

    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns


def test_bronze_pipeline_wishlist():

    spark = SparkSessionManager.get_session()

    wishlist_df = spark.createDataFrame(
        [
            (
                "WL1001",
                "100",
                "P1001",
                datetime(2026, 7, 1, 9, 0, 0),
                datetime(2026, 7, 1, 10, 0, 0),
                True,
            )
        ],
        [
            "wishlist_id",
            "customer_id",
            "product_id",
            "created_date",
            "updated_date",
            "is_active",
        ],
    )

    reader = FakeReader(wishlist_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("wishlist")

    assert writer.dataset_name == "wishlist"
    assert writer.layer == "bronze"
    assert writer.written_df is not None
    assert writer.written_df.count() == 1

    assert "wishlist_id" in writer.written_df.columns
    assert "product_id" in writer.written_df.columns

    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns