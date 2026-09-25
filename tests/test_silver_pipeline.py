from datetime import datetime
from decimal import Decimal

from core.config_loader import ConfigLoader
from core.spark_session_manager import SparkSessionManager

from framework.validation.validator_engine import ValidatorEngine
from framework.validation.mandatory_validator import MandatoryValidator
from framework.validation.duplicate_validator import DuplicateValidator

from framework.transformation.transformer_provider import TransformerProvider

from pipeline.silver.silver_pipeline import SilverPipeline

from schemas.orders_schema import orders_schema


# ============================== COLUMN DEFINITIONS ==============================

categories_columns = [
    "category_id",
    "category_name",
    "is_active",
    "created_date",
    "updated_date",
]

customer_columns = [
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
]

order_items_columns = [
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "price_id",
    "discount_percent",
    "tax_percent",
    "created_date",
    "updated_date",
]

orders_columns = [
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
]

product_prices_columns = [
    "price_id",
    "product_id",
    "price",
    "currency",
    "created_date",
    "updated_date",
]

product_columns = [
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

sub_categories_columns = [
    "sub_category_id",
    "category_id",
    "sub_category_name",
    "is_active",
    "created_date",
    "updated_date",
]

wishlist_columns = [
    "wishlist_id",
    "customer_id",
    "product_id",
    "created_date",
    "updated_date",
    "is_active",
]


class FakeReader:

    def __init__(self, df):
        self.df = df

    def read(self, dataset_name):
        return self.df


class FakeWriter:

    def __init__(self):
        self.writes = {}

    def write(
        self,
        df,
        dataset_name,
        layer,
    ):
        self.writes[layer] = df

# ============================== CATEGORIES ==============================

def test_silver_pipeline_categories_mandatory_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "CAT01",
            "Electronics",
            True,
            datetime(2024, 1, 1, 9, 0),
            datetime(2026, 8, 20, 10, 30),
        ),
        (
            None,
            "Fashion",
            True,
            datetime(2024, 2, 1, 9, 0),
            datetime(2026, 8, 21, 10, 30),
        ),
        (
            "CAT03",
            None,
            True,
            datetime(2024, 3, 1, 9, 0),
            datetime(2026, 8, 22, 10, 30),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        categories_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("categories")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 1
    assert quarantine_df.count() == 2

    row = silver_df.collect()[0]

    assert row.category_id == "CAT01"
    assert row.category_name == "Electronics"

    quarantine_rows = quarantine_df.collect()

    assert any(
        row.category_id is None
        for row in quarantine_rows
    )

    assert any(
        row.category_name is None
        for row in quarantine_rows
    )


def test_silver_pipeline_categories_duplicate_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "CAT01",
            "Electronics",
            True,
            datetime(2024, 1, 1),
            datetime(2026, 8, 20),
        ),
        (
            "CAT02",
            "Fashion",
            True,
            datetime(2024, 2, 1),
            datetime(2026, 8, 21),
        ),
        (
            "CAT01",
            "Electronics Updated",
            True,
            datetime(2024, 3, 1),
            datetime(2026, 8, 22),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        categories_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        DuplicateValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("categories")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 2
    assert quarantine_df.count() == 1

    duplicate_row = quarantine_df.collect()[0]

    assert duplicate_row.category_id == "CAT01"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_categories_transformation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            " CAT01 ",
            " Electronics ",
            True,
            "2024-01-10 10:15",
            "2026-08-20 14:30",
        ),
        (
            " CAT02 ",
            " Fashion ",
            True,
            "2024-02-15 12:00",
            "2026-08-21 09:45",
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        categories_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(
        config_loader=config
    )

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider,
    )

    pipeline.run("categories")

    silver_df = writer.writes["silver"]

    rows = {
        row.category_id: row
        for row in silver_df.collect()
    }

    assert rows["CAT01"].category_name == "Electronics"
    assert rows["CAT02"].category_name == "Fashion"

    assert rows["CAT01"].created_date == datetime(
        2024, 1, 10, 10, 15
    )

    assert rows["CAT01"].updated_date == datetime(
        2026, 8, 20, 14, 30
    )

# ============================== CUSTOMERS ==============================

def test_silver_pipeline_customers_mandatory_validation():

    spark = SparkSessionManager.get_session()

    customer_data = [
        # Valid
        (
            "C001",
            "Amrit",
            "Rathore",
            "Jaipur",
            "Rajasthan",
            "12 MG Road",
            "Home",
            datetime(2024, 1, 1, 9, 0),
            "amrit@test.com",
            "9876543210",
            "Male",
            datetime(1990, 1, 1),
            datetime(2026, 8, 20, 10, 30),
            True,
            "302001",
            "India",
        ),

        # Invalid: customer_id missing
        (
            None,
            "John",
            "Doe",
            "Delhi",
            "Delhi",
            "20 Main Road",
            "Home",
            datetime(2024, 2, 1, 9, 0),
            "john@test.com",
            "9876543211",
            "Male",
            datetime(1992, 2, 2),
            datetime(2026, 8, 21, 10, 30),
            True,
            "110001",
            "India",
        ),

        # Invalid: first_name missing
        (
            "C003",
            None,
            "Smith",
            "Mumbai",
            "Maharashtra",
            "30 Market Road",
            "Home",
            datetime(2024, 3, 1, 9, 0),
            "smith@test.com",
            "9876543212",
            "Female",
            datetime(1993, 3, 3),
            datetime(2026, 8, 22, 10, 30),
            True,
            "400001",
            "India",
        ),

        # Invalid: email missing
        (
            "C004",
            "Sarah",
            "Jones",
            "Pune",
            "Maharashtra",
            "40 Park Road",
            "Home",
            datetime(2024, 4, 1, 9, 0),
            None,
            "9876543213",
            "Female",
            datetime(1994, 4, 4),
            datetime(2026, 8, 23, 10, 30),
            True,
            "411001",
            "India",
        ),
    ]

    bronze_df = spark.createDataFrame(
        customer_data,
        customer_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("customers")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 1
    assert quarantine_df.count() == 3

    silver_row = silver_df.collect()[0]

    assert silver_row.customer_id == "C001"
    assert silver_row.email == "amrit@test.com"

    quarantine_rows = quarantine_df.collect()

    assert any(row.customer_id is None for row in quarantine_rows)
    assert any(row.first_name is None for row in quarantine_rows)
    assert any(row.email is None for row in quarantine_rows)


def test_silver_pipeline_customers_duplicate_validation():

    spark = SparkSessionManager.get_session()

    customer_columns = [
        "customer_id",
        "first_name",
        "email",
    ]

    customer_data = [
        (
            "C001",
            "Amrit",
            "amrit@test.com",
        ),
        (
            "C002",
            "John",
            "john@test.com",
        ),
        (
            "C001",
            "Amrit Updated",
            "amrit.updated@test.com",
        ),
    ]

    bronze_df = spark.createDataFrame(
        customer_data,
        customer_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        DuplicateValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("customers")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 2
    assert quarantine_df.count() == 1

    duplicate_row = quarantine_df.collect()[0]

    assert duplicate_row.customer_id == "C001"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_customers_transformation():

    spark = SparkSessionManager.get_session()

    customer_columns = [
        "customer_id",
        "first_name",
        "email",
        "signup_datetime",
        "date_of_birth",
        "last_modified_date",
        "gender",
    ]

    customer_data = [
        (
            " C001 ",
            " Amrit ",
            " AMRIT@Test.COM ",
            "2024-01-01 09:00:00",
            "1990-01-01 00:00:00",
            "2026-08-20 10:30:00",
            "m",
        ),
        (
            " C002 ",
            " Sarah ",
            " SARAH@Test.COM ",
            "2024-02-01 10:00:00",
            "1992-02-02 00:00:00",
            "2026-08-21 11:30:00",
            "female",
        ),
    ]

    bronze_df = spark.createDataFrame(
        customer_data,
        customer_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(
        config_loader=config
    )

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider,
    )

    pipeline.run("customers")

    silver_df = writer.writes["silver"]

    rows = {
        row.customer_id: row
        for row in silver_df.collect()
    }

    row_1 = rows["C001"]

    assert row_1.first_name == "Amrit"
    assert row_1.email == "amrit@test.com"
    assert row_1.gender == "Male"

    assert row_1.signup_datetime == datetime(
        2024, 1, 1, 9, 0
    )

    assert row_1.date_of_birth == datetime(
        1990, 1, 1, 0, 0
    )

    assert row_1.last_modified_date == datetime(
        2026, 8, 20, 10, 30
    )

    row_2 = rows["C002"]

    assert row_2.first_name == "Sarah"
    assert row_2.email == "sarah@test.com"
    assert row_2.gender == "Female"

# ============================== SUB CATEGORIES ==============================

def test_silver_pipeline_sub_categories_mandatory_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "SUBCAT01",
            "CAT01",
            "Keyboards",
            True,
            datetime(2024, 1, 1),
            datetime(2026, 8, 20),
        ),
        (
            None,
            "CAT01",
            "Mouse",
            True,
            datetime(2024, 1, 2),
            datetime(2026, 8, 21),
        ),
        (
            "SUBCAT03",
            None,
            "Speakers",
            True,
            datetime(2024, 1, 3),
            datetime(2026, 8, 22),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        sub_categories_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("sub_categories")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 1
    assert quarantine_df.count() == 2

    row = silver_df.collect()[0]

    assert row.sub_category_id == "SUBCAT01"


def test_silver_pipeline_sub_categories_duplicate_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "SUBCAT01",
            "CAT01",
            "Keyboards",
            True,
            datetime(2024, 1, 1),
            datetime(2026, 8, 20),
        ),
        (
            "SUBCAT02",
            "CAT01",
            "Mouse",
            True,
            datetime(2024, 1, 2),
            datetime(2026, 8, 21),
        ),
        (
            "SUBCAT01",
            "CAT01",
            "Updated Keyboards",
            True,
            datetime(2024, 1, 3),
            datetime(2026, 8, 22),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        sub_categories_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        DuplicateValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("sub_categories")

    assert writer.writes["silver"].count() == 2
    assert writer.writes["quarantine"].count() == 1

    duplicate_row = writer.writes["quarantine"].collect()[0]

    assert duplicate_row.sub_category_id == "SUBCAT01"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_sub_categories_transformation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            " SUBCAT01 ",
            " CAT01 ",
            " Keyboards ",
            True,
            "2024-01-10 10:15",
            "2026-08-20 14:30",
        )
    ]

    bronze_df = spark.createDataFrame(
        data,
        sub_categories_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(
        config_loader=config
    )

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider,
    )

    pipeline.run("sub_categories")

    row = writer.writes["silver"].collect()[0]

    assert row.sub_category_id == "SUBCAT01"
    assert row.category_id == "CAT01"
    assert row.sub_category_name == "Keyboards"

    assert row.created_date == datetime(
        2024, 1, 10, 10, 15
    )

    assert row.updated_date == datetime(
        2026, 8, 20, 14, 30
    )

# ============================== PRODUCT PRICES ==============================

def test_silver_pipeline_product_prices_mandatory_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "PR5001",
            "P1001",
            Decimal("799.00"),
            "INR",
            datetime(2024, 1, 1),
            datetime(2026, 8, 20),
        ),
        (
            None,
            "P1002",
            Decimal("999.00"),
            "INR",
            datetime(2024, 1, 2),
            datetime(2026, 8, 21),
        ),
        (
            "PR5003",
            "P1003",
            None,
            "INR",
            datetime(2024, 1, 3),
            datetime(2026, 8, 22),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        product_prices_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("product_prices")

    assert writer.writes["silver"].count() == 1
    assert writer.writes["quarantine"].count() == 2


def test_silver_pipeline_product_prices_duplicate_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "PR5001",
            "P1001",
            Decimal("799.00"),
            "INR",
            datetime(2024, 1, 1),
            datetime(2026, 8, 20),
        ),
        (
            "PR5002",
            "P1002",
            Decimal("999.00"),
            "INR",
            datetime(2024, 1, 2),
            datetime(2026, 8, 21),
        ),
        (
            "PR5001",
            "P1001",
            Decimal("749.00"),
            "INR",
            datetime(2024, 1, 3),
            datetime(2026, 8, 22),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        product_prices_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        DuplicateValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("product_prices")

    assert writer.writes["silver"].count() == 2
    assert writer.writes["quarantine"].count() == 1

    duplicate_row = writer.writes["quarantine"].collect()[0]

    assert duplicate_row.price_id == "PR5001"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_product_prices_transformation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            " PR5001 ",
            " P1001 ",
            Decimal("799.00"),
            " INR ",
            "2024-01-10 10:15",
            "2026-08-20 14:30",
        )
    ]

    bronze_df = spark.createDataFrame(
        data,
        product_prices_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(
        config_loader=config
    )

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider,
    )

    pipeline.run("product_prices")

    row = writer.writes["silver"].collect()[0]

    assert row.price_id == "PR5001"
    assert row.product_id == "P1001"
    assert row.currency == "INR"

    assert row.created_date == datetime(
        2024, 1, 10, 10, 15
    )

    assert row.updated_date == datetime(
        2026, 8, 20, 14, 30
    )

# ============================== ORDERS ==============================

def test_silver_pipeline_orders_mandatory_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "ORD1001",
            "100",
            datetime(2026, 7, 1),
            "Delivered",
            "UPI",
            "12 MG Road",
            "Jaipur",
            "Rajasthan",
            "302020",
            "India",
            Decimal("2499.00"),
            "INR",
            datetime(2026, 7, 2),
        ),
        (
            "ORD1002",
            None,  # Invalid: customer_id is mandatory
            datetime(2026, 7, 2),
            "Delivered",
            "UPI",
            "20 Main Road",
            "Delhi",
            None,
            None,
            "India",
            Decimal("999.00"),
            "INR",
            datetime(2026, 7, 3),
        ),
        (
            "ORD1003",
            "100",
            None,  # Invalid: order_date is mandatory
            "Delivered",
            "UPI",
            "30 Market Road",
            "Mumbai",
            None,
            None,
            "India",
            Decimal("1999.00"),
            "INR",
            datetime(2026, 7, 4),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        orders_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("orders")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 1
    assert quarantine_df.count() == 2

    # Valid record
    row = silver_df.collect()[0]

    assert row.order_id == "ORD1001"
    assert row.customer_id == "100"
    assert row.order_date == datetime(2026, 7, 1)

    # Nullable fields can contain values
    assert row.shipping_state == "Rajasthan"
    assert row.shipping_postal_code == "302020"

    # Verify invalid records went to quarantine
    quarantine_rows = quarantine_df.collect()

    quarantine_order_ids = {
        row.order_id
        for row in quarantine_rows
    }

    assert "ORD1002" in quarantine_order_ids
    assert "ORD1003" in quarantine_order_ids


def test_silver_pipeline_orders_duplicate_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "ORD1001",
            "100",
            datetime(2026, 7, 1),
            "Placed",
            "UPI",
            "12 MG Road",
            "Jaipur",
            "Rajasthan",
            "302001",
            "India",
            Decimal("2499.00"),
            "INR",
            datetime(2026, 7, 2),
        ),
        (
            "ORD1002",
            "101",
            datetime(2026, 7, 2),
            "Delivered",
            "COD",
            "20 Main Road",
            "Delhi",
            "Delhi",
            "110001",
            "India",
            Decimal("999.00"),
            "INR",
            datetime(2026, 7, 3),
        ),
        (
            "ORD1001",
            "100",
            datetime(2026, 7, 1),
            "Delivered",
            "UPI",
            "12 MG Road",
            "Jaipur",
            "Rajasthan",
            "302001",
            "India",
            Decimal("2499.00"),
            "INR",
            datetime(2026, 7, 4),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        orders_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        DuplicateValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("orders")

    assert writer.writes["silver"].count() == 2
    assert writer.writes["quarantine"].count() == 1

    duplicate_row = writer.writes["quarantine"].collect()[0]

    assert duplicate_row.order_id == "ORD1001"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_orders_transformation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            " ORD1001 ",
            " 100 ",
            "2026-07-01 10:15",
            " Delivered ",
            " UPI ",
            " 12 MG Road ",
            " Jaipur ",
            " Rajasthan ",
            " 302001 ",
            " India ",
            Decimal("2499.00"),
            " INR ",
            "2026-07-02 14:30",
        )
    ]

    bronze_df = spark.createDataFrame(
        data,
        orders_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(
        config_loader=config
    )

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider,
    )

    pipeline.run("orders")

    row = writer.writes["silver"].collect()[0]

    assert row.order_id == "ORD1001"
    assert row.customer_id == "100"
    assert row.order_status == "Delivered"
    assert row.payment_method == "UPI"
    assert row.shipping_city == "Jaipur"
    assert row.currency == "INR"

    assert row.order_date == datetime(
        2026, 7, 1, 10, 15
    )

    assert row.update_date == datetime(
        2026, 7, 2, 14, 30
    )

# ============================== ORDER ITEMS ==============================

def test_silver_pipeline_order_items_mandatory_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "OI1001",
            "ORD1001",
            "P1001",
            2,
            "PR5001",
            10,
            18,
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
        ),
        (
            None,
            "ORD1001",
            "P1001",
            2,
            "PR5001",
            10,
            18,
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
        ),
        (
            "OI1003",
            "ORD1001",
            "P1001",
            None,
            "PR5001",
            10,
            18,
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        order_items_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("order_items")

    assert writer.writes["silver"].count() == 1
    assert writer.writes["quarantine"].count() == 2


def test_silver_pipeline_order_items_duplicate_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "OI1001",
            "ORD1001",
            "P1001",
            2,
            "PR5001",
            10,
            18,
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
        ),
        (
            "OI1002",
            "ORD1001",
            "P1002",
            1,
            "PR5002",
            0,
            18,
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
        ),
        (
            "OI1001",
            "ORD1002",
            "P1003",
            1,
            "PR5003",
            5,
            18,
            datetime(2026, 7, 3),
            datetime(2026, 7, 4),
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        order_items_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        DuplicateValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("order_items")

    assert writer.writes["silver"].count() == 2
    assert writer.writes["quarantine"].count() == 1

    duplicate_row = writer.writes["quarantine"].collect()[0]

    assert duplicate_row.order_item_id == "OI1001"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_order_items_transformation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            " OI1001 ",
            " ORD1001 ",
            " P1001 ",
            2,
            " PR5001 ",
            10,
            18,
            "2026-07-01 10:15",
            "2026-07-02 14:30",
        )
    ]

    bronze_df = spark.createDataFrame(
        data,
        order_items_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(
        config_loader=config
    )

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider,
    )

    pipeline.run("order_items")

    row = writer.writes["silver"].collect()[0]

    assert row.order_item_id == "OI1001"
    assert row.order_id == "ORD1001"
    assert row.product_id == "P1001"
    assert row.price_id == "PR5001"

    assert row.created_date == datetime(
        2026, 7, 1, 10, 15
    )

    assert row.updated_date == datetime(
        2026, 7, 2, 14, 30
    )

# ============================== PRODUCTS ==============================

def test_silver_pipeline_products_mandatory_validation():

    spark = SparkSessionManager.get_session()

    product_data = [
        # Valid
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
        ),

        # Invalid: product_id missing
        (
            None,
            "RD-K552",
            "Mechanical Keyboard",
            "Redragon",
            "RGB keyboard",
            datetime(2024, 2, 15, 12, 0),
            datetime(2026, 8, 21, 9, 45),
            True,
            "CAT01",
            "SUBCAT02",
        ),

        # Invalid: product_name missing
        (
            "P1003",
            "ANK-HUB7",
            None,
            "Anker",
            "USB-C hub",
            datetime(2024, 3, 1, 9, 30),
            datetime(2026, 8, 22, 11, 20),
            True,
            "CAT01",
            "SUBCAT03",
        ),

        # Invalid: category_id missing
        (
            "P1004",
            "NK-RUN-01",
            "Running Shoes",
            "Nike",
            "Lightweight running shoes",
            datetime(2024, 3, 20, 8, 45),
            datetime(2026, 8, 23, 16, 10),
            True,
            None,
            "SUBCAT04",
        ),
    ]

    bronze_df = spark.createDataFrame(
        product_data,
        product_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("products")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 1
    assert quarantine_df.count() == 3

    silver_row = silver_df.collect()[0]

    assert silver_row.product_id == "P1001"

    quarantine_rows = quarantine_df.collect()

    assert any(row.product_id is None for row in quarantine_rows)
    assert any(row.product_name is None for row in quarantine_rows)
    assert any(row.category_id is None for row in quarantine_rows)


def test_silver_pipeline_products_duplicate_validation():

    spark = SparkSessionManager.get_session()

    product_data = [
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
        ),
        (
            "P1002",
            "RD-K552",
            "Mechanical Keyboard",
            "Redragon",
            "RGB keyboard",
            datetime(2024, 2, 15, 12, 0),
            datetime(2026, 8, 21, 9, 45),
            True,
            "CAT01",
            "SUBCAT02",
        ),
        (
            "P1001",
            "LOG-M186",
            "Wireless Mouse Updated",
            "Logitech",
            "Duplicate product id",
            datetime(2024, 1, 10, 10, 15),
            datetime(2026, 8, 22, 10, 0),
            True,
            "CAT01",
            "SUBCAT01",
        ),
    ]

    bronze_df = spark.createDataFrame(
        product_data,
        product_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        DuplicateValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("products")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 2
    assert quarantine_df.count() == 1

    duplicate_row = quarantine_df.collect()[0]

    assert duplicate_row.product_id == "P1001"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_products_transformation():

    spark = SparkSessionManager.get_session()

    product_data = [
        (
            " P1001 ",
            " LOG-M185 ",
            " Wireless Mouse ",
            " Logitech ",
            " Ergonomic wireless mouse ",
            "2024-01-10 10:15",
            "2026-08-20 14:30",
            True,
            " CAT01 ",
            " SUBCAT01 ",
        ),
        (
            " P1002 ",
            " RD-K552 ",
            " Mechanical Keyboard ",
            " Redragon ",
            " RGB keyboard ",
            "2024-02-15 12:00",
            "2026-08-21 09:45",
            True,
            " CAT01 ",
            " SUBCAT02 ",
        ),
    ]

    bronze_df = spark.createDataFrame(
        product_data,
        product_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(
        config_loader=config
    )

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider,
    )

    pipeline.run("products")

    silver_df = writer.writes["silver"]

    rows = {
        row.product_id: row
        for row in silver_df.collect()
    }

    row_1 = rows["P1001"]

    assert row_1.sku == "LOG-M185"
    assert row_1.product_name == "Wireless Mouse"
    assert row_1.brand == "Logitech"
    assert row_1.category_id == "CAT01"
    assert row_1.sub_category_id == "SUBCAT01"

    assert row_1.listing_date == datetime(
        2024, 1, 10, 10, 15
    )

    assert row_1.update_date == datetime(
        2026, 8, 20, 14, 30
    )

    row_2 = rows["P1002"]

    assert row_2.product_name == "Mechanical Keyboard"
    assert row_2.brand == "Redragon"
    assert row_2.category_id == "CAT01"

# ============================== WISHLIST ==============================

def test_silver_pipeline_wishlist_mandatory_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "WL1001",
            "100",
            "P1001",
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
            True,
        ),
        (
            None,
            "101",
            "P1002",
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
            True,
        ),
        (
            "WL1003",
            None,
            "P1003",
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
            True,
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        wishlist_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("wishlist")

    assert writer.writes["silver"].count() == 1
    assert writer.writes["quarantine"].count() == 2


def test_silver_pipeline_wishlist_duplicate_validation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "WL1001",
            "100",
            "P1001",
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
            True,
        ),
        (
            "WL1002",
            "101",
            "P1002",
            datetime(2026, 7, 1),
            datetime(2026, 7, 2),
            True,
        ),
        (
            "WL1001",
            "102",
            "P1003",
            datetime(2026, 7, 3),
            datetime(2026, 7, 4),
            True,
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        wishlist_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    validator_engine = ValidatorEngine([
        DuplicateValidator(config=config),
    ])

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        validator_engine=validator_engine,
    )

    pipeline.run("wishlist")

    assert writer.writes["silver"].count() == 2
    assert writer.writes["quarantine"].count() == 1

    duplicate_row = writer.writes["quarantine"].collect()[0]

    assert duplicate_row.wishlist_id == "WL1001"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_wishlist_transformation():

    spark = SparkSessionManager.get_session()

    data = [
        (
            " WL1001 ",
            " 100 ",
            " P1001 ",
            "2026-07-01 10:15",
            "2026-07-02 14:30",
            True,
        )
    ]

    bronze_df = spark.createDataFrame(
        data,
        wishlist_columns,
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(
        config_loader=config
    )

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider,
    )

    pipeline.run("wishlist")

    row = writer.writes["silver"].collect()[0]

    assert row.wishlist_id == "WL1001"
    assert row.customer_id == "100"
    assert row.product_id == "P1001"

    assert row.created_date == datetime(
        2026, 7, 1, 10, 15
    )

    assert row.updated_date == datetime(
        2026, 7, 2, 14, 30
    )