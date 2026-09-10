from datetime import datetime
from pyspark.sql.types import TimestampType

from core.config_loader import ConfigLoader
from core.spark_session_manager import SparkSessionManager

from framework.reader.parquet_reader import ParquetReader
from framework.writer.parquet_writer import ParquetWriter

from framework.validation.validator_engine import ValidatorEngine
from framework.validation.mandatory_validator import MandatoryValidator
from framework.validation.duplicate_validator import DuplicateValidator

from framework.transformation.transformer_provider import TransformerProvider

from pipeline.silver.silver_pipeline import SilverPipeline


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


# Renamed to MockConfig to avoid PytestCollectionWarning
class MockConfig:

    def __init__(self, dataset):
        self.dataset = dataset

    def get_dataset(self, dataset_name):
        return self.dataset


def test_silver_pipeline_validates_transforms_and_writes(tmp_path):

    spark = SparkSessionManager.get_session()

    bronze_path = str(tmp_path / "bronze" / "customers")
    silver_path = str(tmp_path / "silver" / "customers")
    quarantine_path = str(tmp_path / "quarantine" / "customers")

    config = MockConfig(
        {
            "bronze": bronze_path,
            "silver": silver_path,
            "quarantine": quarantine_path,
            "validation": {
                "mandatory_columns": [
                    "customer_id",
                    "first_name",
                    "email",
                ],
                "duplicate_keys": [
                    "customer_id"
                ],
            },
            "transformations": [
                {"type": "trim"},
                {"type": "email_normalizer"},
                {
                    "type": "date_normalizer",
                    "columns": ["signup_datetime", "date_of_birth", "last_modified_date"]
                },
                {
                    "type": "standardization",
                    "mappings": {
                        "Gender": {
                            "m": "Male",
                            "male": "Male",
                            "f": "Female",
                            "female": "Female",
                        }
                    }
                },
            ]
        }
    )

    data = [
        (" C001 ", " Amrit ", " AMRIT@Test.COM ", " 2026-08-01 10:30:00", " 1990-01-01 05:00:00", " 2023-01-01 07:00:00", "M "),
        (" C002 ", " John ", " JOHN@Test.COM ", " 2023-01-02 10:30:00", " 1985/01/01 10:30:00", " 2023/01/02 01:00:00", "female"),
        (" C003 ", None, "sarah@test.com", " 2023-01-03 02:00:00", " 1995-01-01 05:00:00", " 2023-01-03 07:00:00", "F"),
        (" C002 ", " Mike ", "mike@test.com", " 2023-01-04 10:30:00", " 1988-01-01 05:00:00", " 2023-01-04 07:00:00", "M"),
    ]

    columns = [
        "customer_id",
        "first_name",
        "email",
        "signup_datetime",
        "date_of_birth",
        "last_modified_date",
        "Gender",
    ]

    df = spark.createDataFrame(data, columns)

    # Create Bronze input
    df.write.mode("overwrite").parquet(bronze_path)

    validator_engine = ValidatorEngine(
        validators=[
            MandatoryValidator(config=config),
            DuplicateValidator(config=config),
        ]
    )

    transformer_provider = TransformerProvider(config)

    pipeline = SilverPipeline(
        reader=ParquetReader(
            layer="bronze",
            config=config,
            spark=spark
        ),
        writer=ParquetWriter(
            config=config,
            spark=spark
        ),
        validator_engine=validator_engine,
        transformer_provider=transformer_provider,
    )

    # Execute
    pipeline.run("customers")

    # Read outputs
    silver_df = spark.read.parquet(silver_path)
    quarantine_df = spark.read.parquet(quarantine_path)

    # ---------------------------------------------------------
    # Validation assertions
    # ---------------------------------------------------------
    # C003 fails mandatory validation.
    # One C002 is rejected as duplicate.
    assert silver_df.count() == 2
    assert quarantine_df.count() == 2

    # ---------------------------------------------------------
    # Schema assertions (DataFrame level)
    # ---------------------------------------------------------
    assert isinstance(silver_df.schema["signup_datetime"].dataType, TimestampType)
    assert isinstance(silver_df.schema["date_of_birth"].dataType, TimestampType)
    assert isinstance(silver_df.schema["last_modified_date"].dataType, TimestampType)

    # ---------------------------------------------------------
    # Row transformation assertions
    # ---------------------------------------------------------
    rows = {
        row.customer_id: row
        for row in silver_df.collect()
    }

    assert rows["C001"].first_name == "Amrit"
    assert rows["C001"].email == "amrit@test.com"
    assert rows["C001"].Gender == "Male"

    assert rows["C002"].first_name == "John"
    assert rows["C002"].email == "john@test.com"
    assert rows["C002"].Gender == "Female"


def test_silver_pipeline_products_mandatory_validation():

    spark = SparkSessionManager.get_session()

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

    product_data = [
        # Valid row
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

        # Invalid: missing product_id
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

        # Invalid: missing product_name
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

        # Invalid: missing category_id
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

    assert "silver" in writer.writes
    assert "quarantine" in writer.writes

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 1
    assert quarantine_df.count() == 3

    silver_row = silver_df.collect()[0]

    assert silver_row.product_id == "P1001"

    quarantine_rows = quarantine_df.collect()

    quarantine_product_ids = {
        row.product_id
        for row in quarantine_rows
    }

    assert None in quarantine_product_ids
    assert "P1003" in quarantine_product_ids
    assert "P1004" in quarantine_product_ids


def test_silver_pipeline_products_duplicate_validation():

    spark = SparkSessionManager.get_session()

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
        product_columns
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
        validator_engine=validator_engine
    )

    pipeline.run("products")

    silver_df = writer.writes["silver"]
    quarantine_df = writer.writes["quarantine"]

    assert silver_df.count() == 2
    assert quarantine_df.count() == 1

    duplicate_row = quarantine_df.collect()[0]

    assert duplicate_row.product_id == "P1001"
    assert "Duplicate record" in duplicate_row.validation_reason


def test_silver_pipeline_products_trim_transformation():

    spark = SparkSessionManager.get_session()

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

    product_data = [
        (
            "P1001",
            "LOG-M185",
            " Wireless Mouse ",
            " Logitech ",
            "Ergonomic wireless mouse",
            datetime(2024, 1, 10, 10, 15),
            datetime(2026, 8, 20, 14, 30),
            True,
            " CAT01 ",
            "SUBCAT01",
        ),
        (
            "P1002",
            "RD-K552",
            " Mechanical Keyboard  ",
            "Redragon ",
            "RGB keyboard",
            datetime(2024, 2, 15, 12, 0),
            datetime(2026, 8, 21, 9, 45),
            True,
            " CAT01",
            "SUBCAT02",
        ),
    ]

    bronze_df = spark.createDataFrame(
        product_data, 
        product_columns
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(config_loader=config)

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider
    )

    pipeline.run("products")

    silver_df = writer.writes["silver"]

    assert silver_df.count() == 2

    rows = {
        row.product_id: row
        for row in silver_df.collect()
    }

    row_1001 = rows["P1001"]

    assert row_1001.product_name == "Wireless Mouse"
    assert row_1001.brand == "Logitech"
    assert row_1001.category_id == "CAT01"

    row_1002 = rows["P1002"]

    assert row_1002.product_name == "Mechanical Keyboard"
    assert row_1002.brand == "Redragon"
    assert row_1002.category_id == "CAT01"


def test_silver_pipeline_products_date_normalizer_transformation():

    spark = SparkSessionManager.get_session()

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

    product_data = [
        (
            "P1001",
            "LOG-M185",
            "Wireless Mouse",
            "Logitech",
            "Ergonomic wireless mouse",
            "2024-01-10 10:15",
            "2026-08-20 14:30",
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
            "2024-02-15 12:00:00",
            "2026-08-21 09:45:00",
            True,
            "CAT01",
            "SUBCAT02",
        ),
    ]

    bronze_df = spark.createDataFrame(
        product_data, 
        product_columns
    )

    reader = FakeReader(bronze_df)
    writer = FakeWriter()

    config = ConfigLoader()

    transformer_provider = TransformerProvider(config_loader=config)

    pipeline = SilverPipeline(
        reader=reader,
        writer=writer,
        transformer_provider=transformer_provider
    )

    pipeline.run("products")

    silver_df = writer.writes["silver"]

    assert silver_df.count() == 2

    schema_by_column = {
        field.name: field.dataType.simpleString()
        for field in silver_df.schema.fields
    }

    assert schema_by_column["listing_date"] == "timestamp"
    assert schema_by_column["update_date"] == "timestamp"

    rows = {
        row.product_id: row
        for row in silver_df.collect()
    }

    row_1001 = rows["P1001"]

    assert row_1001.listing_date == datetime(
        2024, 1, 10, 10, 15
    )

    assert row_1001.update_date == datetime(
        2026, 8, 20, 14, 30
    )

    row_1002 = rows["P1002"]

    assert row_1002.listing_date == datetime(
        2024, 2, 15, 12, 0
    )

    assert row_1002.update_date == datetime(
        2026, 8, 21, 9, 45
    )