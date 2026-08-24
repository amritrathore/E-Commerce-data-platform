import datetime
from pyspark.sql.types import TimestampType

from core.spark_session_manager import SparkSessionManager

from framework.reader.parquet_reader import ParquetReader
from framework.writer.parquet_writer import ParquetWriter

from framework.validation.validator_engine import ValidatorEngine
from framework.validation.mandatory_validator import MandatoryValidator
from framework.validation.duplicate_validator import DuplicateValidator

from framework.transformation.transformer_provider import TransformerProvider

from pipeline.silver.silver_pipeline import SilverPipeline


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