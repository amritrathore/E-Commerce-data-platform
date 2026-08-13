from core.spark_session_manager import SparkSessionManager

from framework.reader.parquet_reader import ParquetReader
from framework.writer.parquet_writer import ParquetWriter

from framework.validation.validator_engine import ValidatorEngine
from framework.validation.mandatory_validator import MandatoryValidator
from framework.validation.duplicate_validator import DuplicateValidator

from framework.transformation.transformer_engine import TransformerEngine
from framework.transformation.trim_transformer import TrimTransformer
from framework.transformation.email_normalizer_transformer import EmailNormalizerTransformer

from pipeline.silver.silver_pipeline import SilverPipeline


class TestConfig:

    def __init__(self, dataset):
        self.dataset = dataset

    def get_dataset(self, dataset_name):
        return self.dataset


def test_silver_pipeline_validates_transforms_and_writes(
    tmp_path
):

    spark = SparkSessionManager.get_session()

    bronze_path = str(
        tmp_path / "bronze" / "customers"
    )

    silver_path = str(
        tmp_path / "silver" / "customers"
    )

    quarantine_path = str(
        tmp_path / "quarantine" / "customers"
    )

    config = TestConfig(
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
        }
    )

    data = [
        (" C001 ", " Amrit ", " AMRIT@Test.COM "),
        (" C002 ", " John ", " JOHN@Test.COM "),
        (" C003 ", None, "sarah@test.com"),
        (" C002 ", " Mike ", "mike@test.com"),
    ]

    columns = [
        "customer_id",
        "first_name",
        "email",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    # Create Bronze input
    df.write.mode("overwrite").parquet(
        bronze_path
    )

    validator_engine = ValidatorEngine(
        validators=[
            MandatoryValidator(
                config=config
            ),
            DuplicateValidator(
                config=config
            ),
        ]
    )

    transformer_engine = TransformerEngine(
        transformers=[
            TrimTransformer(),
            EmailNormalizerTransformer()
        ]
    )

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
        transformer_engine=transformer_engine,
    )

    # Execute
    pipeline.run("customers")

    # Read outputs
    silver_df = spark.read.parquet(
        silver_path
    )

    quarantine_df = spark.read.parquet(
        quarantine_path
    )

     # ---------------------------------------------------------
    # Validation assertions
    # ---------------------------------------------------------

    # C003 fails mandatory validation.
    # One C002 is rejected as duplicate.

    assert silver_df.count() == 2
    assert quarantine_df.count() == 2

    # ---------------------------------------------------------
    # Transformation assertions
    # ---------------------------------------------------------

    rows = {
        row.customer_id: row
        for row in silver_df.collect()
    }

    assert rows["C001"].first_name == "Amrit"
    assert rows["C001"].email == "amrit@test.com"

    assert rows["C002"].first_name == "John"
    assert rows["C002"].email == "john@test.com"