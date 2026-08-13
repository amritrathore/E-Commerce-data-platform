

from framework.reader.base_reader import BaseReader
from framework.reader.parquet_reader import ParquetReader
from framework.writer.base_writer import BaseWriter
from framework.writer.parquet_writer import ParquetWriter
from framework.validation.validator_engine import ValidatorEngine
from framework.transformation.transformer_engine import TransformerEngine
from framework.transformation.transformer_factory import TransformerFactory
from framework.transformation.transformer_provider import TransformerProvider

from core.config_loader import ConfigLoader
from core.logger import get_logger
from pyspark.sql import DataFrame


class SilverPipeline:

    def __init__(
            self, 
            reader: BaseReader | None = None,
            writer: BaseWriter | None = None, 
            validator_engine: ValidatorEngine | None = None,
            transformer_provider: TransformerProvider  | None = None):
    
        self.reader = reader or ParquetReader(layer="bronze")
        self.writer = writer or ParquetWriter()
        self.validator_engine = validator_engine or ValidatorEngine([])
        self.transformer_provider = transformer_provider or TransformerProvider()

        self.logger = get_logger(self.__class__.__name__)


    def run(self, dataset_name: str) -> None:

        self.logger.info(f"Starting Silver pipeline for dataset '{dataset_name}'.")

        try:
            # Read Bronze data
            df = self.reader.read(dataset_name)

            self.logger.info(
                f"Bronze data read successfully for '{dataset_name}'."
            )

            # Validate data
            validation_result = self.validator_engine.validate(df, dataset_name)


            self.logger.info(
                f"Validation completed for '{dataset_name}'. "
                f"Invalid records: "
                f"{validation_result.failed_count}"
            )

            # Write invalid records to quarantine

            if validation_result.invalid_df.count() > 0:

                self.writer.write(validation_result.invalid_df, dataset_name, "quarantine")

                self.logger.info(
                    f"Invalid records written to quarantine for "
                    f"'{dataset_name}'."
                )


            transformers = self.transformer_provider.get_transformers(dataset_name)

            transformer_engine = TransformerEngine(transformers)

            # Transform valid records

            transformed_df = transformer_engine.transform(validation_result.valid_df, dataset_name)

            # Write Silver data
            
            self.writer.write(transformed_df, dataset_name, "silver")

            self.logger.info(
                f"Silver pipeline completed successfully for "
                f"'{dataset_name}'."
            )

        except Exception:
            self.logger.exception(f"Silver pipeline failed for '{dataset_name}'.")
            raise


    def _create_transformer_engine(self, dataset_name: str) -> TransformerEngine:

        config = ConfigLoader()

        dataset_config = config.get_dataset(dataset_name)

        transformation_configs = (
            dataset_config.get("transformations",[])
            )

        transformers = [
            TransformerFactory.create(transformation_config)
            for transformation_config in transformation_configs
        ]

        return TransformerEngine(
            transformers
        )