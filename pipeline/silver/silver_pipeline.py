

from framework.reader.base_reader import BaseReader
from framework.reader.parquet_reader import ParquetReader
from framework.writer.base_writer import BaseWriter
from framework.writer.parquet_writer import ParquetWriter
from framework.validation.validator_engine import ValidatorEngine
from framework.transformation.transformer_engine import TransformerEngine
from core.logger import get_logger
from pyspark.sql import DataFrame


class SilverPipeline:

    def __init__(
            self, 
            reader: BaseReader | None = None,
            writer: BaseWriter | None = None, 
            validator_engine: ValidatorEngine | None = None,
            transformer_engine: TransformerEngine | None = None):
    
        self.reader = reader or ParquetReader(layer="bronze")
        self.writer = writer or ParquetWriter()
        self.validator_engine = validator_engine or ValidatorEngine([])
        self.transformer_engine = transformer_engine or TransformerEngine([])

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


            # Transform valid records

            transformed_df = self.transformer_engine.transform(validation_result.valid_df, dataset_name)

            # Write Silver data
            
            self.writer.write(transformed_df, dataset_name, "silver")

            self.logger.info(
                f"Silver pipeline completed successfully for "
                f"'{dataset_name}'."
            )

        except Exception:
            self.logger.exception(f"Silver pipeline failed for '{dataset_name}'.")
            raise