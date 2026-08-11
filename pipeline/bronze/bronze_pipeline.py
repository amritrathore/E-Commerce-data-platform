from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit
)

from framework.reader.base_reader import BaseReader
from framework.reader.csv_reader import CsvReader
from framework.writer.base_writer import BaseWriter
from framework.writer.parquet_writer import ParquetWriter
from core.logger import get_logger


class BronzePipeline:

    def __init__(
            self,
            reader: BaseReader | None = None,
            writer: BaseWriter | None = None):
        self.reader = reader or CsvReader()
        self.writer = writer or ParquetWriter()
        self.logger = get_logger(self.__class__.__name__)


    def run(self, dataset_name: str) -> None:

        batch_id = str(uuid4())

        self.logger.info(
            f"Starting Bronze pipeline for dataset '{dataset_name}'."
            f"Batch ID: {batch_id}")

        try:
            # Read source data
            df = self.reader.read(dataset_name)

            # Add bronze metadata
            df = self._add_metadata(df, batch_id)

            # Write bronze output
            self.writer.write(
                df,
                dataset_name,
                "bronze"
            )

            self.logger.info(f"Bronze pipeline completed successfully for '{dataset_name}'.")

        except Exception:
            self.logger.exception(f"Bronze pipeline failed for '{dataset_name}'.")
            raise


    def _add_metadata(self, df: DataFrame, batch_id: str) -> DataFrame:
        return (
            df
            .withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "source_file",
                input_file_name()
            )
            .withColumn(
                "batch_id",
                lit(batch_id)
            )
        )