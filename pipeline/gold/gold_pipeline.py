from framework.reader.base_reader import BaseReader
from framework.reader.parquet_reader import ParquetReader

from framework.writer.base_writer import BaseWriter
from framework.writer.parquet_writer import ParquetWriter

from framework.gold.gold_transformer_factory import (
    GoldTransformerFactory,
)
from framework.gold.scd2_processor import SCD2Processor
from pyspark.sql import DataFrame

from core.logger import get_logger


class GoldPipeline:

    def __init__(
            self,
            reader: BaseReader | None = None,
            gold_reader: BaseReader | None = None,
            writer: BaseWriter | None = None,
            scd2_processor: SCD2Processor | None = None):

        self.reader = reader or ParquetReader(layer="silver")

        self.gold_reader = gold_reader or ParquetReader(layer="gold")

        self.writer = writer or ParquetWriter()

        self.scd2_processor = (scd2_processor or SCD2Processor())

        self.logger = get_logger(self.__class__.__name__)


    def run(
            self,
            dataset_name: str)-> None:

        self.logger.info(
            f"Starting Gold pipeline for "
            f"dataset '{dataset_name}'."
        )

        try:

            # Read silver data
            silver_df = self.reader.read(dataset_name=dataset_name)

            # Resolve dataset-specific transformation
            transformer = (
                GoldTransformerFactory.create(dataset_name=dataset_name)
            )

            # Build Gold model
            incoming_df = transformer.transform(df=silver_df, dataset_name=dataset_name)

            # read existing gold
            existing_df = self._read_existing_gold(dataset_name=dataset_name)

            # apply SCD Type 2
            final_gold_df = self.scd2_processor.process(
                incoming_df=incoming_df,
                existing_df=existing_df,
                business_key="customer_id"
            )

            final_gold_df = final_gold_df.cache()

            final_gold_df.count()

            # Persist complete dimension
            try:
                self.writer.write(
                    df=final_gold_df,
                    dataset_name=dataset_name,
                    layer="gold"
                )
            finally:
                final_gold_df.unpersist()

            self.logger.info(
                f"Gold pipeline completed successfully "
                f"for '{dataset_name}'."
            )

        except Exception:
            self.logger.exception(
                f"Gold pipeline failed for "
                f"'{dataset_name}'."
            )
            raise


    def _read_existing_gold(self, dataset_name: str) -> DataFrame:

        try:
            return self.gold_reader.read(
                dataset_name=dataset_name
            )

        except Exception:

            self.logger.info(
                f"No existing Gold data found for "
                f"'{dataset_name}'. Initial load."
            )

            return None