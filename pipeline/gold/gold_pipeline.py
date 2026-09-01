from framework.reader.base_reader import BaseReader
from framework.reader.parquet_reader import ParquetReader

from framework.writer.base_writer import BaseWriter
from framework.writer.parquet_writer import ParquetWriter

from framework.gold.gold_transformer_factory import (
    GoldTransformerFactory,
)

from core.logger import get_logger


class GoldPipeline:

    def __init__(
            self,
            reader: BaseReader | None = None,
            writer: BaseWriter | None = None):

        self.reader = reader or ParquetReader(layer="silver")

        self.writer = writer or ParquetWriter()

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
            df = self.reader.read(dataset_name=dataset_name)

            # Resolve dataset-specific
            # Gold transformation
            transformer = (
                GoldTransformerFactory.create(dataset_name=dataset_name)
            )

            # Build Gold model
            gold_df = transformer.transform(df=df, dataset_name=dataset_name)

            # Write Gold output
            self.writer.write(gold_df, dataset_name,"gold")

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