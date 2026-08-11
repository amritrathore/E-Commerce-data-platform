from pyspark.sql import DataFrame

from framework.reader.base_reader import BaseReader


class ParquetReader(BaseReader):

    def __init__(self, layer: str = "bronze", config = None, spark = None):
        super().__init__(config, spark)
        self.layer = layer


    def read(self, dataset_name) -> DataFrame:

        dataset = self.config.get_dataset(dataset_name)

        if self.layer not in dataset:
            raise ValueError(
                f"Layer '{self.layer}' not found "
                f"for dataset '{dataset_name}'."
            )

        source = dataset[self.layer]

        self.logger.info(
            f"Reading dataset '{dataset_name}' "
            f"from {self.layer} layer: {source}"
        )


        try:
            return self.spark.read.parquet(source)

        except Exception:
            self.logger.exception(
                f"Failed to read dataset '{dataset_name}' "
                f"from layer '{self.layer}'."
            )
            raise
