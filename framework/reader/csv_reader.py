from framework.reader.base_reader import BaseReader
from pyspark.sql import DataFrame

class CsvReader(BaseReader):

    def read(self, dataset_name) -> DataFrame:
        dataset = self.config.get_dataset(dataset_name)

        source = dataset["source"]
        delimiter = dataset["delimiter"]
        header = dataset["header"]

        self.logger.info(f"Reading dataset '{dataset_name}' from {source}")

        try:

            return (self.spark.read
                .option("header", header)
                .option("delimiter", delimiter)
                .option("inferSchema", True)
                .csv(source))
        except Exception:
            self.logger.exception(f"Failed to read dataset '{dataset_name}'.")
            raise