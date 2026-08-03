from framework.reader.base_reader import BaseReader
from pyspark.sql import DataFrame
from framework.schema.schema_registry import SchemaRegistry

class CsvReader(BaseReader):

    def read(self, dataset_name) -> DataFrame:
        dataset = self.config.get_dataset(dataset_name)

        source = dataset["source"]
        delimiter = dataset["delimiter"]
        header = dataset["header"]
        schema_name = dataset["schema"]

        schema = SchemaRegistry.get(schema_name)

        self.logger.info(f"Reading dataset '{dataset_name}' from {source}")

        try:

            return (self.spark.read
                .schema(schema)
                .option("header", header)
                .option("delimiter", delimiter)
                .csv(source))
        except Exception:
            self.logger.exception(f"Failed to read dataset '{dataset_name}'.")
            raise