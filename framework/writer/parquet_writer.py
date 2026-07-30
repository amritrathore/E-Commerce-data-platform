from pyspark.sql import DataFrame

from framework.writer.base_writer import BaseWriter

class ParquetWriter(BaseWriter):

    def write(self, df: DataFrame, dataset_name: str, layer: str) -> None:

        dataset = self.config.get_dataset(dataset_name)

        if layer not in dataset:
            raise ValueError(f"Layer '{layer}' not found for dataset '{dataset_name}'.")

        output_path = dataset[layer]

        self.logger.info(
            f"Writing dataset '{dataset_name}' to {layer} layer: {output_path}"
        )

        try:
            (df.write.mode("overwrite").parquet(output_path))

            self.logger.info(f"Dataset '{dataset_name}' written successfully.")

        except Exception:
            self.logger.exception(f"Failed to write dataset '{dataset_name}' to layer '{layer}'.")
            raise