from pyspark.sql import DataFrame

from framework.transformation.base_transformer import BaseTransformer


class TransformerEngine:

    """
    Executes a sequence of transformations against a DataFrame.

    Transformers are executed sequentially. Each transformer
    receives the DataFrame produced by the previous transformer.
    """

    def __init__(self, transformers: list[BaseTransformer]):
        self.transformers = transformers

    def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:

        current_df = df

        for transformer in self.transformers:
            current_df = transformer.transform(current_df, dataset_name)

        return current_df

