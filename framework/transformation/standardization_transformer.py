from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.transformation.base_transformer import BaseTransformer

class StandardizationTransformer(BaseTransformer):

    def __init__(self, mappings: dict[str, dict[str, str]]):
        self.mappings = mappings

    def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        for column, mapping in self.mappings.items():
            if column not in df.columns:
                continue

            normalized_mapping = {
                key.strip().lower(): value
                for key, value in mapping.items()
            }

            mapping_expression = F.create_map(
                *[
                    F.lit(item)
                    for pair in normalized_mapping.items()
                    for item in pair
                ]
            )

            normalized_column = F.lower(
                F.trim(F.col(column))
            )

            df = df.withColumn(
                column,
                F.coalesce(
                    mapping_expression.getItem(normalized_column),
                    F.col(column)
                )
            )

        return df