from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.transformation.base_transformer import BaseTransformer


class DateNormalizerTransformer(BaseTransformer):


    def __init__(self, columns: list[str]):
        self.columns = columns


    def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        for column in self.columns:
            if column not in df.columns:
                continue
            df = df.withColumn(column, F.try_to_timestamp(F.col(column)))

        return df