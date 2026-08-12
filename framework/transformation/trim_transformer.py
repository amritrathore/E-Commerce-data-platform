from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.transformation.base_transformer import BaseTransformer


class TrimTransformer(BaseTransformer):

    def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:

        string_columns = (
            field.name
            for field in df.schema.fields
            if field.dataType.simpleString() == "string"
        )

        for column in string_columns:
            df = df.withColumn(
                column,
                F.trim(F.col(column))
            )

        return df