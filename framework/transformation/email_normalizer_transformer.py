from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.transformation.base_transformer import BaseTransformer


class EmailNormalizerTransformer(BaseTransformer):

    """
        Normalizes an email column by:

        1. Removing leading/trailing whitespace.
        2. Converting the email to lowercase.

        This transformer does not validate email format.
    """

    def __init__(self, column: str = "email"):
        self.column = column


    def transform(self, df, dataset_name) -> DataFrame:

        if self.column not in df.columns:
            raise ValueError(
                f"Email column '{self.column}' not found "
                f"in dataset '{dataset_name}'."
            )

        return df.withColumn(self.column, 
                             F.lower(
                                 F.trim(
                                     F.col(self.column))))
