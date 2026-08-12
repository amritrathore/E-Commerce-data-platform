from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseTransformer(ABC):

    @abstractmethod
    def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Transform a DataFrame.

        Args:
            df: Input Spark DataFrame.
            dataset_name: Dataset being transformed.

        Returns:
            Transformed Spark DataFrame.
        """
        pass