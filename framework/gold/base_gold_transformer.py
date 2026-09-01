from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseGoldTransformer(ABC):

    @abstractmethod
    def transform(
        self,
        df: DataFrame,
        dataset_name: str) -> DataFrame:
        pass
