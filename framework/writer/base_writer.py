from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from core.config_loader import ConfigLoader
from core.spark_session_manager import SparkSessionManager
from core.logger import get_logger

class BaseWriter(ABC):

    def __init__(self,
                 config: ConfigLoader | None = None,
                 spark: SparkSession | None = None,):
        self.config = config or ConfigLoader()
        self.spark = spark or SparkSessionManager.get_session()
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def write(self, df: DataFrame, dataset_name: str, layer: str) -> None:
        """
        Write dataframe to storage.
        """
        pass