from core.config_loader import ConfigLoader
from core.logger import get_logger
from core.spark_session_manager import SparkSessionManager
from pyspark.sql import SparkSession, DataFrame
from abc import ABC, abstractmethod


class BaseReader(ABC):

    def __init__(self,
                 config: ConfigLoader | None = None,
                 spark: SparkSession | None = None,):
        self.config = config or ConfigLoader()
        self.spark = spark or SparkSessionManager.get_session()
        self.logger = get_logger(self.__class__.__name__)


    @abstractmethod
    def read(self, dataset_name: str) -> DataFrame:
        """Read a dataset and return a Spark DataFrame."""
        pass