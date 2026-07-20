from pyspark.sql import SparkSession

from config_loader import ConfigLoader
from logger import get_logger


class SparkSessionManager:

    _spark = None


    @classmethod
    def get_session(cls):

        if cls._spark is not None:
            return cls._spark

        config = ConfigLoader("D:/Project/E-Commerce-data-platform/E-Commerce-data-platform/config/config.yaml")

        spark_config = config.get_spark_config()

        logger = get_logger(__name__)

        builder = (
            SparkSession.builder
            .appName(spark_config["app_name"])
            .master(spark_config["master"])
        )

        for key, value in spark_config.get("config", {}).items():
            builder = builder.config(key, value)

        cls._spark = builder.getOrCreate()


        logger.info("Spark Session Created")

        return cls._spark
    

    @classmethod
    def stop_session(cls):
        if cls._spark:
            cls._spark.stop()
            cls._spark = None