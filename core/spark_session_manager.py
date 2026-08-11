from pyspark.sql import SparkSession
import os
import sys

from core.config_loader import ConfigLoader
from core.logger import get_logger


class SparkSessionManager:

    _spark: SparkSession | None = None
    _logger = get_logger(__name__)

    @classmethod
    def get_session(cls) -> SparkSession:

        if cls._spark is not None:
            return cls._spark

        config = ConfigLoader()

        spark_config = config.get_spark_config()

        python_path = sys.executable

        os.environ["PYSPARK_PYTHON"] = python_path
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

        hadoop_home = os.environ.get("HADOOP_HOME")

        if hadoop_home:
            os.environ["hadoop.home.dir"] = hadoop_home

        try:
            builder = (
                SparkSession.builder
                .appName(spark_config["app_name"])
                .master(spark_config["master"])
                .config("spark.pyspark.python", python_path)
                .config("spark.pyspark.driver.python", python_path)
            )

            for key, value in spark_config.get("config", {}).items():
                builder = builder.config(key, value)

            cls._spark = builder.getOrCreate()


            cls._logger.info("Spark session created successfully.")

            return cls._spark
        except Exception:
            cls._logger.exception("Failed to initialize Spark session.")
            raise
    

    @classmethod
    def stop_session(cls) -> None:
        if cls._spark is not None:
            cls._logger.info("Stopping Spark session.")
            cls._spark.stop()
            cls._spark = None
            cls._logger.info("Spark session stopped.")
            