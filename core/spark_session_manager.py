from pyspark.sql import SparkSession

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
        try:
            builder = (
                SparkSession.builder
                .appName(spark_config["app_name"])
                .master(spark_config["master"])
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
            