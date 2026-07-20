from core.spark_session_manager import SparkSessionManager


def test_spark_created():
    spark = SparkSessionManager.get_session()

    assert spark is not None



def test_spark_app_name():
    spark = SparkSessionManager.get_session()

    assert spark.sparkContext.appName == "Ecommerce Data Warehouse"



def test_same_session_returned():
    spark1 = SparkSessionManager.get_session()

    spark2 = SparkSessionManager.get_session()

    assert spark1 is spark2