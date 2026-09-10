from datetime import datetime

from core.spark_session_manager import SparkSessionManager
from pipeline.bronze.bronze_pipeline import BronzePipeline

class FakeReader:

    def __init__(self, df):
        self.df = df

    def read(self, dataset_name):
        return self.df

class FakeWriter:

    def __init__(self):
        self.written_df = None
        self.dataset_name = None
        self.layer = None

    def write(self, df, dataset_name, layer):
        self.written_df = df
        self.dataset_name = dataset_name
        self.layer = layer


def test_bronze_pipeline_creation():
    pipeline = BronzePipeline()

    assert pipeline.reader is not None
    assert pipeline.writer is not None


def test_bronze_pipeline_products():

    spark = SparkSessionManager.get_session()

    product_df = spark.createDataFrame(
        [
            (
                "P1001",
                "LOG-M185",
                "Wireless Mouse",
                "Logitech",
                "Ergonomic wireless mouse",
                datetime(2024, 1, 10, 10, 15),
                datetime(2026, 8, 20, 14, 30),
                True,
                "CAT01",
                "SUBCAT01",
            )
        ],
        [
            "product_id",
            "sku",
            "product_name",
            "brand",
            "description",
            "listing_date",
            "update_date",
            "is_active",
            "category_id",
            "sub_category_id",
        ],
    )

    reader = FakeReader(product_df)
    writer = FakeWriter()

    pipeline = BronzePipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("products")

    assert writer.dataset_name == "products"
    assert writer.layer == "bronze"
    assert writer.written_df is not None

    assert "ingestion_timestamp" in writer.written_df.columns
    assert "source_file" in writer.written_df.columns
    assert "batch_id" in writer.written_df.columns