from datetime import datetime

from core.spark_session_manager import SparkSessionManager
from pipeline.gold.gold_pipeline import GoldPipeline


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


def test_gold_pipeline_transforms_and_writes_customer_dimension():

    spark = SparkSessionManager.get_session()

    data = [
        (
            "100",
            "Priya",
            "Yadav",
            "Jaipur",
            "Rajasthan",
            "190, Shastri Nagar",
            "Home",
            datetime(2023, 5, 23, 23, 6),
            "priya.yadav100@example.com",
            "9193349856",
            "Female",
            datetime(1988, 1, 1),
            datetime(2024, 2, 26, 23, 6),
            True,
            "302001",
            "India",
        )
    ]

    columns = [
        "customer_id",
        "first_name",
        "last_name",
        "city",
        "state",
        "address",
        "address_type",
        "signup_datetime",
        "email",
        "phone_number",
        "gender",
        "date_of_birth",
        "last_modified_date",
        "is_active",
        "postal_code",
        "country",
    ]

    silver_df = spark.createDataFrame(
        data,
        columns
    )

    reader = FakeReader(silver_df)
    writer = FakeWriter()

    pipeline = GoldPipeline(
        reader=reader,
        writer=writer,
    )

    pipeline.run("customers")

    assert writer.dataset_name == "customers"
    assert writer.layer == "gold"
    assert writer.written_df is not None

    row = writer.written_df.collect()[0]

    assert row.customer_id == "100"
    assert row.customer_key is not None
    assert row.is_current is True
    assert row.effective_from is not None
    assert row.effective_to is None