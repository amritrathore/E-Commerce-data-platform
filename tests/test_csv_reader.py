from framework.reader.csv_reader import CsvReader


def test_read_customers_returns_dataframe():

    reader = CsvReader()

    df = reader.read("customers")

    assert df is not None


def test_customer_schema_applied():

    reader = CsvReader()

    df = reader.read("customers")

    assert "customer_id" in df.columns
    assert len(df.schema.fields) == 16