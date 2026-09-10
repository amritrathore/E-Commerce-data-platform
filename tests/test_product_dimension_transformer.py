from datetime import datetime
from core.spark_session_manager import SparkSessionManager

from framework.gold.product_dimension_transformer import ProductDimensionTransformer

def test_product_dimension_transformer_builds_gold():

    spark = SparkSessionManager.get_session()

    product_data = [(
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
    )]

    product_columns = [
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
    ]

    df = spark.createDataFrame(
        product_data,
        product_columns,
    )

    transformer = ProductDimensionTransformer()

    result_df = transformer.transform(
        df=df,
        dataset_name="products",
    )

    row = result_df.collect()[0]

    assert row.product_key is not None

    assert row.product_id == "P1001"
    assert row.sku == "LOG-M185"
    assert row.product_name == "Wireless Mouse"
    assert row.brand == "Logitech"

    assert row.category_id == "CAT01"
    assert row.sub_category_id == "SUBCAT01"

    assert row.listing_date == datetime(2024, 1, 10, 10, 15)

    assert row.is_active is True

    assert row.effective_from is not None
    assert row.effective_to is None
    assert row.is_current is True

    assert result_df.columns == [
        "product_key",
        "product_id",
        "sku",
        "product_name",
        "brand",
        "category_id",
        "sub_category_id",
        "listing_date",
        "is_active",
        "effective_from",
        "effective_to",
        "is_current",
    ]


def test_product_dimension_transformer_same_data_same_key():

    spark = SparkSessionManager.get_session()

    data = [
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
    ]

    columns = [
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
    ]

    df1 = spark.createDataFrame(data, columns)
    df2 = spark.createDataFrame(data, columns)

    transformer = ProductDimensionTransformer()

    key1 = transformer.transform(
        df1,
        "products",
    ).collect()[0].product_key

    key2 = transformer.transform(
        df2,
        "products",
    ).collect()[0].product_key

    assert key1 == key2



def test_product_dimension_transformer_changed_data_new_key():

    spark = SparkSessionManager.get_session()

    columns = [
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
    ]

    old_data = [
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
    ]

    new_data = [
        (
            "P1001",
            "LOG-M185",
            "Wireless Mouse",
            "Logitech Pro",
            "Ergonomic wireless mouse",
            datetime(2024, 1, 10, 10, 15),
            datetime(2026, 8, 21, 14, 30),
            True,
            "CAT01",
            "SUBCAT01",
        )
    ]

    old_df = spark.createDataFrame(
        old_data,
        columns,
    )

    new_df = spark.createDataFrame(
        new_data,
        columns,
    )

    transformer = ProductDimensionTransformer()

    old_key = transformer.transform(
        old_df,
        "products",
    ).collect()[0].product_key

    new_key = transformer.transform(
        new_df,
        "products",
    ).collect()[0].product_key

    assert old_key != new_key