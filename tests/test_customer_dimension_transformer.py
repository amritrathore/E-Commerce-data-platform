from datetime import datetime

from core.spark_session_manager import SparkSessionManager
from framework.gold.customer_dimension_transformer import (
    CustomerDimensionTransformer,
)


def test_customer_dimension_transformer_builds_dim_customer():

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

    df = spark.createDataFrame(
        data,
        columns
    )

    transformer = CustomerDimensionTransformer()

    result_df = transformer.transform(
        df,
        "customers"
    )

    row = result_df.collect()[0]

    assert row.customer_key is not None

    assert row.customer_id == "100"
    assert row.first_name == "Priya"
    assert row.last_name == "Yadav"

    assert row.gender == "Female"
    assert row.date_of_birth == datetime(1988, 1, 1)

    assert row.city == "Jaipur"
    assert row.state == "Rajasthan"
    assert row.postal_code == "302001"
    assert row.country == "India"

    assert row.address == "190, Shastri Nagar"
    assert row.address_type == "Home"

    assert row.signup_datetime == datetime(
        2023, 5, 23, 23, 6
    )

    assert row.is_active is True

    assert row.effective_from is not None
    assert row.effective_to is None
    assert row.is_current is True



def test_customer_key_is_deterministic():

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

    df = spark.createDataFrame(
        data,
        columns
    )

    transformer = CustomerDimensionTransformer()

    first_result = transformer.transform(
        df,
        "customers"
    ).collect()[0]

    second_result = transformer.transform(
        df,
        "customers"
    ).collect()[0]

    assert (
        first_result.customer_key
        == second_result.customer_key
    )



def test_customer_key_changes_when_dimension_attribute_changes():

    spark = SparkSessionManager.get_session()

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

    old_data = [
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

    new_data = [
        (
            "100",
            "Priya",
            "Yadav",
            "Delhi",  # changed
            "Delhi",
            "190, Shastri Nagar",
            "Home",
            datetime(2023, 5, 23, 23, 6),
            "priya.yadav100@example.com",
            "9193349856",
            "Female",
            datetime(1988, 1, 1),
            datetime(2024, 2, 26, 23, 6),
            True,
            "110001",
            "India",
        )
    ]

    transformer = CustomerDimensionTransformer()

    old_df = spark.createDataFrame(
        old_data,
        columns
    )

    new_df = spark.createDataFrame(
        new_data,
        columns
    )

    old_row = transformer.transform(
        old_df,
        "customers"
    ).collect()[0]

    new_row = transformer.transform(
        new_df,
        "customers"
    ).collect()[0]

    assert (
        old_row.customer_key
        != new_row.customer_key
    )