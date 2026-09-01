from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.gold.base_gold_transformer import BaseGoldTransformer


class CustomerDimensionTransformer(BaseGoldTransformer):

    def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:

        customer_key = F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(
                    F.col("customer_id"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("first_name"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("last_name"),
                    F.lit("")
                ),
                 F.coalesce(
                    F.col("gender"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("date_of_birth").cast("string"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("city"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("state"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("postal_code"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("country"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("address"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("address_type"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("signup_datetime").cast("string"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("is_active").cast("string"),
                    F.lit("")
                ),
            ),
            256
        )




        return (
            df
            .select(
                customer_key.alias("customer_key"),

                F.col("customer_id"),

                F.col("first_name"),
                F.col("last_name"),
                F.col("gender"),
                F.col("date_of_birth"),

                F.col("city"),
                F.col("state"),
                F.col("postal_code"),
                F.col("country"),
                F.col("address"),
                F.col("address_type"),

                F.col("signup_datetime"),
                F.col("is_active"),

                F.current_timestamp().alias("effective_from"),

                F.lit(None)
                .cast("timestamp")
                .alias("effective_to"),

                F.lit(True).alias("is_current"),
            )
        )