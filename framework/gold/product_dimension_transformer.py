from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.gold.base_gold_transformer import BaseGoldTransformer

class ProductDimensionTransformer(BaseGoldTransformer):

    def transform(self, df, dataset_name)-> DataFrame:

        product_key = F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(
                    F.col("product_id"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("sku"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("product_name"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("brand"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("category_id"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("sub_category_id"),
                    F.lit("")
                ),

                F.coalesce(
                    F.col("is_active"),
                    F.lit("")
                )
            ),
            256
        )

        return (
            df
            .select(
                product_key.alias("product_key"),

                F.col("product_id"),
                F.col("sku"),
                F.col("product_name"),
                F.col("brand"),
                F.col("category_id"),
                F.col("sub_category_id"),
                F.col("listing_date"),
                F.col("is_active"),

                F.current_timestamp()
                .alias("effective_from"),

                F.lit(None)
                .cast("timestamp")
                .alias("effective_to"),

                F.lit(True)
                .alias("is_current"),
            )
        )
