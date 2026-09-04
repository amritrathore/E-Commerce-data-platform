from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class SCD2Processor:

    def process(
            cls,
            incoming_df: DataFrame,
            existing_df: DataFrame | None,
            business_key: str) -> DataFrame:

        # Initial load
        if existing_df is None:
            return incoming_df

        current_existing_df = (
            existing_df.filter(F.col("is_current") == True)
        )

        historical_df = (
            existing_df.filter(F.col("is_current") == False)
        )

        incoming_alias = incoming_df.alias("incoming")
        existing_alias = current_existing_df.alias("existing")

        comparison_df = (
            incoming_alias
            .join(
                existing_alias,
                F.col(f"incoming.{business_key}") == F.col(f"existing.{business_key}"),
                "left",
            )
        )

        # Completely new business keys
        new_records_df = (
            comparison_df
            .filter(
                F.col(f"existing.{business_key}").isNull()
            )
            .select("incoming.*")
        )

        # Existing customer with changed version hash/key
        changed_records_df = (
            comparison_df
            .filter(
                (F.col(f"existing.{business_key}").isNotNull())
                &
                (F.col("incoming.customer_key") != F.col("existing.customer_key"))
            )
        )

        changed_business_key_df = (
            changed_records_df
            .select(
                F.col(
                    f"incoming.{business_key}"
                ).alias(business_key)
            )
            .distinct()
        )

         # Expire old current versions

        expired_records_df = (
            current_existing_df.alias("existing")
            .join(
                changed_business_key_df.alias("changed"),
                business_key,
                "inner",
            )
            .withColumn(
                "effective_to",
                F.current_timestamp(),
            )
            .withColumn(
                "is_current",
                F.lit(False)
            )
        )

        # New versions for changed customers

        changed_new_versions_df = (
            changed_records_df
            .select("incoming.*")
        )

        # Existing current rows that did not change

        unchanged_current_df = (
            current_existing_df.alias("existing")
            .join(
                changed_business_key_df.alias("changed"),
                business_key,
                "left_anti",
            )
        )

        result_df = historical_df

        result_df = result_df.unionByName(
            unchanged_current_df
        )

        result_df = result_df.unionByName(
            expired_records_df
        )

        result_df = result_df.unionByName(
            new_records_df
        )

        result_df = result_df.unionByName(
            changed_new_versions_df
        )

        return result_df

