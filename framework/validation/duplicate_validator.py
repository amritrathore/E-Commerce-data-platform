from pyspark.sql import functions as F
from framework.validation.base_validator import BaseValidator
from framework.validation.validation_result import ValidationResult
from pyspark.sql.window import Window

class DuplicateValidator(BaseValidator):

    def validate(self, df, dataset_name) -> ValidationResult:
        """
            Validate duplicate records in a dataset.

            Args:
                df: PySpark DataFrame to validate.
                dataset_name: Name of dataset used to look up configuration.

            Returns:
                ValidationResult containing valid and duplicate records.

            Raises:
                ValueError: If configured duplicate key columns are missing.
        """
        dataset_config = self.config.get_dataset(dataset_name)

        validation_config = dataset_config.get("validation", {})

        duplicate_keys = validation_config.get("duplicate_keys", [])


# No duplicate keys configured
        if not duplicate_keys:
            self.logger.info(
                f"No duplicate keys configured for '{dataset_name}'."
            )

            return ValidationResult(
                valid_df=df,
                invalid_df=df.limit(0),
                failed_count=0
            )


# Validate that all configured keys exist
        missing_columns = [
            column
            for column in duplicate_keys
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Duplicate key columns not found in dataset "
                f"'{dataset_name}': {missing_columns}"
            )


        # Identify duplicate rows.
        #
        # row_number() gives the first occurrence of each key combination
        # row_number = 1  -> valid
        # row_number > 1  -> duplicate
        #
        # We use monotonically_increasing_id() only to provide a deterministic
        # ordering within the current DataFrame execution.

        window = (
            Window
            .partitionBy(*duplicate_keys)
            .orderBy(F.monotonically_increasing_id())
        )

        ranked_df = df.withColumn(
            "_duplicate_row_number",
            F.row_number().over(window)
        )


        invalid_df = (
            ranked_df
            .filter(F.col("_duplicate_row_number") > 1)
            .drop("_duplicate_row_number")
            .withColumn(
                "validation_reason",
                F.lit(
                    "Duplicate record based on key(s): "
                    + ", ".join(duplicate_keys)
                )
            )
        )

        valid_df = (
            ranked_df
            .filter(F.col("_duplicate_row_number") == 1)
            .drop("_duplicate_row_number")
        )

        failed_count = invalid_df.count()

        self.logger.info(
            f"Duplicate validation completed for '{dataset_name}'. "
            f"Duplicate records: {failed_count}"
        )

        return ValidationResult(
            valid_df=valid_df,
            invalid_df=invalid_df,
            failed_count=failed_count
        )