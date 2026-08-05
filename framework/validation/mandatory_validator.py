from pyspark.sql import functions as F
from framework.validation.base_validator import BaseValidator
from framework.validation.validation_result import ValidationResult


class MandatoryValidator(BaseValidator):
    """
    Validates that mandatory columns do not contain null or empty values.
    
    A row is considered invalid if ANY mandatory column:
    - Contains a null value
    - Contains an empty string
    - Contains only whitespace (after trimming)
    
    Invalid rows are flagged with a 'validation_reason' column for audit trails.
    """
    
    def validate(self, df, dataset_name) -> ValidationResult:
        """
        Validate mandatory fields in a dataset.
        
        Args:
            df: PySpark DataFrame to validate
            dataset_name: Name of dataset (used to look up config)
            
        Returns:
            ValidationResult with:
            - valid_df: Rows with all mandatory fields populated
            - invalid_df: Rows with at least one empty/null mandatory field
            - failed_count: Number of invalid rows
            
        Raises:
            ValueError: If mandatory columns don't exist in the dataframe
        """
        
        # Fetch configuration for this dataset
        dataset_config = self.config.get_dataset(dataset_name)
        validation_config = dataset_config.get("validation", {})
        mandatory_columns = validation_config.get("mandatory_columns", [])
        
        # Early return: no mandatory columns configured
        if not mandatory_columns:
            self.logger.info(
                f"No mandatory columns configured for '{dataset_name}'."
            )
            return ValidationResult(
                valid_df=df,
                invalid_df=df.limit(0),  # Empty dataframe with same schema
                failed_count=0
            )
        
        # Validate that all mandatory columns exist in the dataframe
        missing_columns = [
            column
            for column in mandatory_columns
            if column not in df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Mandatory columns not found in dataset '{dataset_name}': "
                f"{missing_columns}"
            )
        
        # Build validation condition: row is INVALID if ANY mandatory column is null/empty
        invalid_condition = None
        for column in mandatory_columns:
            # A column is invalid if:
            # 1. It's null, OR
            # 2. When cast to string and trimmed, it's empty
            column_invalid = (
                F.col(column).isNull()
                | (F.trim(F.col(column).cast("string")) == "")
            )
            
            # Combine all column conditions with OR (any column invalid = row invalid)
            if invalid_condition is None:
                invalid_condition = column_invalid
            else:
                invalid_condition = invalid_condition | column_invalid
        
        # Split dataframe into invalid and valid rows
        invalid_df = (
            df
            .filter(invalid_condition)
            .withColumn(
                "validation_reason",
                F.lit("Mandatory field is null or empty")
            )
        )
        valid_df = df.filter(~invalid_condition)
        
        # Count invalid rows (triggers action)
        failed_count = invalid_df.count()
        
        self.logger.info(
            f"Mandatory validation completed for '{dataset_name}'. "
            f"Invalid records: {failed_count}"
        )
        
        return ValidationResult(
            valid_df=valid_df,
            invalid_df=invalid_df,
            failed_count=failed_count
        )