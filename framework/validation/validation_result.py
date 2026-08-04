from dataclasses import dataclass
from pyspark.sql import DataFrame


@dataclass
class ValidationResult:
    valid_df: DataFrame
    invalid_df: DataFrame
    failed_count: int
