from framework.validation.base_validator import BaseValidator
from framework.validation.validation_result import ValidationResult


class ValidatorEngine:

    """
    Executes a sequence of validators against a DataFrame.

    Validators are executed  sequentially. Each validator receives
    the valid records produced by the previouse validator.
    """

    def __init__(self, validators: list[BaseValidator]):
        self.validators =  validators


    def validate(self, df, dataset_name) -> ValidationResult:

        current_valid_df = df
        invalid_dfs = []
        failed_count = 0


        for validator in self.validators:

            result = validator.validate(current_valid_df, dataset_name)

            current_valid_df = result.valid_df

            if result.failed_count > 0:
                invalid_dfs.append(result.invalid_df)
                failed_count += result.failed_count


        # No validation failures
        if not invalid_dfs:

            return ValidationResult(
                valid_df=current_valid_df,
                invalid_df=df.limit(0),
                failed_count=0
            )

        # Combine invalid records from all validators
        invalid_df = invalid_dfs[0]


        for current_invalid_df in invalid_dfs[1:]:
            invalid_df = invalid_df.unionByName(current_invalid_df)


        return ValidationResult(
            valid_df= current_valid_df,
            invalid_df=invalid_df,
            failed_count=failed_count
        )