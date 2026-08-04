from framework.validation.validation_result import ValidationResult
from framework.validation.base_validator import BaseValidator
import pytest


class DummyValidator(BaseValidator):

    def validate(self, df, dataset_name):
        return None
    
    

def test_validation_result():

    valid_df = "valid"
    invalid_df = "invalid"

    result = ValidationResult(
        valid_df=valid_df,
        invalid_df=invalid_df,
        failed_count=5
    )

    assert result.valid_df == valid_df
    assert result.invalid_df == invalid_df
    assert result.failed_count == 5


def test_base_validator_can_be_extended():

    validator = DummyValidator()

    assert validator is not None
    assert validator.config is not None
    assert validator.logger is not None


def test_base_validator_cannot_be_instantiated():

    with pytest.raises(TypeError):
        BaseValidator()
