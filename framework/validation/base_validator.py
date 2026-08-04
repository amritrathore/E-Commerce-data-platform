from abc import ABC, abstractmethod
from core.config_loader import ConfigLoader
from core.logger import get_logger
from framework.validation.validation_result import ValidationResult

class BaseValidator(ABC):

    def __init__(
            self,
            config: ConfigLoader | None = None):
        self.config = config or ConfigLoader()
        self.logger = get_logger(self.__class__.__name__)


    @abstractmethod
    def validate(self, df, dataset_name) -> ValidationResult:
        """
        Validate a DataFrame.

        Returns:
            ValidationResult
        """
        pass
