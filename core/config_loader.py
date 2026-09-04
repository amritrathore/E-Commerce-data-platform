from pathlib import Path
import yaml
from typing import Any


class ConfigLoader:


    REQUIRED_SECTIONS =[
        "project",
        "spark",
        "logging",
        "datasets"
    ]


    def __init__(self, config_path: str | Path | None = None):
        self._config_path = Path(config_path) if config_path else self._defaults_config_path()
        self._config = self._load_config()
        self._validate()


    @property
    def config(self) -> dict[str, Any] :
        return self._config


    def _load_config(self) -> dict:

        """
        Read the YAML configuration file.

        Returns:
            dict: Parsed configuration.

        Raises:
            FileNotFoundError:
                If the configuration file doesn't exist.

            ValueError:
                If the YAML file is empty.

            RuntimeError:
                If the YAML syntax is invalid.
        """

        if not self._config_path.exists():
            raise FileNotFoundError(f"Configuration file not found : {self._config_path}")

        try:
            with self._config_path.open("r", encoding="utf-8") as file:
                config = yaml.safe_load(file)
        except yaml.YAMLError as ex:
            raise RuntimeError(f"Invalid YAML configuration: {self._config_path}") from ex
        
        if config is None:
            raise ValueError("Configuration file is empty.")
        
        return config


    def _validate(self) -> None:
        # validate mandatory configuration section.
        missing_section = [
            section
            for section in self.REQUIRED_SECTIONS
            if section not in self._config
        ]

        if missing_section:
            raise ValueError(
                f"Missing configuration sections: {missing_section}"
            )

        if not isinstance(self.get_datasets(), dict):
            raise ValueError("datasets is not dict type")


    def get_datasets(self):
        return self.config["datasets"]


    def get_metadata(self):
        return self.config.get("metadata", {})


    def get_file_defaults(self):
        return self.config.get("file", {})
    

    def get_dataset(self, dataset_name: str) -> dict[str, Any]:

        """
        Return configuration for a dataset.

        Example:
            config.get_dataset("customers")
        """

        datasets = self.get_datasets()

        if dataset_name not in datasets:
            raise ValueError(f"Dataset '{dataset_name}' not found.")
        
        return datasets[dataset_name]

    
    def get_spark_config(self) -> dict[str,Any]:
        return self._config["spark"]

    
    def get_logging_config(self) -> dict[str, Any]:
        return self._config["logging"]
    

    def get_project_config(self) -> dict[str, Any]:
        return self._config["project"]


    def get_enabled_datasets(self) -> list[str]:
        return [
            dataset_name
            for dataset_name, dataset_config in self.get_datasets().items()
            if dataset_config.get("enabled", False)
        ]


    @staticmethod
    def _defaults_config_path() -> Path:
        project_path = Path(__file__).resolve().parent.parent
        return project_path / "config" / "config.yaml"