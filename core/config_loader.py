from pathlib import Path
import yaml


class ConfigLoader:


    REQUIRED_SECTIONS =[
        "project",
        "spark",
        "logging",
        "datasets"
    ]


    def __init__(self, config_path: str):
        self._config_path = Path(config_path)
        self._config = self._load_config()
        self._validate()


    @property
    def config(self) -> dict :
        return self._config


    def _load_config(self) -> dict:

        """
        Read the YAML Configuration file.

        Returns:
            dict: Parsed YAML configuration.

        Raises:
            FileNotFoundError: If Config file doesn't exist.
            ValueError: If YAML is empty.
        """

        if not self._config_path.exists():
            raise FileNotFoundError(f"Configuration file not found : {self._config_path}")
        
        with self._config_path.open("r", encoding="utf-8") as file:
            return yaml.safe_load(file)
        
        if config is None:
            raise ValueError("Configuration file is empty.")
        
        return self.config


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


    def get_dataset(self, dataset_name: str):

        """
        Return configuration for a dataset.

        Example:
            config.get_dataset("customers")
        """

        datasets = self._config["datasets"]

        if dataset_name not in datasets:
            raise ValueError(f"Dataset '{dataset_name}' not found.")
        
        return datasets[dataset_name]

    
    def get_spark_config(self) -> dict:
        return self._config["spark"]

    
    def get_logging_config(self) -> dict:
        return self._config["logging"]
    

    def get_project_config(self) -> dict:
        return self._config["project"]