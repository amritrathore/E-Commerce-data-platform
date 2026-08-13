from core.config_loader import ConfigLoader

from framework.transformation.base_transformer import BaseTransformer
from framework.transformation.transformer_factory import (
    TransformerFactory,
)


class TransformerProvider:

    def __init__(self,
                 config_loader: ConfigLoader | None = None,):
        
        self.config_loader = config_loader or ConfigLoader()

    def get_transformers(self, dataset_name: str,) -> list[BaseTransformer]:

        dataset_config = self.config_loader.get_dataset(
            dataset_name
        )

        transformation_configs = (
            dataset_config.get(
                "transformations",
                []
            )
        )

        return [
            TransformerFactory.create(transformation_config)
            for transformation_config in transformation_configs
        ]