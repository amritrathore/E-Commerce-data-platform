from framework.transformation.base_transformer import BaseTransformer
from framework.transformation.trim_transformer import TrimTransformer
from framework.transformation.email_normalizer_transformer import EmailNormalizerTransformer
from framework.transformation.date_normalizer_transformer import DateNormalizerTransformer
from framework.transformation.standardization_transformer import StandardizationTransformer

class TransformerFactory:

    _TRANSFORMER = {
        "trim": TrimTransformer,
        "email_normalizer": EmailNormalizerTransformer,
        "date_normalizer": DateNormalizerTransformer,
        "standardization": StandardizationTransformer
    }


    @classmethod
    def create(cls, transformation_config: dict,) -> BaseTransformer:

        transformation_type = transformation_config.get("type")

        if not transformation_type:
            raise ValueError("Transformation configuration must contain 'type'")

        transformer_class = cls._TRANSFORMER.get(transformation_type)

        if transformer_class is None:
            raise ValueError(
                f"Unsupported transformation type: "
                f"{transformation_type}"
            )

        parameters = {
            key: value
            for key, value in transformation_config.items()
            if key != "type"
        }

        return transformer_class(**parameters)

