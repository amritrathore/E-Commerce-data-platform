from framework.gold.base_gold_transformer import BaseGoldTransformer
from framework.gold.customer_dimension_transformer import CustomerDimensionTransformer
from framework.gold.product_dimension_transformer import ProductDimensionTransformer

class GoldTransformerFactory:

    _TRANSFORMERS = {
        "customers": CustomerDimensionTransformer,
        "products": ProductDimensionTransformer,
    }

    @classmethod
    def create(
        cls,
        dataset_name: str
    )-> BaseGoldTransformer:

        transformer_class = cls._TRANSFORMERS.get(dataset_name)

        if transformer_class is None:
            raise ValueError(
                f"No Gold transformer configured for "
                f"dataset '{dataset_name}'."
            )

        return transformer_class()