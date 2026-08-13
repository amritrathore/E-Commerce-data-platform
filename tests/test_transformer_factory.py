from framework.transformation.trim_transformer import TrimTransformer
from framework.transformation.email_normalizer_transformer import (
    EmailNormalizerTransformer,
)
from framework.transformation.date_normalizer_transformer import (
    DateNormalizerTransformer,
)
from framework.transformation.standardization_transformer import (
    StandardizationTransformer,
)
from framework.transformation.transformer_factory import TransformerFactory

import pytest


def test_factory_creates_trim_transformer():

    transformer = TransformerFactory.create(
        {
            "type": "trim",
        }
    )

    assert isinstance(
        transformer,
        TrimTransformer,
    )

def test_factory_creates_email_normalizer_transformer():

    transformer = TransformerFactory.create(
        {
            "type": "email_normalizer",
            "column": "email",
        }
    )

    assert isinstance(
        transformer,
        EmailNormalizerTransformer,
    )

    assert transformer.column == "email"
    

def test_factory_creates_date_normalizer_transformer():

    transformer = TransformerFactory.create(
        {
            "type": "date_normalizer",
            "columns": [
                "signup_datetime",
                "date_of_birth",
            ],
        }
    )

    assert isinstance(
        transformer,
        DateNormalizerTransformer,
    )

    assert transformer.columns == [
        "signup_datetime",
        "date_of_birth",
    ]

def test_factory_creates_standardization_transformer():

    mappings = {
        "gender": {
            "m": "Male",
            "male": "Male",
            "f": "Female",
            "female": "Female",
        }
    }

    transformer = TransformerFactory.create(
        {
            "type": "standardization",
            "mappings": mappings,
        }
    )

    assert isinstance(
        transformer,
        StandardizationTransformer,
    )

    assert transformer.mappings == mappings


def test_factory_raises_error_when_type_missing():

    try:
        TransformerFactory.create({})
        assert False

    except ValueError as exc:
        assert str(exc) == (
            "Transformation configuration must contain 'type'"
        )


def test_factory_raises_error_when_type_missing():

    with pytest.raises(
        ValueError,
        match="Transformation configuration must contain 'type'",
    ):
        TransformerFactory.create({})


def test_factory_raises_error_for_unsupported_type():

    with pytest.raises(
        ValueError,
        match="Unsupported transformation type",
    ):
        TransformerFactory.create(
            {
                "type": "unknown_transformer",
            }
        )