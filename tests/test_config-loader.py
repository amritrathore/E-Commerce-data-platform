import pytest
from core.config_loader import ConfigLoader

def test_config_file_loads():
    config = ConfigLoader()

    assert config.config is not None


def test_project_section_exists():
    config = ConfigLoader()

    project = config.get_project_config()

    assert "app_name" in project
    assert "version" in project


def test_spark_section_exists():
    config = ConfigLoader()

    spark = config.get_spark_config()

    assert "app_name" in spark
    assert "master" in spark


def test_customer_dataset_exists():
    config = ConfigLoader()

    dataset = config.get_dataset('customers')

    assert dataset["source"] is not None
    assert dataset["bronze"] is not None
    assert dataset["silver"] is not None


def test_invalid_dataset():
    config = ConfigLoader()

    with pytest.raises(ValueError):
        config.get_dataset("invalid_table")
