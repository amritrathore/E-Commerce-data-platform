import pytest
from core.config_loader import ConfigLoader

CONFIG_PATH = "D:/Project/E-Commerce-data-platform/E-Commerce-data-platform/config/config.yaml"

def test_config_file_loads():
    config = ConfigLoader(CONFIG_PATH)

    assert config.config is not None


def test_project_section_exists():
    config = ConfigLoader(CONFIG_PATH)

    project = config.get_project_config()

    assert "name" in project
    assert "version" in project


def test_spark_section_exists():
    config = ConfigLoader(CONFIG_PATH)

    spark = config.get_spark_config()

    assert "app_name" in spark
    assert "master" in spark


def test_customer_dataset_exists():
    config = ConfigLoader(CONFIG_PATH)

    dataset = config.get_dataset('customers')

    assert dataset["source"] is not None
    assert dataset["bronze"] is not None
    assert dataset["silver"] is not None


def test_invalid_dataset():
    config = ConfigLoader(CONFIG_PATH)

    with pytest.raises(ValueError):
        config.get_dataset("invalid_table")
