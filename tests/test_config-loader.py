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


def test_empty_config(tmp_path):
    # Create an empty YAML file
    config_file = tmp_path / "empty_config.yaml"
    config_file.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="Configuration file is empty."):
        ConfigLoader(config_file)
    

def test_invalid_yaml(tmp_path):
    # Invalid YAML syntax
    config_file = tmp_path / "invalid_config.yaml"
    config_file.write_text(
        """
        project:
          app_name: Ecommerce Data Platform
          version: 1.0

        spark:
          app_name: Ecommerce Data Platform
          master: local[*]

          config:
            spark.sql.shuffle.partitions: 8
            invalid: [1, 2
        """,
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Invalid YAML configuration"):
        ConfigLoader(config_file)
        

def test_metadata_exists():
    config = ConfigLoader()
    metadata = config.get_metadata()
    assert metadata is not None
    assert len(metadata) > 0
    assert metadata["owner"] == "Amrit"
    assert metadata["pipeline"] == "Bronze"


def test_get_enabled_datasets():
    config = ConfigLoader()
    enabled_datasets = config.get_enabled_datasets()
    assert "customers" in enabled_datasets


def test_only_customers_enabled():

    config_loader = ConfigLoader()

    enabled_datasets = config_loader.get_enabled_datasets()

    assert enabled_datasets == ["customers"]
