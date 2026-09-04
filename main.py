from core.config_loader import ConfigLoader
from core.logger import get_logger
from core.spark_session_manager import SparkSessionManager

from framework.validation.mandatory_validator import MandatoryValidator
from framework.validation.duplicate_validator import DuplicateValidator
from framework.validation.validator_engine import ValidatorEngine

from framework.transformation.transformer_provider import TransformerProvider

from pipeline.bronze.bronze_pipeline import BronzePipeline
from pipeline.silver.silver_pipeline import SilverPipeline
from pipeline.gold.gold_pipeline import GoldPipeline


logger = get_logger(__name__)


def run_pipeline(dataset_name: str, config: ConfigLoader):

    logger.info(
        f"Starting end-to-end pipeline "
        f"for dataset '{dataset_name}'."
    )

    # Customers validation chain.
    validator_engine = ValidatorEngine([
        MandatoryValidator(config=config),
        DuplicateValidator(config=config)
    ])

    # Transformers are resolved from config.yaml.
    transformer_provider = TransformerProvider(config_loader=config)

    # Bronze:
    # customers.csv -> Bronze paraquet
    bronze_pipeline = BronzePipeline()

    # Silver
    # Bronze -> validation -> quarantine -> transform -> Silver
    silver_pipeline = SilverPipeline(validator_engine=validator_engine, transformer_provider=transformer_provider)

    # Gold
    gold_pipeline = GoldPipeline()

    logger.info(f"Starting end-to-end pipeline for dataset '{dataset_name}'.")

    bronze_pipeline.run(dataset_name=dataset_name)
    silver_pipeline.run(dataset_name=dataset_name)
    gold_pipeline.run(dataset_name=dataset_name)

    logger.info(f"Pipeline completed successfully for dataset '{dataset_name}'.")


def main() -> None:

    config = ConfigLoader()

    try:
        # Initialize Spark before starting the pipeline:
        SparkSessionManager.get_session()

        enabled_datasets = (
            config.get_enabled_datasets()
        )

        logger.info(
            f"Enabled datasets: "
            f"{enabled_datasets}"
        )

        for dataset_name in enabled_datasets:
            run_pipeline(dataset_name, config)
            
    finally:
        SparkSessionManager.stop_session()


if __name__ == "__main__":
    main()
        
