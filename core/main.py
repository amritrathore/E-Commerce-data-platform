from config_loader import ConfigLoader
from logger import get_logger

config = ConfigLoader("D:/Project/E-Commerce-data-platform/E-Commerce-data-platform/config/config.yaml")

customers = config.get_dataset("customers")
logger = get_logger(__name__)



logger.info(customers["source"])
logger.warning("duplicate customerID found")
logger.error("Failed to write parquet")