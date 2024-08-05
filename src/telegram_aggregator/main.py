import logging.config

from omegaconf import OmegaConf
from telegram_aggregator.api.bot import start_bot


# Load logging configuration with OmegaConf
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)

# Apply the logging configuration
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

# Define configuration
CONFIG = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/config.yaml"), resolve=True)

start_bot()
