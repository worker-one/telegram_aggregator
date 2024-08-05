import logging.config

from omegaconf import OmegaConf
import pandas as pd

from telegram_aggregator.api.message import fetch_all_messages, client


# Load logging configuration with OmegaConf
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)

# Apply the logging configuration
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

# Define configuration
CONFIG = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/config.yaml"), resolve=True)

channels = CONFIG['channels']

logger.info(f"Channels to fetch: {channels}")

with client:
    all_messages = client.loop.run_until_complete(fetch_all_messages(channels))
    df = pd.DataFrame(all_messages, columns=['time', 'message_text', 'message_id', 'channel_name'])
    df.to_excel("data.xlsx", index=False)
