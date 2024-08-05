import datetime
import logging

from omegaconf import OmegaConf

from .models import Message
from .database import get_session


# Load logging configuration with OmegaConf
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_bot/conf/logging_config.yaml"), resolve=True)

# Apply the logging configuration
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)


def add_message(time, message_text, message_id, channel_name):
	session = get_session()
	message = Message(time=time, message_text=message_text, message_id=message_id, channel_name=channel_name)
	session.add(message)
	session.commit()
	logger.info(f"Message added to the database: {message}")
	session.close()
