import datetime
import logging
from sqlalchemy import select, and_

from omegaconf import OmegaConf
from .models import Message, Channel
from .database import get_session


# Load logging configuration with OmegaConf
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)

# Apply the logging configuration
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)


def add_message(message_id: int, time: datetime.datetime, message_text: str, channel_name: str):
    session = get_session()
    try:
        # Check if a record with the same primary key (message_id) already exists
        existing_message = session.query(Message).filter_by(message_id=message_id).first()

        if existing_message is None:
            # If no existing record is found, add the new message
            message = Message(message_id=message_id, time=time, message_text=message_text, channel_name=channel_name)
            session.add(message)
            session.commit()
            logger.info(f"Message added to the database: {message}")
        else:
            logger.info(f"Message with id {message_id} already exists, not adding.")
    except Exception as e:
        session.rollback()
        logger.error(f"An error occurred: {e}")
    finally:
        session.close()

def add_channel(channel_name: str, comment: str = None):
    session = get_session()
    channel = Channel(channel_name=channel_name, comment=comment)
    session.add(channel)
    session.commit()
    logger.info(f"Channel added to the database: {channel}")
    session.close()

def get_channel_names() -> list[str]:
    session = get_session()
    try:
        # Query to get distinct channel names
        query = select(Channel.channel_name).distinct()
        result = session.execute(query)
        
        # Fetch all distinct channel names
        channel_names = [row[0] for row in result]
        return channel_names
    
    finally:
        session.close()

def get_messages_in_timerange(start_time: datetime.datetime, end_time: datetime.datetime) -> list[Message]:
    session = get_session()
    try:
        # Query to filter messages based on the time range
        query = select(Message).where(and_(Message.time >= start_time, Message.time <= end_time))
        result = session.execute(query)
        
        # Fetch all messages in the given time range
        messages = result.scalars().all()
        return messages
    
    finally:
        session.close()
