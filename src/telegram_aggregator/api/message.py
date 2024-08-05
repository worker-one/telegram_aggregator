import os
import logging.config

from dotenv import load_dotenv, find_dotenv
from omegaconf import OmegaConf
import pandas as pd

from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel


# Load logging configuration with OmegaConf
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)

# Apply the logging configuration
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

# Define configuration
CONFIG = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/config.yaml"), resolve=True)

# Load the channels from the configuration file
load_dotenv(find_dotenv(usecwd=True))
API_ID = os.getenv("API_ID")

if API_ID is None:
    logger.error("API_ID is not set in the environment variables.")
    exit(1)

API_HASH = os.getenv("API_HASH")
if API_HASH is None:
    logger.error("API_HASH is not set in the environment variables.")
    exit(1)


# Create the client and connect
client = TelegramClient('test session', int(API_ID), API_HASH)

channels = CONFIG['channels']

logger.info(f"Channels to fetch: {channels}")

async def get_last_5_messages(channel_username):
    channel = await client.get_entity(channel_username)
    result = await client(GetHistoryRequest(
        peer=PeerChannel(channel.id),
        limit=10,
        offset_date=None,
        offset_id=0,
        max_id=0,
        min_id=0,
        add_offset=0,
        hash=0
    ))
    messages = result.messages
    data = []
    for message in messages:
        data.append({
            'time': message.date.replace(tzinfo=None),
            'message_text': message.message,
            'message_id': message.id,
            'channel_name': channel_username
        })
    return data

async def fetch_all_messages(channels):
    all_data = []
    for channel in channels:
        all_data.extend(await get_last_5_messages(channel))
    return all_data
