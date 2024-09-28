import asyncio
import os
from venv import logger

from dotenv import find_dotenv, load_dotenv
from omegaconf import OmegaConf
from telegram_aggregator.db.crud import add_message, get_channel_names
from telegram_aggregator.db.database import create_tables
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel

config = OmegaConf.load("./src/telegram_aggregator/conf/config.yaml")

# Load environment variables
load_dotenv(find_dotenv(usecwd=True))
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

async def get_last_5_messages(user_client, channel_username):
    channel = await user_client.get_entity(channel_username)
    result = await user_client(GetHistoryRequest(
        peer=PeerChannel(channel.id),
        limit=config.last_n_messages,
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
            'datetime': message.date.replace(tzinfo=None),
            'content': message.message,
            'id': message.id,
            'channel_name': channel_username
        })
    return data

async def fetch_all_messages(channels: str):
    all_data = []
    user_client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)
    await user_client.start()

    for channel in channels:
        all_data.extend(await get_last_5_messages(user_client, channel))
    return all_data

async def main_loop():
    create_tables()
    while True:
        channels = get_channel_names()  # Get channels from the database
        logger.info(f"Fetching messages from channels: {channels}")
        records = await fetch_all_messages(channels)
        
        for record in records:
            add_message(
                id=record['id'],
                datetime=record['datetime'],
                content=record['content'],
                channel_name=record['channel_name']
            )
        
        await asyncio.sleep(config.interval_seconds)

if __name__ == "__main__":
    asyncio.run(main_loop())