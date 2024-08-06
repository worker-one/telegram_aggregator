import logging.config
import os
import urllib

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from omegaconf import OmegaConf
from telethon import TelegramClient, events

# Load logging configuration
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(find_dotenv(usecwd=True))
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
TOKEN = os.getenv("BOT_TOKEN")

if not API_ID or not API_HASH or not TOKEN:
    logger.error("API_ID, API_HASH, or BOT_TOKEN is not set in the environment variables.")
    exit(1)

DOWNLOAD_DIR = "input/"

# Initialize Telegram client
client = TelegramClient('test_session', int(API_ID), API_HASH)

@client.on(events.NewMessage(pattern='/start'))
async def send_welcome(event):
    await event.respond("Загрузите excel файл со названиями каналов")

@client.on(events.NewMessage(func=lambda e: e.file))
async def handle_document(event):
    if not event.file or not event.file.name.endswith('.xlsx'):
        await event.reply("Please upload a valid Excel file.")
        return

    file_path = os.path.join(DOWNLOAD_DIR, event.file.name)
    await event.download_media(file=file_path)
    await event.reply("File downloaded successfully")

    await process_file(file_path, event.sender_id)

async def process_file(file_path, user_id):
    from src.telegram_aggregator.api.message import fetch_all_messages

    channels = pd.read_excel(file_path)["channel_name"].to_list()
    logger.info(f"Channels to fetch: {channels}")

    async with client:
        all_messages = await fetch_all_messages(channels)
        df = pd.DataFrame(all_messages, columns=['time', 'message_text', 'message_id', 'channel_name'])
        user_directory = f"output/{user_id}"
        if not os.path.exists(user_directory):
            os.makedirs(user_directory)
        df.to_excel(f"{user_directory}/data.xlsx", index=False)
        await client.send_file(user_id, f"{user_directory}/data.xlsx", caption="Here are the collected messages.")

async def start_bot():
    await client.start(bot_token=TOKEN)
    me = await client.get_me()
    logger.info(f"bot `{me.username}` has started")
    await client.run_until_disconnected()

if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_bot())
