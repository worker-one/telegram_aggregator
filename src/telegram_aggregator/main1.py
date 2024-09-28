import logging.config
import os
import urllib

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from omegaconf import OmegaConf
from telethon import TelegramClient, events, sync
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel

# Load logging configuration
logging_config = OmegaConf.to_container(
    OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"),
    resolve=True
)
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(find_dotenv(usecwd=True))
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

if not API_ID or not API_HASH or not BOT_TOKEN or not PHONE_NUMBER:
    logger.error("API_ID, API_HASH, BOT_TOKEN, or PHONE_NUMBER is not set in the environment variables.")
    exit(1)

DOWNLOAD_DIR = "input/"

# Initialize Telegram clients
bot_client = TelegramClient('bot_session', int(API_ID), API_HASH)
user_client = TelegramClient('user_session', int(API_ID), API_HASH)

@bot_client.on(events.NewMessage(pattern='/start'))
async def send_welcome(event):
    await event.respond("Загрузите excel файл со названиями каналов")

@bot_client.on(events.NewMessage(func=lambda e: e.file))
async def handle_document(event):
    if not event.file or not event.file.name.endswith('.xlsx'):
        await event.reply("Please upload a valid Excel file.")
        return

    file_path = os.path.join(DOWNLOAD_DIR, event.file.name)
    await event.download_media(file=file_path)
    await event.reply("File downloaded successfully")

    await process_file(file_path, event.sender_id)

async def process_file(file_path, user_id):
    channels = pd.read_excel(file_path)["channel_name"].to_list()
    logger.info(f"Channels to fetch: {channels}")

    async with user_client:
        all_messages = await fetch_all_messages(channels)
        df = pd.DataFrame(all_messages, columns=['time', 'message_text', 'message_id', 'channel_name'])
        user_directory = f"output/{user_id}"
        if not os.path.exists(user_directory):
            os.makedirs(user_directory)
        output_file = f"{user_directory}/data.xlsx"
        df.to_excel(output_file, index=False)
        await bot_client.send_file(user_id, output_file, caption="Here are the collected messages.")

async def fetch_all_messages(channels):
    all_data = []
    for channel in channels:
        all_data.extend(await get_last_5_messages(channel))
    return all_data

async def get_last_5_messages(channel_username):
    channel = await user_client.get_entity(channel_username)
    result = await user_client(GetHistoryRequest(
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

async def start_bot_client():
    await bot_client.start(bot_token=BOT_TOKEN)
    me = await bot_client.get_me()
    logger.info(f"Bot `{me.username}` has started")
    await bot_client.run_until_disconnected()

async def start_user_client():
    await user_client.start(phone=PHONE_NUMBER)
    me = await user_client.get_me()
    logger.info(f"User `{me.username}` has started")

if __name__ == '__main__':
    import asyncio

    async def main():
        await start_user_client()
        await start_bot_client()

    # Check if the event loop is already running
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        loop.create_task(main())
    else:
        loop.run_until_complete(main())