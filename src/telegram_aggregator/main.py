import logging.config
import os
import urllib
import shutil

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from datetime import datetime, timedelta
from omegaconf import OmegaConf
from telethon import TelegramClient, events, sync
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel

from telegram_aggregator.db.database import create_tables
from telegram_aggregator.db.crud import add_message, add_channel, get_channel_names, get_messages_in_timerange

# Load logging configuration
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)
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


@bot_client.on(events.NewMessage(pattern='/update'))
async def handle_update(event):
    channel_names = get_channel_names()
    logger.info(f"Channels to fetch: {channel_names}")

    async with user_client:
        all_messages = await fetch_all_messages(channel_names)
        df = pd.DataFrame(all_messages)

        for index, row in df.iterrows():
            add_message(
                message_id=row['message_id'],
                time=row['time'],
                message_text=row['message_text'],
                channel_name=row['channel_name']
            )
            
        await event.reply(f"Last {len(all_messages)} messages from {len(channel_names)} channels have been aggregated")


@bot_client.on(events.NewMessage(pattern='/add_channel'))
async def handle_add_channel(event):
    # Extract the channel name from the message
    message_parts = event.text.split(maxsplit=1)
    if len(message_parts) < 2:
        await event.respond("Please provide the name of the channel.")
        return

    channel_name = message_parts[1].strip()

    # Add channel to the database
    try:
        add_channel(channel_name)
        await event.respond(f"Channel '{channel_name}' has been added successfully.")
    except Exception as e:
        logger.error(f"Failed to add channel '{channel_name}': {e}")
        await event.respond(f"Failed to add channel '{channel_name}'. Please try again later.")


@bot_client.on(events.NewMessage(pattern='/get_channels'))
async def handle_get_channels(event):
    # Extract the channel name from the message
    channel_names = get_channel_names()
    channel_names_str = '\n' + '\n-'.join(channel_names)
    await event.respond(f"The list of tracked channels: {channel_names_str}.")


@bot_client.on(events.NewMessage(pattern='/get_messages'))
async def handle_get_messages(event):
    # Ask the user for the number of hours
    
    message_parts = event.text.split(maxsplit=1)
    if len(message_parts) < 2:
        await event.respond("Please provide the number of hours for the time range (e.g., 2 for last 2 hours).")
        return

    response = message_parts[1].strip()
    
    try:
        n_hours = int(response.strip())
    except ValueError:
        await event.respond("Invalid number. Please provide a valid integer.")
        return

    # Calculate time range
    now = datetime.utcnow()
    start_time = now - timedelta(hours=n_hours)
    end_time = now

    logger.info(f"Fetching messages from {start_time} to {end_time}")

    # Query database for messages in this time range
    # Here you would implement logic to query messages from your database based on the start_time and end_time

    messages = get_messages_in_timerange(start_time, end_time)

    if not messages:
        await event.respond("No messages found for the given time range.")
        return

    # Save the messages to a file and send it to the user
    df = pd.DataFrame(
        [
            [message.id, message.time, message.message_text, message.channel_name]
            for message in messages
        ],
            columns=['message_id', 'time', 'message_text', 'channel_name']
    )
            
    user_directory = f"output/{event.sender_id}"
    
    if os.path.exists(user_directory):
        shutil.rmtree(user_directory)
    os.makedirs(user_directory)

    output_file = f"{user_directory}/messages_{int(start_time.timestamp())}_{int(end_time.timestamp())}.xlsx"
    df.to_excel(output_file, index=False)
    await bot_client.send_file(event.sender_id, output_file, caption="Here are the messages from the requested time range.")
    os.remove(output_file)


async def fetch_all_messages(channels) -> list[dict[str, str]]:
    all_data = []
    for channel in channels:
        if "@" not in channel:
            channel = f"@{channel}"
        try:
            all_data.extend(await get_last_5_messages(channel))
        except:
            logger.warning(f"Skip {channel}")
    return all_data

async def get_last_5_messages(channel_username: str) -> pd.DataFrame:
    channel = await user_client.get_entity(channel_username)
    result = await user_client(GetHistoryRequest(
        peer=PeerChannel(channel.id),
        limit=5,
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
            'message_id': message.id,
            'time': message.date.replace(tzinfo=None),
            'message_text': message.message,
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

    create_tables()

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
