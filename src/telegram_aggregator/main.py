from typing import List
import os
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from fastapi import FastAPI
from omegaconf import OmegaConf
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel


class UserInput(BaseModel):
    channels_list: List[str]
    last_k_messages: int

app = FastAPI()

# define and configure logger
import logging.config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load logging configuration
logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(find_dotenv(usecwd=True))
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

if not API_ID or not API_HASH or not PHONE_NUMBER:
    logger.error("API_ID, API_HASH, or PHONE_NUMBER is not set in the environment variables.")
    exit(1)

DOWNLOAD_DIR = "input/"

# Initialize Telegram clients
user_client = TelegramClient('user_session', int(API_ID), API_HASH)

@app.post("/get_last_messages")
async def process_channels(user_input: UserInput):
    channels = user_input.channels_list
    logger.info(f"Channels to fetch: {channels}")

    all_messages = await fetch_all_messages(channels, user_input.last_k_messages)
    df = pd.DataFrame(all_messages, columns=['time', 'message_text', 'message_id', 'channel_name'])
    output_file = "output/data.xlsx"
    df.to_excel(output_file, index=False)
    return {"message": "Data processed successfully", "file": output_file}

async def fetch_all_messages(channels, last_k_messages):
    all_data = []
    for channel in channels:
        all_data.extend(await get_last_k_messages(channel, last_k_messages))
    return all_data

async def get_last_k_messages(channel_username, last_k_messages):
    channel = await user_client.get_entity(channel_username)
    result = await user_client(GetHistoryRequest(
        peer=PeerChannel(channel.id),
        limit=last_k_messages,
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


async def start_user_client():
    await user_client.start(phone=PHONE_NUMBER)
    me = await user_client.get_me()
    logger.info(f"User `{me.username}` has started")


if __name__ == '__main__':
    import asyncio

    import uvicorn

    async def main():
        await start_user_client()


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

    uvicorn.run(app, host="0.0.0.0", port=8000)
