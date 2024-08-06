import logging.config
import os
import urllib

import pandas as pd
import telebot
from dotenv import find_dotenv, load_dotenv
from omegaconf import OmegaConf
import os
import logging.config


logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_aggregator/conf/logging_config.yaml"), resolve=True)

# Apply the logging configuration
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

load_dotenv(find_dotenv(usecwd=True))  # Load environment variables from .env file
TOKEN = os.getenv("BOT_TOKEN")


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


client = TelegramClient('test session', int(API_ID), API_HASH, loop=loop)

if TOKEN is None:
    logger.error("BOT_TOKEN is not set in the environment variables.")
    exit(1)

bot = telebot.TeleBot(TOKEN, parse_mode=None)

DOWNLOAD_DIR = "input/"

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "Загрузите excel файл со названиями каналов")

@bot.message_handler(content_types=['document'])
def handle_document(message):

    file_info = bot.get_file(message.document.file_id)
    file_url = f"https://api.telegram.org/file/bot{TOKEN}/{file_info.file_path}"

    # Download the file
    file_name = os.path.join(DOWNLOAD_DIR, message.document.file_name)
    urllib.request.urlretrieve(file_url, file_name)

    with open('./src/telegram_aggregator/api/message.py') as file:
        exec(file.read())


def start_bot():
    logger.info(f"bot `{str(bot.get_me().username)}` has started")
    bot.polling()
