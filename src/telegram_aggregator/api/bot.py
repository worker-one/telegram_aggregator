import logging.config
import os
import urllib

import pandas as pd
import telebot
from dotenv import find_dotenv, load_dotenv
from omegaconf import OmegaConf

from telegram_aggregator.api.message import get_messages

logging_config = OmegaConf.to_container(OmegaConf.load("./src/telegram_bot/conf/logging_config.yaml"), resolve=True)

# Apply the logging configuration
logging.config.dictConfig(logging_config)

# Configure logging
logger = logging.getLogger(__name__)

load_dotenv(find_dotenv(usecwd=True))  # Load environment variables from .env file
TOKEN = os.getenv("BOT_TOKEN")

if TOKEN is None:
    logger.error("BOT_TOKEN is not set in the environment variables.")
    exit(1)

app_config = OmegaConf.load("./src/telegram_bot/conf/app.yaml")
bot = telebot.TeleBot(TOKEN, parse_mode=None)

DOWNLOAD_DIR = "input/"

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "Загрузите excel файл со названиями каналов")

@bot.message_handler(content_types=['document'])
def handle_document(message):
    try:
        file_info = bot.get_file(message.document.file_id)
        file_url = f"https://api.telegram.org/file/bot{TOKEN}/{file_info.file_path}"

        # Download the file
        file_name = os.path.join(DOWNLOAD_DIR, message.document.file_name)
        urllib.request.urlretrieve(file_url, file_name)

        df = pd.read_csv(file_name, header=["channel_name"])
        channels = df['channel_name'].tolist()
        get_messages(channels, message.user.id)


    except Exception as e:
        bot.reply_to(message, f"An error occurred: {e}")

def start_bot():
    logger.info(f"bot `{str(bot.get_me().username)}` has started")
    bot.infinity_polling()
