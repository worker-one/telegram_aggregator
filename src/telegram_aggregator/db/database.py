import asyncio
import logging.config
import os

from dotenv import find_dotenv, load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from .models import Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(find_dotenv(usecwd=True))

# Retrieve environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SSL_MODE = os.getenv("SSL_MODE")
DATABASE_URL = None

# Check if required environment variables are set
if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    logger.error("One or more critical database environment variables are not set. Exiting.")
    raise SystemExit("Database configuration incomplete.")
else:
    # Construct the database URL
    DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    if SSL_MODE:
        DATABASE_URL += f"?sslmode={SSL_MODE}"

# Create the async engine
engine = create_async_engine(
    DATABASE_URL,
    connect_args={'connect_timeout': 10, "application_name": "telegram_aggregator"}
)

# Create a configured "Session" class
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

def get_session():
    """Provide a transactional scope around a series of operations."""
    return AsyncSessionLocal

async def create_tables():
    """Create database tables based on the metadata."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tables created successfully.")
