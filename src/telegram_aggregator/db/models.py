from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Message(Base):
    __tablename__ = 'messages'

    message_id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    message_text = Column(String)
    channel_name = Column(String)

class Channel(Base):
    __tablename__ = "channels"

    channel_id = Column(Integer, primary_key=True, autoincrement=True)
    channel_name = Column(String)
    comment = Column(String)
