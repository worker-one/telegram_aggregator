from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Message(Base):
    __tablename__ = 'messages'

    time = Column(DateTime, primary_key=True)
    message_text = Column(String)
    message_id = Column(Integer)
    channel_name = Column(String)

