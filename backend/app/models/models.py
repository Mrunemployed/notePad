from sqlalchemy import Column,Integer,String,TIMESTAMP,BOOLEAN,DateTime, ForeignKey, Enum, func, JSON, BigInteger, Float, Text
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
import uuid
from datetime import datetime
import uuid

print(sqlalchemy.__version__)

Base = declarative_base()

class tables:
    USER = "user"


def CreateUUID():
    """
    Generate a unique UUID for user identification.

    Returns:
        str: A string representation of the generated UUID.
    """
    return str(uuid.uuid4().hex)


class users(Base):
    __tablename__ = tables.USER
    user_id = Column(String, primary_key=True, default=CreateUUID)
    username = Column(String, nullable=False)
    password = Column(String, nullable=False)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False)


class schemas:
    USERS = users

