import os

from dotenv import find_dotenv, load_dotenv
from pydantic_settings import BaseSettings

load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_broker_address: str
    kafka_topic: str
    feature_group_name: str
    feature_group_version: int
    feature_group_send_seconds: int
    live_or_historical: str
    hopsworks_project_name: str
    hopsworks_api_key: str

config = Config()
