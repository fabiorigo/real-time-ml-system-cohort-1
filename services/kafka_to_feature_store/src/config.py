import os

from dotenv import find_dotenv, load_dotenv
from pydantic_settings import BaseSettings

load_dotenv(find_dotenv())


class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_topic: str = 'candles'
    feature_group_name: str = 'ohlc_feature_group'
    feature_group_version: int = 1
    feature_group_send_seconds: int = os.environ['FEATURE_GROUP_SEND_SECONDS']
    hopsworks_project_name: str = (os.environ['HOPSWORKS_PROJECT_NAME'],)
    hopsworks_api_key: str = os.environ['HOPSWORKS_API_KEY']


config = Config()
