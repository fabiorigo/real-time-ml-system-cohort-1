import os
from typing import List
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_broker_address: str
    kafka_topic_name: str
    product_ids: List[str]
    live_or_historical: str
    last_n_days: int
    max_trades_per_sec: int

config = Config()