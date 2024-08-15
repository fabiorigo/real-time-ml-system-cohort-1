import os
from typing import List

from dotenv import find_dotenv, load_dotenv
from pydantic_settings import BaseSettings

load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_broker_address: str
    kafka_input_topic: str
    kafka_output_topic: str
    ohlc_window_seconds: int
    product_ids: List[str]

config = Config()
