import os
from typing import List
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_topic_name: str = 'trades'
    product_ids: List[str] = [
        'BTC/USD',
        'ETH/USD',
        'USDT/USD',
        'SOL/USD',
        'USDC/USD',
        'XRP/USD',
        'DOGE/USD',
        'ADA/USD',
        'TRX/USD',
        'AVAX/USD',
    ]

config = Config()