import os
from typing import List

from dotenv import find_dotenv, load_dotenv
from pydantic_settings import BaseSettings

load_dotenv(find_dotenv())


class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_input_topic: str = 'trades'
    kafka_output_topic: str = 'candles'
    ohlc_window_seconds: int = os.environ['OHLC_WINDOW_SECONDS']
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
