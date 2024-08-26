import time

from time import sleep
from typing import Dict, List

from loguru import logger
from quixstreams import Application

from .config import config
from .kraken_api.websocket import KrakenWebsocketTradeApi
from .kraken_api.rest import KrakenRestAPI
from .kraken_api.trade import Trade


def produce_trades(
    kafka_broker_address: str, 
    kafka_topic_name: str,
    product_ids: List[str],
    live_or_historical: str,
    last_n_days: int
    ) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_topic_name (str): The name of the Kafka topic.

    Returns:
        None
    """
    assert live_or_historical in { "live", "historical" }, f"Invalid value for live_or_historical: {live_or_historical}"

    app = Application(broker_address=kafka_broker_address)

    # the topic where we will save the trades
    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    # Create an instance of the Kraken API
    if live_or_historical == 'live':
        kraken_api = KrakenWebsocketTradeApi(product_ids=product_ids)
    else:
        to_ms = int(time.time() * 1000)
        from_ms = to_ms - last_n_days * 24 * 60 * 60 * 1000
        kraken_api = KrakenRestAPI(product_ids=product_ids, from_ms=from_ms, to_ms=to_ms)

    logger.info('Creating the producer...')

    # Create a Producer instance
    with app.get_producer() as producer:
        while True:
            if kraken_api.is_done():
                logger.info('Done fetching historical data')
                break

            # Get the trades from the Kraken API
            trades: List[Trade] = kraken_api.get_trades()

            for trade in trades:
                # Serialize an event using the defined Topic
                message = topic.serialize(key=trade.product_id, value=trade.model_dump())

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                # we produce a maximum of around 100 messages per second, so that the next microservice does not discard historical messages
                sleep(0.01)

                logger.info(message.value)


if __name__ == '__main__':
    try:
        produce_trades(
            kafka_broker_address=config.kafka_broker_address,
            kafka_topic_name=config.kafka_topic_name,
            product_ids=config.product_ids,
            live_or_historical=config.live_or_historical,
            last_n_days=config.last_n_days
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')
