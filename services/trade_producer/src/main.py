from time import sleep
from typing import Dict, List

from loguru import logger
from quixstreams import Application

from src import config
from src.kraken_api import KrakenWebsocketTradeApi


def produce_trades(kafka_broker_address: str, kafka_topic_name: str) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_topic_name (str): The name of the Kafka topic.

    Returns:
        None
    """
    app = Application(broker_address=kafka_broker_address)

    # the topic where we will save the trades
    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    # Create an instance of the Kraken API
    kraken_api = KrakenWebsocketTradeApi(product_ids=config.product_ids)

    logger.info('Creating the producer...')

    # Create a Producer instance
    with app.get_producer() as producer:
        while True:
            # Get the trades from the Kraken API
            trades: List[Dict] = kraken_api.get_trades()

            for trade in trades:
                # Serialize an event using the defined Topic
                message = topic.serialize(key=trade['product_id'], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.info('Message sent!')

            sleep(1)


if __name__ == '__main__':
    produce_trades(
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic_name=config.kafka_topic_name,
    )
