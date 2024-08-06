import json

from loguru import logger
from quixstreams import Application

from src.config import config
from src.hopsworks_api import push_data_to_feature_store


def kafka_to_feature_store(
    kafka_topic: str,
    kafka_broker_address: str,
    feature_group_name: str,
    feature_group_version: int,
) -> None:
    """
    Reads 'ohlc' data from the Kafka topic and writes it to the feature store
    More specifically, it writes the data to the feature group specified by
    - `feature_group_name` and `feature_group_version`
    """

    app = Application(
        broker_address=kafka_broker_address, consumer_group='kafka_to_feature_store'
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])

        while True:
            msg = consumer.poll(1)
            if msg is None:
                continue
            elif msg.error():
                logger.error('Kafka error: ', msg.error())
                continue
            else:
                # there is data that we need to send to the feature store

                # step 1 -> parse the message from kafka into a dictionary
                ohlc = json.loads(msg.value().decode('utf-8'))

                # step 2 -> send data to the feature store
                push_data_to_feature_store(
                    feature_group_name=feature_group_name,
                    feature_group_version=feature_group_version,
                    data=ohlc,
                )


if __name__ == '__main__':
    kafka_to_feature_store(
        kafka_topic=config.kafka_topic,
        kafka_broker_address=config.kafka_broker_address,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
    )
