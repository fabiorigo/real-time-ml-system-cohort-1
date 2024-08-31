import json

from datetime import datetime
from loguru import logger
from quixstreams import Application

from .config import config
from .hopsworks_api import HopsworksApi


def kafka_to_feature_store(
    kafka_topic: str,
    kafka_broker_address: str,
    feature_group_name: str,
    feature_group_version: int,
    feature_group_send_seconds: int,
    live_or_historical: str
) -> None:
    """
    Reads 'ohlc' data from the Kafka topic and writes it to the feature store
    More specifically, it writes the data to the feature group specified by
    - `feature_group_name` and `feature_group_version`
    """

    app = Application(
        broker_address=kafka_broker_address, 
        consumer_group='kafka_to_feature_store'
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])
        batch = []
        milestone = datetime.now().timestamp()
        api = HopsworksApi(
            feature_group_name=feature_group_name, 
            feature_group_version=feature_group_version,
            live_or_historical=live_or_historical
        )

        while True:
            msg = consumer.poll(1)
            if msg is None:
                logger.debug(f'No message')
                continue
            elif msg.error():
                logger.error('Kafka error: ', msg.error())
                continue
            else:
                # there is data that we need to send to the feature store
                logger.info('Consumed OHLC data')

                # step 1 -> parse the message from kafka into a dictionary
                ohlc = json.loads(msg.value().decode('utf-8'))
                batch.append(ohlc)

                # step 2 -> send data to the feature store
                if datetime.now().timestamp() - milestone > feature_group_send_seconds:
                    milestone = datetime.now().timestamp()
                    logger.info('Publishing an OHLC batch')
                    api.push_data_to_feature_store(data=batch)
                    batch = []

if __name__ == '__main__':
    try: 
        kafka_to_feature_store(
            kafka_topic=config.kafka_topic,
            kafka_broker_address=config.kafka_broker_address,
            feature_group_name=config.feature_group_name,
            feature_group_version=config.feature_group_version,
            feature_group_send_seconds=config.feature_group_send_seconds,
            live_or_historical=config.live_or_historical
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')
