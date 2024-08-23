from datetime import timedelta

from loguru import logger
from quixstreams import Application

from .config import config


def trade_to_ohlc(
    kafka_input_topic: str,
    kafka_output_topic: str,
    kafka_broker_address: str,
    ohlc_window_seconds: int,
) -> None:
    """
    Reads trades from the kafka input topic
    Aggregates them into OHLC candles using the specified window in `ohlc_window_seconds`
    Saves the olhc data into another kafka topic

    Args:
        kafka_input_topic : str : Kafka topic to read trade data from
        kafka_output_topic : str : Kafka topic to write ohlc data to
        kafka_broker_address : str : Kafka broker address
        ohlc_window_seconds : int : Window size in seconds for OHLC aggregation

    Returns:
        None
    """

    # this handles all low level comunication with kafka
    app = Application(
        broker_address=kafka_broker_address, consumer_group='trade_to_ohlc'
    )

    # specify input and output topics for this application
    input_topic = app.topic(name=kafka_input_topic, value_serializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # creating a streaming data frame
    # to apply transformations on the incoming data
    sdf = app.dataframe(topic=input_topic)

    # apply transformations to the incoming data
    # 1. defines non overlapping time windows
    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=ohlc_window_seconds))
    # 2. consumes the trades within each time window and generate one candle per product; only stream the resulting candle in the end
    sdf = sdf.reduce(reducer=update_candle_dict, initializer=init_candle_dict).final()
    # 3. generate a stream of candles from the dictionary, one per entry
    sdf = sdf.apply(extract_candles_from_dict, expand=True)
    # 4. log the candles
    sdf = sdf.update(logger.info)
    # 5. publish the candles to the output topic
    sdf = sdf.to_topic(output_topic)

    # kick-off the streaming application
    app.run(sdf)


def init_candle_dict(value: dict) -> dict:
    candle_dict: dict = dict()

    if value['product_id'] in config.product_ids:
        candle_dict[value['product_id']] = {
            'open': value['price'],
            'high': value['price'],
            'low': value['price'],
            'close': value['price'],
            'product_id': value['product_id'],
        }

    return candle_dict


def update_candle_dict(candle_dict: dict, value: dict) -> dict:
    if value['product_id'] in config.product_ids:
        if value['product_id'] not in candle_dict.keys():
            candle_dict[value['product_id']] = {
                'open': value['price'],
                'high': value['price'],
                'low': value['price'],
                'close': value['price'],
                'product_id': value['product_id'],
            }
        else:
            candle_dict[value['product_id']]['high'] = max(
                candle_dict[value['product_id']]['high'], value['price']
            )
            candle_dict[value['product_id']]['low'] = min(
                candle_dict[value['product_id']]['low'], value['price']
            )
            candle_dict[value['product_id']]['close'] = value['price']

    return candle_dict


def extract_candles_from_dict(value: dict):
    candle_list = value['value'].values()
    for candle in candle_list:
        candle['timestamp'] = value['end']
    return candle_list


if __name__ == '__main__':
    try:
        trade_to_ohlc(
            kafka_input_topic=config.kafka_input_topic,
            kafka_output_topic=config.kafka_output_topic,
            kafka_broker_address=config.kafka_broker_address,
            ohlc_window_seconds=config.ohlc_window_seconds,
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')
