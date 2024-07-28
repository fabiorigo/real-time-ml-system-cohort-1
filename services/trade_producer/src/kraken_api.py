import json
from typing import Dict, List

from loguru import logger
from websocket import create_connection


class KrakenWebsocketTradeApi:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(
        self,
        product_id: str,
    ):
        self.product_id = product_id

        # establish connection to the Kraken websocket API
        self._ws = create_connection(self.URL)
        logger.info('Connection established')

        # subscribe to the trades for the given `product_id`
        self._subscribe(product_id)

    def _subscribe(self, product_id: str):
        """
        Establish the connection to Kraken websocket API and subscribe to the trades for the given `product_id`
        """
        logger.info(f'Subscribing to the trades for {product_id}')

        # let's subscribe to the trades fro the given 'product_id'
        msg = {
            'method': 'subscribe',
            'params': {'channel': 'trade', 'symbol': [product_id], 'snapshot': False},
        }

        self._ws.send(json.dumps(msg))
        logger.info('Subscription worked!')

        # dumping the first 2 messages we got from the websocket, because they contain no trade data,
        # just confirmation on their end that the subscription was successful
        _ = self._ws.recv()
        _ = self._ws.recv()

    def get_trades(self) -> List[Dict]:
        message = self._ws.recv()

        if 'heartbeat' in message:
            # when I get a heartbeat, I return an empty list
            return []

        # parse the message string as a dictionary
        message = json.loads(message)

        trades = []
        for trade in message['data']:
            trades.append(
                {
                    'product_id': self.product_id,
                    'price': trade['price'],
                    'volume': trade['qty'],
                    'timestamp': trade['timestamp'],
                }
            )

        return trades


## PAREI NO VIDEO 4, 1:00:00

##
## (Pdb) message
## '{"channel":"status","data":[{"api_version":"v2","connection_id":15564437896659189083,"system":"online","version":"2.0.7"}],"type":"update"}'
## (Pdb) message = self._ws.recv()
## (Pdb) message
## '{"method":"subscribe","result":{"channel":"trade","snapshot":false,"symbol":"BTC/USD"},"success":true,"time_in":"2024-07-27T21:15:57.923475Z","time_out":"2024-07-27T21:15:57.923542Z"}'
## (Pdb) message = self._ws.recv()
## (Pdb) message
## '{"channel":"heartbeat"}'
## (Pdb) message = self._ws.recv()
## (Pdb) message
## '{"channel":"heartbeat"}'
## (Pdb) message = self._ws.recv()
## (Pdb) message
## '{"channel":"heartbeat"}'
## (Pdb) message = self._ws.recv()
## (Pdb) message
## '{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":68580.0,"qty":0.14225864,"ord_type":"limit","trade_id":72166031,"timestamp":"2024-07-27T21:16:00.512060Z"}]}'
