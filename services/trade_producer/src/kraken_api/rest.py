from typing import List, Dict
from loguru import logger
import requests
import json
import time

class KrakenRestAPI:

    def __init__(
            self, 
            product_ids: List[str],
            from_ms: int,
            to_ms: int
        ) -> None:
        
        self.URL='https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_sec}'
        self.product_ids=product_ids
        self.from_ms=from_ms
        self.to_ms=to_ms
        self.next_req = {}

        for id in product_ids:
            self.next_req[id] = from_ms

    def get_trades(self) -> List[Dict]:
        """
        Fetches a batch of trades from the Kraken REST API and returns them as a list
        of dictionaries.

        Args:
            None

        Returns:
            List[Dict]: A list of dictionaries, where each dictionary contains the trade data.
        """

        trades = []
        urls = [ self.URL.format(product_id=pid, since_sec=self.from_ms / 1000) for pid in self.product_ids ]

        for pid in self.product_ids:
            while pid in self.next_req:
                url = self.URL.format(product_id=pid, since_sec=self.next_req[pid] / 1000)

                # forcing an interval between calls, dut to Kraken rate limits
                time.sleep(1)
                response = requests.request("GET", url, headers={ 'Accept': 'application/json' }, data={})

                data = json.loads(response.text)

                if (data['error']) is not None and len(data['error']) > 0:
                    raise Exception(data['error']);

                if len(data['error']) == 0:
                    product_id = list(data['result'])[0]
                    for trade in data['result'][product_id]:
                        resolved_trade = {
                            'product_id': product_id,
                            'price': float(trade[0]),
                            'volume': float(trade[1]),
                            'timestamp': int(trade[2] * 1000),
                        }
                        logger.info(resolved_trade)
                        trades.append(resolved_trade)
                    
                    # Check if we are done fetching historical data
                    last_ts_ms = int(data['result']['last']) / 1_000_000
                    if last_ts_ms >= self.to_ms:
                        self.next_req.pop(pid)
                    else:
                        self.next_req[pid] = last_ts_ms

                    logger.debug(f'Fetched {len(trades)} trades')
                    logger.debug(f'Last trade timestamp: {last_ts_ms}')

        return trades

    def is_done(self) -> bool:
        return len(self.next_req) == 0