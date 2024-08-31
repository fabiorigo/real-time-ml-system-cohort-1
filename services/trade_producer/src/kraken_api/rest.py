from typing import List, Dict
from loguru import logger
from datetime import datetime
from .trade import Trade
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
        self.lastRoundTradeIds = []

        for pid in product_ids:
            self.next_req[pid] = from_ms

    def get_trades(self) -> List[Trade]:
        """
        Fetches a batch of trades from the Kraken REST API and returns them as a list
        of dictionaries.

        Args:
            None

        Returns:
            List[Dict]: A list of dictionaries, where each dictionary contains the trade data.
        """

        trades: List[Trade] = []
        thisRoundTradeIds = []

        for pid in self.product_ids:
            url = self.URL.format(product_id=pid, since_sec=int(self.next_req[pid] / 1000))

            response = requests.request("GET", url, headers={ 'Accept': 'application/json' }, data={})

            data = json.loads(response.text)

            if (data['error']) is not None and len(data['error']) > 0:
                raise Exception(data['error']);

            if len(data['error']) == 0:
                product_id = list(data['result'])[0]
                for trade in data['result'][product_id]:
                    thisRoundTradeIds.append(trade[6])
                    if (trade[6] not in self.lastRoundTradeIds):
                        t = Trade(
                            product_id=product_id,
                            price=float(trade[0]),
                            volume=float(trade[1]),
                            timestamp_sec=int(trade[2])
                        )
                        trades.append(t)
                
                # Check if we are done fetching historical data
                last_ts_ms = int(int(data['result']['last']) / 1_000_000)
                if last_ts_ms >= self.to_ms:
                    self.next_req.pop(pid)
                else:
                    self.next_req[pid] = last_ts_ms + 1000

                logger.info(f'Fetched {len(trades)} trades')

            # forcing an interval between calls, due to Kraken rate limits
            time.sleep(0.9)

        def _by_timestamp(t: Trade):
            return t.timestamp_sec

        trades.sort(key=_by_timestamp)
        first_ts=datetime.fromtimestamp(trades[0].timestamp_sec).isoformat()
        last_ts=datetime.fromtimestamp(trades[len(trades)-1].timestamp_sec).isoformat()
        logger.debug(f"first_timestamp:'{first_ts}' last_timestamp:'{last_ts}'")

        self.lastRoundTradeIds = thisRoundTradeIds

        return trades

    def is_done(self) -> bool:
        return len(self.next_req) == 0