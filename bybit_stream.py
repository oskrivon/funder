import hmac
import time
import json
from datetime import datetime as dt
from datetime import timedelta as delta
from multiprocessing import Process, Queue
import websocket
import pandas as pd
import pprint

import threading


SPOT_WSS = 'wss://stream.bybit.com/v5/public/spot'
LINEAR_WSS = 'wss://stream.bybit.com/v5/public/linear'
INVERSE_WSS = 'wss://stream.bybit.com/v5/public/inverse'
USDC_WSS = 'wss://stream.bybit.com/v5/public/option'

PRIVATE_WSS = 'wss://stream.bybit.com/v5/private'


class Bybit_Stream:
    def __init__(self, key, secret, market_type, topics, quotes, harvest_time):
        self.topics = topics
        self.quotes = quotes

        self.key = key
        self.secret = secret
        self.market_type = market_type

        self.df = None

        self.harvest_time = harvest_time

        self.endpoint = {
            'spot': 'wss://stream.bybit.com/v5/public/spot',
            'linear': 'wss://stream.bybit.com/v5/public/linear',
            'inverse': 'wss://stream.bybit.com/v5/public/inverse',
            'option': 'wss://stream.bybit.com/v5/public/option',
            'private': 'wss://stream.bybit.com/v5/private'
        }

    
    def _operation(self, op, args):
        self.ws.send(
            json.dumps({
                'op': op,
                'args': args
            })
        )

    def _auth(self):
        expires = int((time.time() + 10) * 1000)

        _val = f'GET/realtime{expires}'
        signature = str(hmac.new(
            bytes(self.secret, 'utf-8'),
            bytes(_val, 'utf-8'), digestmod='sha256'
        ).hexdigest())

        self._operation('auth', [self.key, expires, signature])

    def _subscribe(self, topics, quotes):
        full_topics = []
        for quotation in quotes:
            full_topics.append(topics + '.' + quotation)
        print('___________\n', full_topics)
        self._operation('subscribe', full_topics)

    # sketch
    def _unsubscribe(self, topics):
        self._operation('unsubscribe', topics)

    # sketch
    def add_topics(self, topics):
        self._subscribe(topics)

    # sketch
    def remove_topics(self, topics):
        self._unsubscribe(topics)

    def trade_log(self, q):
        self.ws = websocket.create_connection(self.endpoint[self.market_type])
        if self.market_type == 'private':
            self._auth()
        self._subscribe(self.topics, self.quotes)

        global data 
        data = ''

        def update():
            while True:
                global data
                data = json.loads(self.ws.recv())
                if 'topic' in data:
                    q.put(data)
            
        # thread for updating data from socket 
        th = threading.Thread(
            target=update
        )
        th.daemon = True
        th.start()

        time_begin = dt.now()
        time_ping = dt.now()

        times, prices, sizes, sides = [], [], [], []

        while True:
            if dt.now() - time_ping > delta(seconds=15):
                self.ws.send(json.dumps({"op": "ping"}))
                time_ping = dt.now()

            if self.harvest_time > 0:
                if dt.now() - time_begin > delta(seconds=self.harvest_time):
                    df_name = 'trade_logs/' + self.topics + ' ' + str(self.funding_rate) + '.csv'
                    df = pd.DataFrame(
                        list(zip(times, prices, sizes, sides)), 
                        columns = ['timestamp', 'price', 'size', 'side']
                    )
                    df.to_csv(df_name, index=False)
                    
                    print('>>>>', df_name + ' df saved')
                    break

            time.sleep(0.2)

    def run(self, q):
        target = self.trade_log

        """
        if self.topic == 'wallet':
            target = self.alert
        else:
        """

        self.process = Process(
            target=target,
            args=(q,)
        )
        self.process.daemon = True
        self.process.start()

if __name__ == "__main__":
    key = 'Q7hUt22dcmeh63vVNS'
    secret = 'mzoKDEPcOc8etT3MUx1xGgdpk4cKnUk4Y6Jc'
    stream = Bybit_Stream(key, secret, 'trade.BTCUSDT', 20, 0.2)

    q = Queue()
    status = [False,]
    stream.run(q)

    while True: #status[0]:
        time.sleep(1)