import hmac
import time
import json
import datetime
from multiprocessing import Process, Queue
import websocket
import pandas as pd
import pprint

import threading


class Bybit_Stream:
    def __init__(self, key, secret, topic, harvest_time, funding_rate) -> None:
        self.topic = topic

        self.key = key
        self.secret = secret

        self.process = None

        self.df = None

        self.harvest_time = harvest_time
        self.funding_rate = funding_rate

    def on_message(self, ws, message):
        data = json.loads(message)
        print(data)

    def on_error(self, ws, error):
        print('we got error')
        print(error)

    def on_close(self, ws):
        print("### about to close please don't close ###")

    def on_pong(self, ws, *data):
        print('pong received')

    def on_ping(self, ws, *data):
        print('ping received')

    def send_auth(self, ws):
        expires = int((time.time() + 10) * 1000)
        _val = f'GET/realtime{expires}'
        signature = str(hmac.new(
            bytes(self.secret, 'utf-8'),
            bytes(_val, 'utf-8'), digestmod='sha256'
        ).hexdigest())
        ws.send(json.dumps({"op": "auth", "args": [self.key, expires, signature]}))

    def on_open(self, ws):
        print('opened')
        self.send_auth(ws)
        print('send subscription ' + self.topic)
        ws.send(json.dumps({"op": "subscribe", "args": [self.topic]}))

    # for websocket connection
    def connWS(self):
        ws = websocket.WebSocketApp(
            'wss://stream.bybit.com/realtime_private',
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_ping=self.on_ping,
            on_pong=self.on_pong,
            on_open=self.on_open
        )
        return ws

    def alert(self, q):
        ws = websocket.create_connection('wss://stream.bybit.com/realtime_private')
        self.send_auth(ws)
        self.on_open(ws)

        wallet_change = False

        while not wallet_change:
            data = json.loads(ws.recv())
            print(data)

            if 'topic' in data:
                if data['topic'] == 'wallet':
                    q.put([
                        True, datetime.datetime.now()
                    ])
                    wallet_change = True
            else:
                q.put([
                    False, datetime.datetime.now()
                ])

    def trade_log(self, q):
        ws = websocket.create_connection('wss://stream.bybit.com/realtime_public')
        self.on_open(ws)

        global data 
        data = ''

        ###
        def update():
            while True:
                global data
                data = json.loads(ws.recv())
            
        th = threading.Thread(
            target=update
        )
        th.daemon = True
        th.start()
        ###

        times, prices, sizes = [], [], []

        time_begin = datetime.datetime.now()
        _time = datetime.datetime.now()

        while True:
            if datetime.datetime.now() - _time > datetime.timedelta(seconds=15):
                ws.send(json.dumps({"op": "ping"}))

                _time = datetime.datetime.now()
                #print('>>>>', data)

            if 'data' in data:
                times.append(data['data'][0]['trade_time_ms'])
                prices.append(data['data'][0]['price'])
                sizes.append(data['data'][0]['size'])

                print(
                    data['data'][0]['price'], 
                    data['data'][0]['size'],
                    data['data'][0]['trade_time_ms'],
                )

                data = ''
            else:
                pass

            if datetime.datetime.now() - time_begin > datetime.timedelta(seconds=self.harvest_time):
                df_name = 'trade_logs/' + self.topic + ' ' + str(self.funding_rate) + '.csv'
                df = pd.DataFrame(
                    list(zip(times, prices, sizes)), 
                    columns = ['timestamp', 'price', 'size']
                )
                df.to_csv(df_name, index=False)
                
                print('>>>>', df_name + ' df saved')
                break

            time.sleep(0.2)

    def run(self, q):
        target = self.trade_log

        if self.topic == 'wallet':
            target = self.alert
        else:
            target = self.trade_log

        self.process = Process(
            target=target,
            args=(q,)
        )
        self.process.daemon = True
        self.process.start()

if __name__ == "__main__":
    key = 'Q7hUt22dcmeh63vVNS'
    secret = 'mzoKDEPcOc8etT3MUx1xGgdpk4cKnUk4Y6Jc'
    stream = Bybit_Stream(key, secret, 'trade.RSS3USDT')

    q = Queue()
    status = [False,]
    stream.run(q)

    while True: #status[0]:
        time.sleep(1)