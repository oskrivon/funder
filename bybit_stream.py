import hmac
import time
import json
import datetime
from multiprocessing import Process, Queue
import websocket

import threading


class Bybit_Stream:
    def __init__(self, key, secret) -> None:
        self.topic = "wallet"

        self.key = key
        self.secret = secret

        self.process = None

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

    def run(self, q):
        self.process = Process(
            target=self.alert,
            args=(q,)
        )

        self.process.daemon = True
        self.process.start()

        #status = [False,]
        #while not status[0]:
        #    status = q.get()

if __name__ == "__main__":
    key = 'Q7hUt22dcmeh63vVNS'
    secret = 'mzoKDEPcOc8etT3MUx1xGgdpk4cKnUk4Y6Jc'
    stream = Bybit_Stream(key, secret)

    q = Queue()
    status = [False,]
    stream.run(q)

    def process_status():
        while True:
            print(stream.process.is_alive())
            time.sleep(1)

    th = threading.Thread(
        target=process_status
    )
    th.daemon = True
    th.start()

    while True: #status[0]:
        status = q.get()
        print(status[0])