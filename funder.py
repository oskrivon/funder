import time
import datetime
from multiprocessing import Process, Queue
import threading
import yaml

from pybit import usdt_perpetual
import schedule

import connector
import bybit_stream
import entry_points_comput as epc


class Funder:
    def __init__(self, qoutation, qty) -> None:
        with open('config.yaml') as f:
            self.cfg = yaml.safe_load(f)

        self.session = usdt_perpetual.HTTP(
            endpoint=self.cfg['endpoint'],
            api_key=self.cfg['api'],
            api_secret=self.cfg['secret']
        )

        self.bybit_connector = connector.Connector(self.session)

        self.qoutation = qoutation
        self.qty = qty

        self.funding_flag = False

        self.wallet_status = [False,]

        self.entry_points = epc.entry_points_comput(self.cfg['funding_times'], self.cfg['offset'])
        print(self.entry_points)
        for point in self.entry_points:
            schedule.every().day.at(point).do(self.funder)

        print('>>> funder init')

        self.bybit_socket = usdt_perpetual.WebSocket(
                test=False,
                api_key=self.cfg['api'],
                api_secret=self.cfg['secret'],
                ping_interval=30,
                ping_timeout=10,
                domain=self.cfg['domain']
            )

        while True:
           #while not self.funding_flag:
            schedule.run_pending()
            #print(self.wallet_status[0])
            time.sleep(1)

            if self.funding_flag:
                break
    

    def wallet_changes_check(self, msg):
        self.funding_flag = True
        
        print(datetime.datetime.now(), msg)
        
        self.log_update('a', 'wallet change')

    
    def log_update(self, mod, note):
        with open("log.txt", mod) as file:
            file.write(str(datetime.datetime.now()) + ' ' + note + '\n')

    
    def wallet_stream_creation(self):
        q = Queue()
        stream = bybit_stream.Bybit_Stream(
            self.cfg['api'],
            self.cfg['secret']
        )
        stream.run(q)

        # wallet change check
        def wallet_check():
            while True:
                self.wallet_status = q.get()
                #print(self.wallet_status)

        wallet_check_th = threading.Thread(
            target=wallet_check
        )
        wallet_check_th.daemon = True
        wallet_check_th.start()
        # wallet change check end


    def funder(self):
        self.log_update('w', 'input')
        #print(self.bybit_connector.create_trade(self.qoutation, 'Buy', self.qty, False, False))
        print('open trade')
        self.log_update('a', 'open trade')

        #self.bybit_socket.wallet_stream(  # wallet_stream trade_stream
        #    self.wallet_changes_check#, 'BTCUSDT'
        #)

        #q = Queue() print(self.wallet_status)
        self.wallet_stream_creation()

        self.log_update('a', 'socket wallet_stream connected')
        print('>>> socket create')

        while True:
            if self.wallet_status[0]:
                print(self.wallet_status[0])
                self.log_update('a', 'get funding flag')
                #print(self.bybit_connector.create_trade(self.qoutation, 'Sell', self.qty, False, False))
                print('close trade')
                self.log_update('a', 'close trade')
                self.funding_flag = False

                #print(self.bybit_socket.active_connections)
                break
            else:
                pass


if __name__ == '__main__':
    funder = Funder('BOBAUSDT', 20) # SOLUSDT