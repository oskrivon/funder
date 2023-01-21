import time
import datetime
import threading
import yaml

from pybit import usdt_perpetual
import schedule

import connector
import entry_points_comput as epc


class Funder:
    def __init__(self, qoutation, qty) -> None:
        with open('config.yaml') as f:
            cfg = yaml.safe_load(f)

        self.bybit_socket = usdt_perpetual.WebSocket(
            test=False,
            api_key=cfg['api'],
            api_secret=cfg['secret'],
            ping_interval=30,
            ping_timeout=10,
            domain=cfg['domain']
        )

        self.session = usdt_perpetual.HTTP(
            endpoint=cfg['endpoint'],
            api_key=cfg['api'],
            api_secret=cfg['secret']
        )

        self.bybit_connector = connector.Connector(self.session)

        self.qoutation = qoutation
        self.qty = qty

        self.funding_flag = False

        self.entry_points = epc.entry_points_comput(cfg['funding_times'], cfg['offset'])
        print(self.entry_points)
        for point in self.entry_points:
            schedule.every().day.at(point).do(self.funder)

        print('>>> funder init')

        while True:
            schedule.run_pending()
            time.sleep(1)
    

    def wallet_changes_check(self, msg):
        self.funding_flag = True
        
        print(datetime.datetime.now(), msg)
        
        self.log_update('a', 'wallet change')

    
    def log_update(self, mod, note):
        with open("log.txt", mod) as file:
            file.write(str(datetime.datetime.now()) + ' ' + note + '\n')


    def funder(self):
        self.log_update('w', 'input')
        print(self.bybit_connector.create_trade(self.qoutation, 'Buy', self.qty, False, False))
        self.log_update('a', 'open trade')

        self.bybit_socket.wallet_stream(  # wallet_stream trade_stream
            self.wallet_changes_check
        )
        self.log_update('a', 'socket wallet_stream connected')
        print('>>> socket create')

        while True:
            if self.funding_flag:
                self.log_update('a', 'get funding flag')
                print(self.bybit_connector.create_trade(self.qoutation, 'Sell', self.qty, False, False))
                self.log_update('a', 'close trade')
                self.funding_flag = False

                break


if __name__ == '__main__':
    funder = Funder('CTCUSDT', 10) # SOLUSDT

