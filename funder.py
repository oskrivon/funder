from time import sleep
import datetime
import threading
import yaml

from pybit import usdt_perpetual

import connector


class Funder:
    def __init__(self, qoutation, qty) -> None:
        with open('config.yaml') as f:
            cfg = yaml.safe_load(f)

        self.bybit_connector = connector.Connector(
            cfg['api'], cfg['secret'], cfg['endpoint']
        )

        self.bybit_socket = usdt_perpetual.WebSocket(
            test=False,
            api_key=cfg['api'],
            api_secret=cfg['secret'],
            ping_interval=30,
            ping_timeout=10,
            domain=cfg['domain']
        )

        self.qoutation = qoutation
        self.qty = qty

        self.funding_flag = False

        print('>>> funder init')
    

    def wallet_changes_check(self, msg):
        self.funding_flag = True
        
        print(datetime.datetime.now(), msg)
        
        self.log_update('a', 'wallet change')

    
    def log_update(self, mod, note):
        with open("log.txt", mod) as file:
            file.write(str(datetime.datetime.now()) + ' ' + note + '\n')


    def funder(self):
        while True:
            now = datetime.datetime.now()

            if now > datetime.datetime.strptime('2023-01-15 10:59:58', "%Y-%m-%d %H:%M:%S"):
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

                        print('>>> funding')
                        self.funding_flag = False
                        
                        break
                break

            else:
                pass


if __name__ == '__main__':
    funder = Funder('1000BTTUSDT', 1000) # SOLUSDT
    
    funder.funder()
