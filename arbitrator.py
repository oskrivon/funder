import threading
from multiprocessing import Queue
import requests
import json
import yaml
import time
import pprint
import pandas as pd
import matplotlib.pyplot as plt
import datetime

from pybit import usdt_perpetual

import connector
import bybit_stream
import plotter
import tg_bot as bot

class Arbitrator:
    def __init__(self) -> None:
        with open('config.yaml') as f:
            self.cfg = yaml.safe_load(f)

        # plotter initializing
        self._plotter = plotter.Plotter('reports/DOMs')

        self.prices_linear = {}
        self.prices_spot = {}

        self.orderbook_linear = {}
        self.orderbook_spot = {}

        self.difference = {}

        diffs_historycal_df = pd.read_csv('reports/max_diffs.csv')
        self.diffs_historycal = dict(
            zip(diffs_historycal_df['quotation'], diffs_historycal_df['difference'])
        )
        print(self.diffs_historycal)

        self.arbitrator_bot = bot.Bot(self.cfg['tg_token'])
        self.arbitrator_bot.run()

    
    # returns quotes that are available on both the spot and futures markets
    def get_quotes(self):
        url_linear = 'https://api.bybit.com/v5/market/instruments-info?category=linear'
        url_spot = 'https://api.bybit.com/v5/market/instruments-info?category=spot'

        def symbols_separatop(raw_data):
            result_list = json.loads(raw_data.text)['result']['list']
            return [x['symbol'] for x in result_list]
        
        res_linear = requests.get(url_linear)
        res_spot = requests.get(url_spot)

        return list(
            set(symbols_separatop(res_linear)) & 
            set(symbols_separatop(res_spot))
        )
    
    # later separated into a separate class !!!
    def _parse(self, sample, market_type):
        if 'publicTrade' in sample['topic']:
            price = float(sample['data'][0]['p'])
            symbol = sample['topic'].replace('publicTrade.', '')

            if market_type == 'linear':
                self.prices_linear[symbol] = price
            elif market_type == 'spot':
                self.prices_spot[symbol] = price

        elif 'orderbook' in sample['topic']:
            symbol = sample['topic'].replace('orderbook.50.', '')

            asks = sample['data']['a']
            bids = sample['data']['b']

            asks_prices = [float(x[0]) for x in asks]
            asks_volumes = [float(x[1]) for x in asks]
            ask_side = ['ask'] * len(asks_prices)

            bids_prices = [float(x[0]) for x in bids]
            bids_volumes = [float(x[1]) for x in bids]
            bids_side = ['bid'] * len(bids_prices)

            prices = asks_prices + bids_prices
            volumes = asks_volumes + bids_volumes
            sides = ask_side + bids_side
            
            df = pd.DataFrame([prices, volumes, sides]).transpose()
            df.columns=['price', 'volume', 'side']

            if market_type == 'linear':
                df.columns=['price', 'volume_linear', 'side_linear']
                self.orderbook_linear[symbol] = df
            elif market_type == 'spot':
                df.columns=['price', 'volume_spot', 'side_spot']
                self.orderbook_spot[symbol] = df
    
    # get an update q-data from the stream
    def _update(self, q, market_type):
        while True:
            data = q.get()
            self._parse(data, market_type)
    
    # run a stream with data update from the connector
    def run_update_thread(self, q, market_type):
        data_update_th = threading.Thread(
            target=self._update,
            args=(q, market_type)
        )
        data_update_th.daemon = True
        data_update_th.start()

    # set and run instance of stream for market type and data type
    def run_data_stream(self, quotes, topics, market_type, once_flag = False):
        q = Queue()
        stream = bybit_stream.Bybit_Stream(
            '', '', market_type, topics, quotes, 0, once_flag
        )
        stream.run(q)
        self.run_update_thread(q, market_type)
    
    # need separate to other class
    def _get_orderbook(self, market_type, quote, limit):
        endpoint = 'https://api.bybit.com/v5/market/orderbook'
        params = {
            'category': market_type,
            'symbol': quote,
            'limit': limit
        }
        r = requests.get(endpoint, params=params)
        data = json.loads(r.text)
        print(data)
    
    # get both future and spot orderbooks and assembly it
    def get_pivot_orderbook(srlf, quote):
        pass

    # calculates the difference between the prices 
    # of the spot and futures markets as a percentage
    # diff = (fut - spot) / fut * 100
    def difference_calculation(self):
        for key in self.prices_linear:
            if key in self.prices_spot:
                diff = round(
                    (self.prices_linear[key] - self.prices_spot[key]) / self.prices_linear[key] * 100, 4
                )
                if diff != 0:
                    self.difference[key] = diff
        return self.difference
    
    # the main process of the program. 
    # launches streams for the spot and futures markets. 
    # data from streams goes to dictionaries declared during initialization. 
    # the process processes dictionaries by condition and generates reports
    def arbitration_process(self):
        # run streams for spot and future markets
        self.run_data_stream(quotes, 'publicTrade', 'linear')
        self.run_data_stream(quotes, 'publicTrade', 'spot')

        flag = True # temporary condition for DOM request !!!
        flag_img_creation = False # temporary condition for report creation !!!

        target_quotes = {}

        while True:
            threshold = 1 # threshold for quotes filtration

            time.sleep(1)
            difference = arbitrator.difference_calculation() # dict x{quote: diff}

            if len(difference) > 5: # condition
                print(difference)
                
                greater_th_quotes = {key: val for key, val in difference.items() if val >= threshold or val <= -1 * threshold}

                # added quotes to the target quotes dict
                for key in greater_th_quotes:
                    if key not in target_quotes:
                        target_quotes[key] = greater_th_quotes[key]

                        # action
                        print('new quotes!', key)
                        self._get_orderbook('linear', key, 50)
                        #self.arbitrator_bot.broadcast(str(key) + ':  ' + str(greater_th_quotes[key]))
                
                target_quotes = greater_th_quotes

                print(target_quotes)

                difference_sorted = sorted(
                    difference.items(), key=lambda x: (abs(x[1]), x[1]), reverse=True
                ) # get top 5

                print(difference_sorted[:5])
                print(target_quotes)
                quotes_for_orderbooks = [x[0] for x in difference_sorted]
                
                if not flag: # temporary condition for DOM request !!!
                    flag = True
                    for quotation in quotes_for_orderbooks:
                        print(quotation)
                        self.run_data_stream([quotation], 'orderbook.50', 'spot', once_flag=True)
                        self.run_data_stream([quotation], 'orderbook.50', 'linear', once_flag=True)

                if len(self.orderbook_spot) == 5:
                    if len(self.orderbook_linear) == 5:
                        if not flag_img_creation:
                            flag_img_creation = True

                            for key in self.orderbook_spot:
                                print(key)

                                # create pivot df
                                df = self.orderbook_spot[key].merge(
                                    self.orderbook_linear[key], 
                                    left_on = 'price', 
                                    right_on = 'price', how = 'outer'
                                )

                                if key in self.diffs_historycal:
                                    self._plotter.create_DOM_report(
                                        df, key,
                                        'spot',
                                        self.diffs_historycal[key],
                                        difference[key]
                                    )
                            #print('>>>>', self.orderbook_spot)

            time.sleep(0.5)


if __name__ == '__main__':
    arbitrator = Arbitrator()
    quotes = arbitrator.get_quotes()
    print(len(quotes))

    arbitrator.arbitration_process()