import threading
from multiprocessing import Queue
import requests
import json
import yaml
import time
import pprint

from pybit import usdt_perpetual

import connector
import bybit_stream

class Arbitrator:
    def __init__(self) -> None:
        with open('config.yaml') as f:
            self.cfg = yaml.safe_load(f)

        self.prices_linear = {}
        self.prices_spot = {}

        self.orderbook_linear = {}
        self.orderbook_spot = {}

        self.difference = {}


    
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
    
    def _parse(self, sample, market_type):
        if 'publicTrade' in sample['topic']:
            price = float(sample['data'][0]['p'])
            symbol = sample['topic'].replace('publicTrade.', '')

            if market_type == 'linear':
                self.prices_linear[symbol] = price
            elif market_type == 'spot':
                self.prices_spot[symbol] = price

        elif 'orderbook' in sample['topic']:
            #print('>>>> ', sample['topic'], ' ', market_type, '\n', sample['data']['b'], '\n___\n', sample['data']['a'])
            print('>>>>', market_type, ' ', sample)
    
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

    def difference_calculation(self):
        for key in self.prices_linear:
            if key in self.prices_spot:
                diff = round(
                    (self.prices_linear[key] - self.prices_spot[key]) / self.prices_linear[key] * 100, 4
                )
                if diff != 0:
                    self.difference[key] = diff
        return self.difference
    
    def arbitration_process(self):
        # run streams for spot and future markets
        self.run_data_stream(quotes, 'publicTrade', 'linear')
        self.run_data_stream(quotes, 'publicTrade', 'spot')

        flag = False
        while True:
            time.sleep(1)
            difference = arbitrator.difference_calculation()

            if len(difference) > 10:
                difference_sorted = sorted(
                    difference.items(), key=lambda x: (abs(x[1]), x[1]), reverse=True
                )[:10] # get top 10
                quotes_for_orderbooks = [x[0] for x in difference_sorted]
                #print('___________', quotes_for_orderbooks)
                if not flag:
                    flag = True
                    for quotation in quotes_for_orderbooks:
                        print(quotation)
                        self.run_data_stream([quotation], 'orderbook.50', 'spot', once_flag=True)
                        self.run_data_stream([quotation], 'orderbook.50', 'linear', once_flag=True)
            
            #time.sleep(1)


if __name__ == '__main__':
    arbitrator = Arbitrator()
    quotes = arbitrator.get_quotes()
    print(len(quotes))

    arbitrator.arbitration_process()