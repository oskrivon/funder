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
        self.prices_linear = {}
        self.prices_spot = {}

        self.orderbook_linear = {}
        self.orderbook_spot = {}

    
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
            print('>>>> ', sample['topic'], ' ', sample['data']['b'], '\n___\n', sample['data']['a'])
    
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


if __name__ == '__main__':
    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)

    arbitrator = Arbitrator()
    quotes = arbitrator.get_quotes()
    print(len(quotes))

    #quotes = ['ETHUSDT']#, 'ETHUSDT', 'SOLUSDT']
    topics = 'publicTrade'
    
    market_type = 'linear'
    stream_linear = bybit_stream.Bybit_Stream(cfg['api'], cfg['secret'], market_type, topics, quotes, 0)
    q_linear = Queue()
    stream_linear.run(q_linear)

    arbitrator.run_update_thread(q_linear, market_type)


    #quotes = ['ETHUSDT', 'SOLUSDT']
    market_type = 'spot'
    stream_spot = bybit_stream.Bybit_Stream(cfg['api'], cfg['secret'], market_type, topics, quotes, 0)
    q_spot = Queue()
    stream_spot.run(q_spot)
    
    arbitrator.run_update_thread(q_spot, market_type)


    def get_orderbook(quotes):
        topics = 'orderbook.50'
        market_type = 'linear'
        stream_linear_ob = bybit_stream.Bybit_Stream(
            cfg['api'], cfg['secret'], 
            market_type, topics, quotes, 0,
            once=True
        )
        q_linear_ob = Queue()
        stream_linear_ob.run(q_linear_ob)
        arbitrator.run_update_thread(q_linear_ob, market_type)

    flag = False
    while True:
        print('>>>> linear: ', arbitrator.prices_linear)
        print('>>>> spot: ', arbitrator.prices_spot)

        difference = {}
        for key in arbitrator.prices_linear:
            if key in arbitrator.prices_spot:
                diff = round((arbitrator.prices_linear[key] - arbitrator.prices_spot[key]) / arbitrator.prices_linear[key] * 100, 4)
                if diff != 0:
                    difference[key] = diff
        
        
        if len(difference) > 10:
            difference_sorted = sorted(difference.items(), key=lambda x: (abs(x[1]), x[1]), reverse=True)[:2]
            if not flag:
                qqq = []
                qqq.append(difference_sorted[0][0])
                print('>>>> ', qqq)
                get_orderbook(qqq)
                flag = True

            pprint.pprint(difference_sorted)
            print(arbitrator.orderbook_linear)

        print('count linear: ', len(arbitrator.prices_linear))
        print('count spot: ', len(arbitrator.prices_spot))
        print('count non-zero diff: ', len(difference))

        #print('>>>> linear: ', prices_linear, len(prices_linear))
        #print('>>>> spot: ', prices_spot, len(prices_spot))
        time.sleep(1)