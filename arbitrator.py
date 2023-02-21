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
        pass
    
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

if __name__ == '__main__':
    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)

    session = usdt_perpetual.HTTP(
        endpoint=cfg['endpoint'],
        api_key=cfg['api'],
        api_secret=cfg['secret']
    )

    prices_linear = {}
    prices_spot = {}

    orderbook_linear = {}
    orderbook_spot = {}

    def data_updater(q, market_type, topic):
        print(q, market_type, topic)
        def data_update():
            if 'orderbook' in topic:
                if market_type == 'linear':
                    global orderbook_linear
                    market = orderbook_linear
                else:
                    pass
            else:
                if market_type == 'linear':
                    global prices_linear
                    market = prices_linear
                else:
                    global prices_spot
                    market = prices_spot

            while True:
                data = q.get()

                if 'orderbook' in topic:
                    if data['type'] == 'snapshot':
                        print('>>>> ', topic, ' ', data)
                else:
                    price = float(data['data'][0]['p'])
                    symbol = data['topic'].replace(topics + '.', '')

                    market[symbol] = price
                #print('>>>> ', market_type)
                #print(market)

        # thread for updating data from socket
        wallet_check_th = threading.Thread(
            target=data_update
        )
        wallet_check_th.daemon = True
        wallet_check_th.start()

    arbitrator = Arbitrator()
    quotes = arbitrator.get_quotes()
    print(len(quotes))

    #quotes = ['ETHUSDT']#, 'ETHUSDT', 'SOLUSDT']
    topics = 'publicTrade'
    
    market_type = 'linear'
    stream_linear = bybit_stream.Bybit_Stream(cfg['api'], cfg['secret'], market_type, topics, quotes, 0)
    q_linear = Queue()
    stream_linear.run(q_linear)
    data_updater(q_linear, market_type, topics)

    market_type = 'spot'
    stream_spot = bybit_stream.Bybit_Stream(cfg['api'], cfg['secret'], market_type, topics, quotes, 0)
    q_spot = Queue()
    stream_spot.run(q_spot)
    data_updater(q_linear, market_type, topics)

    def get_orderbook(quotes):
        topics = 'orderbook.500'
        market_type = 'linear'
        stream_linear_ob = bybit_stream.Bybit_Stream(cfg['api'], cfg['secret'], market_type, topics, quotes, 0)
        q_linear_ob = Queue()
        stream_linear_ob.run(q_linear_ob)
        data_updater(q_linear_ob, market_type, topics)

    flag = False
    while True:
        difference = {}
        for key in prices_linear:
            if key in prices_spot:
                diff = round((prices_linear[key] - prices_spot[key]) / prices_linear[key] * 100, 4)
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
            print(orderbook_linear)

        print('count linear: ', len(prices_linear))
        print('count spot: ', len(prices_spot))
        print('count non-zero diff: ', len(difference))

        #print('>>>> linear: ', prices_linear, len(prices_linear))
        #print('>>>> spot: ', prices_spot, len(prices_spot))
        time.sleep(1)