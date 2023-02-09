import requests
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import os
import datetime
import json
import pprint
import yaml
import wget
from pybit import usdt_perpetual


class Connector:
    def __init__(self, session) -> None:
        self.url = 'https://api.bybit.com/'
        self.session_auth = session

        print('>>> connector init')

    
    def create_trade(self, symbol, side, qty, reduce_only, close_on_trigger):
        trade = self.session_auth.place_active_order(
            symbol=symbol,
            side=side,
            order_type="Market",
            qty=qty,
            time_in_force="GoodTillCancel",
            reduce_only=reduce_only,
            close_on_trigger=close_on_trigger,
            position_idx=0
        )

        return trade


    def get_fundings_history(self, symbol):
        fundings_history = self.session_auth.my_last_funding_fee(
            symbol = symbol
        )

        return fundings_history

    
    def get_wallet_balance(self, coin):
        balance = self.session_auth.get_wallet_balance(
            coin=coin
        )

        return balance

    
    def get_wallet_funds_records(self):
        records = self.session_auth.wallet_fund_records()
        return records

    def get_active_order(self, symbol):
        return self.session_auth.get_active_order(
            symbol=symbol
        )

    def cancel_all_active_orders(self, symbol):
        return self.session_auth.cancel_all_active_orders(
            symbol=symbol
        )
    
    def cancel_active_orders(self, symbol, id):
        return self.session_auth.cancel_active_order(
            symbol=symbol,
            order_id=id
        )

    def get_positions(self, symbol):
        return self.session_auth.my_position(
            symbol=symbol
        )

    def get_public_trading_history(self, category):
        access_point = '/v5/market/recent-trade'

        params = dict(
            category = category,
            symbol = 'ETHUSDT',
            limit = 100
        )

        res = requests.get(self.url + access_point, params=params)
        return json.loads(res.text)

    def get_funding_rate(self, quotation, limit=200):
        access_point = 'v5/market/funding/history'

        params = dict(
            category = 'linear',
            symbol = quotation,
            limit = limit
        )
        
        res = requests.get(self.url + access_point, params=params)
        return json.loads(res.text)

    def check_spot_exist(self, symbol):
        access_point = '/v5/market/instruments-info'
        params = dict(
            category = 'spot',
            symbol = symbol
        )

        res = requests.get(self.url + access_point, params=params)
        res_json = json.loads(res.text)

        return len(res_json['result']['list'])

if __name__ == '__main__':
    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)
    
    session = usdt_perpetual.HTTP(
        endpoint=cfg['endpoint'],
        api_key=cfg['api'],
        api_secret=cfg['secret']
    )

    print(cfg)
    connector = Connector(session)
    quotation = 'USDT'
    #RSS3USDT
    #pprint.pprint(connector.get_positions('RSS3USDT'))
    #pprint.pprint(connector.cancel_active_orders('JSTUSDT', 'c19f7eb6-0a6b-4c5e-bdf4-6890e0e111a3'))
    #pprint.pprint(connector.cancel_all_active_orders('JSTUSDT'))
    #print(connector.create_trade('BOBAUSDT', 'Buy', 20, False, False))
    #print(connector.create_trade('BOBAUSDT', 'Sell', 20, False, False))
    #print('_____')
    #print(connector.get_wallet_balance(quotation)['result'][quotation]['available_balance'])
    #print('_____')
    #print(connector.create_trade('DGBUSDT', 'Buy', 100, False, False))
    #print('_____')
    #print(connector.get_wallet_balance(quotation)['result'][quotation]['available_balance'])
    #pprint.pprint(connector.get_active_order('JSTUSDT'))
    #pprint.pprint(connector.get_wallet_balance('USDT'))
    #pprint.pprint(connector.get_wallet_funds_records())
    #pprint.pprint(connector.get_fundings_history('JSTUSDT')['result'])

    def download_history(quotation, market_type, dataset_name):
        # https://public.bybit.com/spot/ETHUSDT/ETHUSDT_2023-02-07.csv.gz
        # https://public.bybit.com/trading/ETHUSDT/ETHUSDT2023-02-07.csv.gz
        sourse = 'https://public.bybit.com/'
        market_type = market_type
        quotation = quotation
        #date = str(date)
        #file_extension = '.csv.gz'

        full_path = (
            sourse + market_type + '/' + quotation + '/' +
            dataset_name
        )
        print(full_path)

        wget.download(full_path, 'history/')

    quotation = 'LDOUSDT'

    fundings = connector.get_funding_rate(quotation)['result']['list']

    #sorted(fundings, key=lambda x: abs(float(x['fundingRate'])))

    fundings_df = pd.DataFrame(fundings)
    fundings_df = fundings_df.astype(
        {
            'symbol': str,
            'fundingRate': float, 
            'fundingRateTimestamp': np.int64
        }
    )

    fundings_df = fundings_df.sort_values(
        by='fundingRate',
        ascending=False,
        key=lambda x: abs(x)
    )[:20]

    #fundings_df.reset_index(drop=True, inplace=True)

    print(fundings_df)

    for date in fundings_df['fundingRateTimestamp']:
        time_ = datetime.datetime.utcfromtimestamp(date/1000)
        print(time_)
        midnight = datetime.time(hour=0, minute=0, second=0)

        datasets_list = []

        if connector.check_spot_exist(quotation):
            ### dowloading
            if time_.time() == midnight:
                dataset_name = quotation + '_' + str((time_ - datetime.timedelta(days=1)).date()) + '.csv.gz'
                datasets_list.append(dataset_name)
                print(dataset_name)

            dataset_name = quotation + '_' + str(time_.date()) + '.csv.gz'
            datasets_list.append(dataset_name)

            content = os.listdir('history')

            for ds in datasets_list:
                if ds not in content:
                    print('>>>> exist!!!')
                    download_history(quotation, 'spot', ds)

            ### downloading end

            ### open datasets
            dfs = []
            for dataset in datasets_list:
                dfs.append(pd.read_csv(
                    'history/' + dataset, compression='gzip', 
                    header=0, sep=',', quotechar='"', on_bad_lines='skip'
                ))


            df = pd.concat(dfs, ignore_index=True)

            df['date'] = pd.to_datetime(
                df['timestamp'], unit='ms'
            )

            date_begin = time_ - datetime.timedelta(minutes=5)
            date_end = time_ + datetime.timedelta(minutes=5)

            print(date_begin, date_end)

            df_ = df.loc[(df['date'] > date_begin) & (df['date'] < date_end)]
            print(df_)
            ### open datasets end

            df_.plot(
                x = 'date', y = 'price', figsize=(16, 10), 
                grid=True, 
                title='title'
            )

            plt.savefig('report' + ' TR.png')

        else:
            print('out')