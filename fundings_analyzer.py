import pandas as pd
import numpy as np
import os
import yaml
from time import sleep
import datetime
import pprint
import time
import wget

from pybit import usdt_perpetual
from pybit import inverse_perpetual

import connector
import plotter as plt


class FundingAnalyzer:
    def __init__(self):
        self.session = usdt_perpetual.HTTP(
            endpoint="https://api.bybit.com"
        )

        self.socket = usdt_perpetual.WebSocket(
            test=False,
            ping_interval=30,
            ping_timeout=10,
            domain="bybit"
        )

        self.fundings = {}

        self.connector = connector.Connector(self.session)

    def get_quotes(self):
        result = self.session.query_symbol()['result']
        quotes = [x['name'] for x in result if x['name'][-1] == 'T']
        return quotes

    def fundings_request(self, msg):
        self.fundings[msg['data']['symbol']] = int(msg['data']['funding_rate_e6'])

    def get_current_fundings(self):
        quotes = self.get_quotes()
        self.socket.instrument_info_stream(
            self.fundings_request, quotes
        )

        while len(quotes) > len(self.fundings): pass

        return sorted(self.fundings.items(), key=lambda x: (abs(x[1]), x[1]))
        #return self.fundings

    def load_one_day_history(self, quotation, market_type, dataset_name):
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
    
    def load_full_dataset(self, quotation, fundings, market_type):
        delimeter = {
            'spot': '_',
            'trading': ''
        }
        midnight = datetime.time(hour=0, minute=0, second=0)
        datasets = []

        for date in fundings['fundingRateTimestamp']:
            funding_date = datetime.datetime.utcfromtimestamp(date/1000)
            datasets_list = []

            if self.connector.check_spot_exist(quotation):
                if funding_date.time() == midnight:
                    dataset_name = (
                        quotation + delimeter[market_type] + 
                        str((funding_date - datetime.timedelta(days=1)).date()) + 
                        '.csv.gz'
                    )
                    datasets_list.append(dataset_name)
                
                dataset_name = (
                    quotation + delimeter[market_type] + 
                    str(funding_date.date()) + 
                    '.csv.gz'
                )
                datasets_list.append(dataset_name)

            content = os.listdir('history')
            for ds in datasets_list:
                if ds not in content:
                    self.load_one_day_history(quotation, market_type, ds)

            datasets.append(datasets_list)
        return datasets

    def get_top_fundings_for_qoutation(self, quotation):
        fundings = self.connector.get_funding_rate(quotation)['result']['list']
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
        )[:5]

        fundings_df.reset_index(drop=True, inplace=True)

        return fundings_df

    def create_near_funding_dataframes(self, funding_date, datasets):
        dfs = []
        for dataset in datasets:
            dfs.append(pd.read_csv(
                'history/' + dataset, compression='gzip', 
                header=0, sep=',', quotechar='"', on_bad_lines='skip'
            ))
        
        df = pd.concat(dfs, ignore_index=True)

        if 'grossValue' in df:
            df['date'] = pd.to_datetime(
                df['timestamp'], unit='s'
            )
        else:
            df['date'] = pd.to_datetime(
                df['timestamp'], unit='ms'
            )
        
        funding_date = (
            datetime.datetime.
            utcfromtimestamp(funding_date/1000)
        )

        date_begin = funding_date - datetime.timedelta(minutes=5)
        date_end = funding_date + datetime.timedelta(minutes=5)
        
        return df.loc[(df['date'] > date_begin) & (df['date'] < date_end)]

if __name__ == '__main__':
    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)
    
    session = usdt_perpetual.HTTP(
        endpoint=cfg['endpoint'],
        api_key=cfg['api'],
        api_secret=cfg['secret']
    )

    conn = connector.Connector(session)

    funds_analyzer = FundingAnalyzer()
    folder_path = 'history/reports/'
    plotter = plt.Plotter(folder_path)

    quotes = funds_analyzer.get_quotes()
    print(quotes)

    for quotation in quotes:
        if conn.check_spot_exist(quotation):
            try:
                funds = funds_analyzer.get_top_fundings_for_qoutation(quotation)
                datasets_spot = \
                    funds_analyzer.load_full_dataset(quotation, funds, 'spot')
                datasets_future = \
                    funds_analyzer.load_full_dataset(quotation, funds, 'trading')

                funds['datasets_spot'] = datasets_spot
                funds['datasets_future'] = datasets_future

                funds['date'] = pd.to_datetime(
                    funds['fundingRateTimestamp'], unit='ms'
                )
            except Exception as e:
                print(e)

            print(funds)

            for row in funds.itertuples():
                try:
                    df_spot = funds_analyzer.create_near_funding_dataframes(row[3], row[4])
                    #print(row[3], row[4])
                    #print(df_spot)
                    df_future = funds_analyzer.create_near_funding_dataframes(row[3], row[5])
                    #print(row[3], row[5])
                    #print(df_spot)

                    title = row[1] + ' ' + str(row[2])
                    report_name = row[1] + ' ' + str(row[2])

                    #plotter.trades_graph(df_spot, df_future, '', title, report_name)
                    plotter.several_graphs(df_spot, df_future, report_name, title)
                except Exception as e:
                    print(e)
            #print(len(funds_analyzer.create_near_funding_dataframes(funds)))