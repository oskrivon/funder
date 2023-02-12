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

    def get_top_fundings_for_qoutation(self, quotation, sampling_depth):
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
        )[:sampling_depth]

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

    def profit_calculation(self, df_future, funding_date, funding_rate):
        # cut minute interval after funding
        minute_after = funding_date + datetime.timedelta(minutes=1)
        df_after_fund = df_future.loc[
            (df_future['date'] > funding_date) & 
            (df_future['date'] < minute_after)
        ]
        df_after_fund.reset_index(drop=True, inplace=True)
        after_volume = round(df_after_fund['size'].sum(), 2)

        # cut minute interval before funding
        minute_before = funding_date - datetime.timedelta(minutes=1)
        df_prev_fund = df_future.loc[
            (df_future['date'] > minute_before) & 
            (df_future['date'] < funding_date)
        ]
        df_prev_fund = df_future.loc[df_future['date'] < row[6]]
        df_prev_fund.reset_index(drop= True , inplace= True )

        buy_price = df_prev_fund.iloc[-1]['price']
        before_volume = round(df_prev_fund['size'].sum(), 2)

        ### go
        rounder = 2

        sell_price_nearest = df_after_fund.iloc[0]['price']
        date_price_nearest = df_after_fund.iloc[0]['date'].second
        sell_price_mean = df_after_fund['price'].mean()

        if funding_rate < 0:
            sell_price_best = df_after_fund['price'].min()
            df_best_price_dates = df_after_fund.loc[df_after_fund['price'] == sell_price_best]
            list_best_seconds = (df_best_price_dates.loc[:, 'date'].dt.second).to_list()

            sell_price_worst = df_after_fund['price'].max()
            df_worst_price_dates = df_after_fund.loc[df_after_fund['price'] == sell_price_worst]
            list_worst_seconds = (df_worst_price_dates.loc[:, 'date'].dt.second).to_list()

            profit_nearest = round((buy_price - sell_price_nearest) / buy_price * 100, rounder)
            profit_mean = round((buy_price - sell_price_mean) / buy_price * 100, rounder)
            profit_best = round((buy_price - sell_price_best) / buy_price * 100, rounder)
            profit_worst = round((buy_price - sell_price_worst) / buy_price * 100, rounder)
        else:
            sell_price_best = df_after_fund['price'].max()
            df_best_price_dates = df_after_fund.loc[df_after_fund['price'] == sell_price_best]
            list_best_seconds = (df_best_price_dates.loc[:, 'date'].dt.second).to_list()

            sell_price_worst = df_after_fund['price'].min()
            df_worst_price_dates = df_after_fund.loc[df_after_fund['price'] == sell_price_worst]
            list_worst_seconds = (df_worst_price_dates.loc[:, 'date'].dt.second).to_list()

            profit_nearest = round((sell_price_nearest - buy_price) / buy_price * 100, rounder)
            profit_mean = round((sell_price_mean - buy_price) / buy_price * 100, rounder)
            profit_best = round((sell_price_best - buy_price) / buy_price * 100, rounder)
            profit_worst = round((sell_price_worst - buy_price) / buy_price * 100, rounder)
        
        return (
            profit_nearest, profit_mean, profit_best, profit_worst,
            date_price_nearest, list_best_seconds, list_worst_seconds,
            after_volume, before_volume
        )

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

    funds_analzing_need_flag = True
    sampling_depth = 5

    if funds_analzing_need_flag:
        quotes = funds_analyzer.get_quotes()#[:10]
    else:
        quotes = ['BNBUSDT']

    print(len(quotes))

    report_data = {
        'quotations': [],
        'fund_rates': [],
        'p_nearest': [],
        'p_mean': [],
        'p_best': [],
        'p_worst': [],
        'count_future_trades': [],
        'after_volume': [],
        'before_volume': []
    }

    nearest_times_list, best_times_list, worst_times_list = [], [], []

    for quotation in quotes:
        if conn.check_spot_exist(quotation):
            try:
                funds = funds_analyzer.get_top_fundings_for_qoutation(quotation, sampling_depth)
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
                # row scructure: Index=0, symbol='1INCHUSDT', fundingRate=0.00052964, fundingRateTimestamp=1673683200000, datasets_spot=['1INCHUSDT_2023-01-14.csv.gz'], datasets_future=['1INCHUSDT2023-01-14.csv.gz'], date=Timestamp('2023-01-14 08:00:00')
                try:
                    df_spot = funds_analyzer.create_near_funding_dataframes(row[3], row[4])
                    df_future = funds_analyzer.create_near_funding_dataframes(row[3], row[5])

                    len_spot_df = len(df_spot)
                    len_fut_df = len(df_future)
                    #print('spot df len: ', len(df_spot))
                    #print('future df len: ', len(df_future))

                    (profit_nearest, profit_mean, profit_best, profit_worst,
                    date_price_nearest, list_best_seconds, list_worst_seconds,
                    after_volume, before_volume) = \
                        funds_analyzer.profit_calculation(df_future, row[6], row[2])

                    funding_rate = round(row[2] * 100, 2)
                    profit_nearest = round(profit_nearest - abs(funding_rate) - 0.12, 2)
                    profit_mean = round(profit_mean - abs(funding_rate) - 0.12, 2)
                    profit_best = round(profit_best - abs(funding_rate) - 0.12, 2)
                    profit_worst = round(profit_worst - abs(funding_rate) - 0.12, 2)

                    title = (
                        row[1] + ', funding rate: ' + str(round(row[2] * 100, 2)) + '\n' +
                        'profit nearest: ' + str(profit_nearest) + '\n' +
                        'profit mean: ' + str(profit_mean) + '\n' +
                        'profit best: ' + str(profit_best) + '\n' +
                        'profit worst: ' + str(profit_worst)
                    )
                    report_name = row[1] + ' ' + str(round(row[2] * 100, 2))

                    #print(row[2], title)

                    report_data['quotations'].append(row[1])
                    report_data['fund_rates'].append(row[2])
                    report_data['p_nearest'].append(profit_nearest)
                    report_data['p_mean'].append(profit_mean)
                    report_data['p_best'].append(profit_best)
                    report_data['p_worst'].append(profit_worst)
                    report_data['count_future_trades'].append(len_fut_df)
                    report_data['after_volume'].append(after_volume)
                    report_data['before_volume'].append(before_volume)

                    nearest_times_list.append(date_price_nearest)
                    best_times_list.extend(list_best_seconds)
                    worst_times_list.extend(list_worst_seconds)

                    plotter.several_graphs(df_spot, df_future, report_name, title)

                except Exception as e:
                    print(e)
            
    print(report_data)
    df = pd.DataFrame.from_dict(report_data).reset_index()
    df.to_csv ('report.csv', index=False)

    pd.DataFrame(nearest_times_list).to_csv('nearest_times.csv', index=False)
    pd.DataFrame(best_times_list).to_csv('best_times.csv', index=False)
    pd.DataFrame(worst_times_list).to_csv('worst_times.csv', index=False)
