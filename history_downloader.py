import datetime
import pandas as pd
from pybit import inverse_perpetual
from urllib.error import HTTPError
import wget
import os


# rework of the get_market_history.py scrypt form level analyzer
class HistoryDownloader:
    def __init__(self):
        pass

    
    # need delete !!!
    def get_market_history(self, quotations = [], period = 3):
        # get the now data
        now = datetime.datetime.now()
        print(now.year, now.month, now.day)

        # get all quotes from bybit using query_symbol() request
        if len(quotations) == 0:
            # get the quotations from bybit
            session_unauth = inverse_perpetual.HTTP(
                endpoint="https://api.bybit.com"
            )
            quotations_ = session_unauth.query_symbol()

            quotation_list = []

            for i in quotations_['result']:
                quotation_list.append(i['alias'])
        else:
            quotation_list = []

            for i in quotations:
                quotation_list.append(i)

        quotation_list.sort()

        # create the dates list
        now = datetime.datetime.now()

        dates = []
        for d in range(period, 0, -1):
            period_ = datetime.timedelta(days=d)
            data = (now - period_).strftime("%Y-%m-%d")
            dates.append(data)

        # create the urls list
        url_list = []

        for q in quotation_list:
            urls = []
            for d in dates:
                url = ('https://public.bybit.com/trading/' + q + '/' + 
                    q + d + '.csv.gz'
                    )
                urls.append(url)

            url_list.append(urls)

        # market data download
        for i in url_list:
            df = []
            for j in i:
                try:
                    df_ = pd.read_csv(j)
                except HTTPError:
                    print(HTTPError)
                    continue

                df.append(df_)

            try:
                df_total = pd.concat(df)
                df_total.reset_index(drop=True, inplace=True)

                s = i[0].split('/')
                file_name = s[4]

                df_total.to_csv('market_history/' + file_name + '.csv')
            except ValueError:
                    continue

    # get dates list from yesterday to a date that an sampling_depth before
    # format "%Y-%m-%d" (f. e. 2022-10-03)
    def dates_generate(self, sampling_depth: int) -> list:
        now = datetime.datetime.now()
        dates = []

        for d in range(sampling_depth, 0, -1):
            dates.append(
                (now - datetime.timedelta(days=d)).
                strftime("%Y-%m-%d")
            )

        return dates
    
    # generate the linf for download
    def link_generate(self, market_type, date, quotation):
        # link exaples: 
        # https://public.bybit.com/trading/10000NFTUSDT/10000NFTUSDT2022-01-30.csv.gz trading endpoint
        # https://public.bybit.com/spot/ALGOUSDT/ALGOUSDT_2023-01-01.csv.gz spot endpoint

        endpoint = 'https://public.bybit.com'
        #market_type = 'spot' # or 'trading'
        #quotation = 'ALGOUSDT'
        file_format = 'csv.gz'

        if market_type == 'spot':
            download_link = f'{endpoint}/{market_type}/{quotation}/{quotation}_{date}.{file_format}'
        else:
            download_link = f'{endpoint}/{market_type}/{quotation}/{quotation}{date}.{file_format}'
        return download_link
    
    # downoad once file
    def dowload_once(self, download_link, download_path):
        file_path = download_path + '/' + download_link.split('/')[-1]
        os.makedirs(download_path, exist_ok=True) # create folder if not exist
        if not os.path.exists(file_path): # file existing check
            wget.download(download_link, download_path)

    # dowload a list of the files
    def downloading(self, quotes, market_type, sampling_depth):
        for quotation in quotes:
            download_path = f'history/{market_type}/{quotation}'

            dates = self.dates_generate(sampling_depth)
            for date in dates:
                download_link = self.link_generate(market_type, date, quotation)
                self.dowload_once(download_link, download_path)

if __name__ == '__main__':
    quotation = ['FITFIUSDT', 'TRXUSDT']
    market_types = ['spot', 'trading']

    hd = HistoryDownloader()
    for market_type in market_types:
        hd.downloading(quotation, market_type, 2) # 'spot' or 'trading'

    #print(hd.date_generate(1))

