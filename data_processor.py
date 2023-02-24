import pandas as pd
import os
import matplotlib.pyplot as plt


class DataPreprocessing:
    def __init__(self):
        pass


    def df_create(self, files):
        df_list = []
        for file in files:
            df_list.append(pd.read_csv(file))
        
        return pd.concat(df_list, ignore_index=True)

    def df_group(self, df, frequency = '1min', round = 5):
        if 'grossValue' in df:
            df['timestamp'] = pd.to_datetime(
                df['timestamp'], unit='s'
            )

            grouped_price = \
            df.groupby([
                pd.Grouper(key='timestamp', freq=frequency)
            ]).agg(
                mean =('price', 'mean'),
                volume = ('size', 'sum'), 
            ).round(round)
        else:
            df['timestamp'] = pd.to_datetime(
                df['timestamp'], unit='ms'
            )
        
            grouped_price = \
            df.groupby([
                pd.Grouper(key='timestamp', freq=frequency)
            ]).agg(
                mean =('price', 'mean'),
                volume = ('volume', 'sum'), 
            ).round(round)

        # clearing nan-values
        grouped_price = grouped_price.fillna(method="ffill")
        grouped_price = grouped_price.fillna(method="bfill")
        return grouped_price


if __name__ == '__main__':
    processor = DataPreprocessing()

    path_spot = 'history/spot'
    path_trading = 'history/trading'
    path_report = 'reports/'
    quotation = 'ACHUSDT'

    quotes = os.listdir(path_spot)

    def get_grouped_df(quotation, data_storage_path, frequency = '1min'):
        full_path_to_quotation = f'{data_storage_path}/{quotation}'
        quotes_list = os.listdir(full_path_to_quotation)
        files_list = [f'{full_path_to_quotation}/{el}' for el in quotes_list]
        df = processor.df_create(files_list)

        return processor.df_group(df, frequency)

    max_diff_list = []
    for quotation in quotes:
        print(quotation)
        df_group_spot = get_grouped_df(quotation, path_spot)
        df_group_trading = get_grouped_df(quotation, path_trading)

        df_group_spot.reset_index(inplace=True)
        df_group_trading.reset_index(inplace=True)

        df_result = pd.DataFrame({
            'timestamp': df_group_spot['timestamp'],
            'spot mean':  df_group_spot['mean'],
            'trading mean':  df_group_trading['mean'],
        })

        df_result['difference'] = abs(
            (df_result['spot mean'] - df_result['trading mean']) / df_result['spot mean'] * 100
        )

        max_ = df_result['difference'].max()
        id_max = df_result['difference'].idxmax()
        max_bit = df_result.loc[[id_max]]

        max_bit['quotation'] = quotation

        max_diff_list.append(max_bit)

        #df_group_spot['mean'].plot()
        #df_group_trading['mean'].plot()
        #plt.show()
    
    print(max_diff_list)

    max_diff_df = pd.concat(max_diff_list, ignore_index=True)
    print(max_diff_df)

    max_diff_df.to_csv('reports/max_diffs.csv')
