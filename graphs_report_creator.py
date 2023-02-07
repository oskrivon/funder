import os
import pandas as pd
import mplfinance as mpf
from matplotlib import pyplot as plt
import numpy as np

def title_preparation(raw_title):
    title = ''

    if 'trade.' in raw_title:
        title = raw_title.replace('trade.', '')
    
    if '.csv' in raw_title:
        title = title.replace('.csv', '')

    meta_data = title.split()

    quotation = meta_data[0]
    funding_rate = int(meta_data[1])
    
    return quotation, funding_rate

def trades_graph(file_name, title, report_name):
    df = pd.read_csv('trade_logs/' + file_name, index_col=False)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', origin='unix')

    x = np.array(df['timestamp'])
    y = np.array(df['price'])
    z = np.array(df['size'])

    markers = {
        'Sell': 'v',
        'Buy': '^'
    }

    colors = {
        'Sell': 'red',
        'Buy': 'green'
    }

    df.plot(
        x = 'timestamp', y = 'price', figsize=(16, 10), 
        grid=True, 
        title=title
    )

    if 'side' in df:
        m = np.array(df['side'])

        for xp, yp, m in zip(x, y, m):
            plt.scatter(xp, yp, marker=markers[m], c=colors[m])
    else:
        for xp, yp in zip(x, y):
            plt.scatter(xp, yp)

    plt.savefig('trade_logs/reports/' + report_name + ' TR.png')

def candle_graph(df, title, report_name, frequency = '5S', round = 5):
    grouped_price = df.groupby([pd.Grouper(
    key='timestamp', freq=frequency)]).agg(
        Open = ('price', 'first'),
        High = ('price', 'max'),
        Low = ('price', 'min'),
        Close = ('price', 'last'),
        Volume = ('size', 'sum'), ).round(round)
    
    #grouped_price = grouped_price.fillna(method="ffill")
    #grouped_price = grouped_price.fillna(method="bfill")

    df_ = grouped_price.reset_index().set_index('timestamp')


    mpf.plot(
        df_, type='candle', volume=True, 
        figsize=(16, 10),
        style='yahoo', 
        savefig='trade_logs/reports/' + report_name + ' C.png',
        title=dict(title = title, y = 0.9, x = 0.2, ha = 'left')
    )

content = os.listdir('trade_logs')

for dataset in content:
    if '.csv' in dataset:
        quotation, funding_rate = title_preparation(dataset)
        title ='quotation: ' + quotation + '\n' + 'funding rate: ' + str(funding_rate)
        report_name = quotation + ' ' + str(funding_rate)

        df = pd.read_csv('trade_logs/' + dataset, index_col=False)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', origin='unix')

        trades_graph(dataset, title, report_name)
        candle_graph(df, title, report_name)
    else: pass

print('reports done!')
