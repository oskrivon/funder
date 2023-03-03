from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import datetime

class Plotter:
    def __init__(self, folder_path):
        self.folder_path = folder_path

        self.markers = {
            'sell': 'v',
            'buy': '^'
        }

        self.colors = {
            'sell': 'red',
            'buy': 'green'
        }


    def open_df(self):
        df = pd.read_csv('trade_logs/trade.FXSUSDT 428.csv', index_col=False)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', origin='unix')
        #df = df.reset_index(drop=True, inplace=True)
        print(df)

    def several_graphs(self, df_spot, df_future, report_name, title):
        #print(df_spot)
        #print(df_future)

        markers_flag = False

        x = np.array(df_spot['date'])
        x2 = np.array(df_future['date'])
        y = np.array(df_spot['price'])
        y2 = np.array(df_future['price'])

        plt.figure(
            figsize=(16, 10), 
            #grid=True, 
            #title=title
        )
        plt.grid(True)
        plt.title(title, loc = 'left')

        plt.plot(x, y, label='spot')
        plt.plot(x2, y2, label='future')
        plt.legend()

        if markers_flag:
            m = np.array(df_spot['side'].str.lower())
            for xp, yp, m in zip(x, y, m):
                plt.scatter(xp, yp, s=10, marker=self.markers[m], c=self.colors[m])
            
            m = np.array(df_future['side'].str.lower())
            for xp, yp, m in zip(x2, y2, m):
                plt.scatter(xp, yp, s=10, marker=self.markers[m], c=self.colors[m])

        plt.savefig(self.folder_path + report_name + ' TR.png')

        plt.close('all')
        plt.clf()
        plt.cla()

    def trades_graph(self, df_spot, df_future, file_name, title, report_name):
        x = np.array(df_spot['date'])
        y = np.array(df_spot['price'])
        z = np.array(df_spot['volume'])

        df_spot.plot(
            x = 'date', y = 'price', figsize=(16, 10), 
            grid=True, 
            title=title
        )

        if 'side' in df_spot:
            m = np.array(df_spot['side'])

            for xp, yp, m in zip(x, y, m):
                plt.scatter(xp, yp, marker=self.markers[m], c=self.colors[m])
        else:
            for xp, yp in zip(x, y):
                plt.scatter(xp, yp)

        
        x = np.array(df_future['date'])
        y = np.array(df_future['price'])
        z = np.array(df_future['size'])

        df_future.plot(
            x = 'date', y = 'price', figsize=(16, 10), 
            grid=True, 
            title=title
        )

        if 'side' in df_future:
            m = np.array(df_future['side'])

            for xp, yp, m in zip(x, y, m):
                plt.scatter(xp, yp, marker=self.markers[m], c=self.colors[m])
        else:
            for xp, yp in zip(x, y):
                plt.scatter(xp, yp)
        
        #plt.show()

        plt.savefig(self.folder_path + report_name + ' TR.png')
    
    # report for Depth of Market
    # creates a bar chart with DOM, writes in the legend 
    # the current and the maximum price difference for the month 
    # on the futures and spot markets
    # takes df of the form:
    # | price | volume | side |
    # |-------|--------|------|
    # |  0.875|    57.5|   bid|  (or ask)

    def create_DOM_report(self, df, symbol, market_type, max_historical_diff, diff):
        df = df.sort_values(by='price')
        #df = df.set_index('price')

        ax = df.plot(
            x='price',
            y = ['volume_spot', 'volume_linear'],
            kind='barh', 
            color = {
                'volume_spot': 
                    df['side_spot'].replace({
                        'ask': 'green', 'bid': 'red', np.nan: 'black'
                    }),
                'volume_linear': 
                    df['side_linear'].replace({
                        'ask': 'lightgreen', 'bid': 'lightcoral', np.nan: 'black'
                    })
            }, 
            figsize=(7, 15), 
            width=1,
            fontsize=5,
            grid=True,
            legend=False,
        )
        ax.text(
            0.5, 1, 
            f'quotation: {symbol}\nmax historical difference: {round(max_historical_diff, 4)}\ncurrent difference: {diff}', 
            fontsize=12, transform=ax.transAxes
        )

        now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
        file_name = f'{symbol} {market_type} {now}'

        plt.savefig(f'{self.folder_path}/{file_name}.png', dpi = 600)
        plt.clf()
        #plt.show()