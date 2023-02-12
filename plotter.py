from matplotlib import pyplot as plt
import pandas as pd
import numpy as np

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