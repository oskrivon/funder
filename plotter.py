from matplotlib import pyplot as plt
import pandas as pd
import numpy as np

df = pd.read_csv('trade_logs/trade_log.csv', index_col=False)
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', origin='unix')
#df = df.reset_index(drop=True, inplace=True)
print(df)

df.plot(x = 'timestamp', y = 'price')
plt.savefig('trade_logs/saved_figure.png')

colors = np.random.rand(40)
x = np.array(df['timestamp'])
y = np.array(df['price'])
z = np.array(df['size'])
print(x)
plt.scatter(x = x, y = y, s=z*1, c='red')
plt.show()