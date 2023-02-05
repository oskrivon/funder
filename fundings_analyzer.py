from time import sleep
import pprint
import time

from pybit import usdt_perpetual
from pybit import inverse_perpetual


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

    def get_quotes(self):
        result = self.session.query_symbol()['result']
        quotes = [x['name'] for x in result if x['name'][-1] == 'T']
        return quotes

    def fundings_request(self, msg):
        self.fundings[msg['data']['symbol']] = int(msg['data']['funding_rate_e6'])

    def get_fundings(self):
        quotes = self.get_quotes()
        self.socket.instrument_info_stream(
            self.fundings_request, quotes
        )

        while len(quotes) > len(self.fundings): pass

        return sorted(self.fundings.items(), key=lambda x: (abs(x[1]), x[1]))
        #return self.fundings

if __name__ == '__main__':
    funds_analyzer = FundingAnalyzer()
    funds = funds_analyzer.get_fundings()
    print(funds[-10:], len(funds))