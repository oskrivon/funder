from time import sleep
import pprint

from pybit import usdt_perpetual
from pybit import inverse_perpetual

session = usdt_perpetual.HTTP(
    endpoint="https://api.bybit.com"
)

ws_linear = usdt_perpetual.WebSocket(
    test=False,
    ping_interval=30,
    ping_timeout=10,
    domain="bybit"
)

quotation = []

result = session.query_symbol()['result']
quotes = [x['name'] for x in result if x['name'][-1] == 'T']
print(len(quotes))

fundings = {}

def handle_message(msg):
    fundings[msg['data']['symbol']] = msg['data']['funding_rate_e6']

    sorted_fundings = sorted(fundings.items(), key=lambda x: x[1])

    print(len(fundings))
    mins, maxs = sorted_fundings[:5], sorted_fundings[-5:]
    pprint.pprint(mins)
    pprint.pprint(maxs)

ws_linear.instrument_info_stream(
    handle_message, quotes
)

while True:
    sleep(1)