import pprint
import yaml
from pybit import usdt_perpetual


class Connector:
    def __init__(self, session) -> None:
        self.session_auth = session

        print('>>> connector init')

    
    def create_trade(self, symbol, side, qty, reduce_only, close_on_trigger):
        trade = self.session_auth.place_active_order(
            symbol=symbol,
            side=side,
            order_type="Market",
            qty=qty,
            time_in_force="GoodTillCancel",
            reduce_only=reduce_only,
            close_on_trigger=close_on_trigger,
            position_idx=0
        )

        return trade


    def get_fundings_history(self, symbol):
        fundings_history = self.session_auth.my_last_funding_fee(
            symbol = symbol
        )

        return fundings_history

    
    def get_wallet_balance(self, coin):
        balance = self.session_auth.get_wallet_balance(
            coin=coin
        )

        return balance

    
    def get_wallet_funds_records(self):
        records = self.session_auth.wallet_fund_records()
        return records

if __name__ == '__main__':
    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)
    
    session = usdt_perpetual.HTTP(
        endpoint=cfg['endpoint'],
        api_key=cfg['api'],
        api_secret=cfg['secret']
    )

    print(cfg)
    connector = Connector(session)
    quotation = 'USDT'
    #print(connector.create_trade('1000BTTUSDT', 'Buy', 1000, False, False))
    #print(connector.create_trade('CTCUSDT', 'Sell', 10, False, False))
    #print('_____')
    #print(connector.get_wallet_balance(quotation)['result'][quotation]['available_balance'])
    #print('_____')
    #print(connector.create_trade('DGBUSDT', 'Buy', 100, False, False))
    #print('_____')
    #print(connector.get_wallet_balance(quotation)['result'][quotation]['available_balance'])
    pprint.pprint(connector.get_wallet_balance('USDT'))
    #pprint.pprint(connector.get_wallet_funds_records())
    pprint.pprint(connector.get_fundings_history('CTCUSDT')['result'])
