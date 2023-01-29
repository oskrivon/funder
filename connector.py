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

    def get_active_order(self, symbol):
        return self.session_auth.get_active_order(
            symbol=symbol
        )

    def cancel_all_active_orders(self, symbol):
        return self.session_auth.cancel_all_active_orders(
            symbol=symbol
        )
    
    def cancel_active_orders(self, symbol, id):
        return self.session_auth.cancel_active_order(
            symbol=symbol,
            order_id=id
        )

    def get_positions(self, symbol):
        return self.session_auth.my_position(
            symbol=symbol
        )

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
    #RSS3USDT
    #pprint.pprint(connector.get_positions('RSS3USDT'))
    #pprint.pprint(connector.cancel_active_orders('JSTUSDT', 'c19f7eb6-0a6b-4c5e-bdf4-6890e0e111a3'))
    #pprint.pprint(connector.cancel_all_active_orders('JSTUSDT'))
    #print(connector.create_trade('BOBAUSDT', 'Buy', 20, False, False))
    #print(connector.create_trade('BOBAUSDT', 'Sell', 20, False, False))
    #print('_____')
    #print(connector.get_wallet_balance(quotation)['result'][quotation]['available_balance'])
    #print('_____')
    #print(connector.create_trade('DGBUSDT', 'Buy', 100, False, False))
    #print('_____')
    #print(connector.get_wallet_balance(quotation)['result'][quotation]['available_balance'])
    #pprint.pprint(connector.get_active_order('JSTUSDT'))
    pprint.pprint(connector.get_wallet_balance('USDT'))
    #pprint.pprint(connector.get_wallet_funds_records())
    #pprint.pprint(connector.get_fundings_history('JSTUSDT')['result'])
