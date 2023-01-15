from pybit.usdt_perpetual import HTTP
import config as cfg

"""
from bybit_connector import BybitConnector
bybit = BybitConnector()
bybit.place_market_order('BTCUSDT', 0.001)  # Открываешь ордер
bybit.close_position('BTCUSDT', 0.001)  # Закрываешь ордер

Мы условились, что ты будешь сам до открытия позы определять размер позы (qty).
Тебе нужно запоминать это значение, потому что ты будешь использовать его и при закрытии позиции в т.ч..
Функции возвращают то же самое, что возвразащает байбит, мб тебе понадобится где-то.
И не забудь вставить свои API ключи.

Вырезанно, потому что пока не нужно, верну, когда сделаю TP и SL к следующему разу:

    qty=round(self.get_qty(symbol, margin, leverage), round_dec),  # todo
    
    def get_qty(self, symbol: str, margin: float, leverage: float) -> float:
        return margin / self.get_sybmol_last_price(symbol) * leverage


    def get_sybmol_last_price(self, symbol: str) -> float:
        return float(self.bybit.latest_information_for_symbol(symbol=symbol)['result'][0]['last_price'])    
"""


class BybitConnector:

    def __init__(self):
        self.bybit = HTTP(
            "https://api.bybit.com",
            api_key=cfg.API_KEY,
            api_secret=cfg.API_SECRET
        )

    def place_market_order(self, symbol: str, qty: float, side='Buy'):
        """Place market order on symbol"""
        return self.bybit.place_active_order(
            symbol=symbol,
            side=side,
            order_type="Market",
            qty=qty,
            time_in_force="GoodTillCancel",
            reduce_only=False,
            close_on_trigger=False,
            position_idx=0
        )

    def close_position(self, symbol: str, qty: float):
        """Close opened position"""
        return self.bybit.place_active_order(
            symbol=symbol,
            side='Sell',
            order_type="Market",
            reduce_only=True,
            qty=qty,
            close_on_trigger=True,
            time_in_force="GoodTillCancel",
            position_idx=0
        )
