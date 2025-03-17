import os
import json
from datetime import datetime, timedelta
import pandas as pd
from crypto_stock import CryptoStock
from binance_client import bclient
from config_atf import ConfigATF
from alt_index import AltIndex


class Portfolio:
    STATE_FILE = "data/portfolio_state.json"
    PRECISION_THRESHOLD = 20

    def __init__(self, name, key_name, alt_index):
        self.name = name
        self.stocks = {}
        self.alt_index = alt_index
        self.config = ConfigATF()
        bclient.set_key(key_name)
        self.cash_reserve = 0
        self.state = {}
        self.fetch_balances()
        self._load_state()

    def fetch_balances(self):
        bin_balances = bclient.client.get_account()['balances']
        self.stocks.clear()
        self.cash_reserve = 0

        for b in bin_balances:
            asset = b['asset']
            qty = float(b['free']) + float(b['locked'])

            if qty == 0:
                continue

            if CryptoStock.is_tradable(asset) == False:
                continue

            if asset in {"USDT", "BUSD"}:
                self.cash_reserve += qty
            else:
                stock = CryptoStock(asset, qty)
                if stock.is_valid:
                    self.stocks[asset] = stock

    def discard_open_orders(self):
        orders = bclient.client.get_open_orders()
        for order in orders:
            symbol = order['symbol']
            client_order_id = order['clientOrderId']
            bclient.client.cancel_order(symbol=symbol, origClientOrderId=client_order_id)
        self.fetch_balances()

    def get_value(self, share=1.0):
        return sum(stock.get_value() for stock in self.stocks.values()) + share * self.cash_reserve

    def get_realtime_index_price(self):
        """
        Computes real-time alt index price from Binance's current coin prices.
        This provides an immediate price update as opposed to the daily historical prices from CoinGecko.
        """
        price = 0
        for asset, data in self.alt_index.get_current_top_10_coins().items():
            weight = data['weight']
            asset_price = data['price']
            market_cap = data['market_cap']

            stock = self.stocks.get(asset, CryptoStock(asset, 0))
            current_price = stock.get_coin_price()
            current_market_cap = market_cap/asset_price * current_price
            price += current_market_cap/self.alt_index.initial_total_market_cap()
        return price

    def _load_state(self):
        if os.path.exists(self.STATE_FILE):
            with open(self.STATE_FILE, 'r') as f:
                self.state = json.load(f)
        else:
            self.state = {'operation': None, 'goal_balance': None}

    def _save_state(self):
        with open(self.STATE_FILE, 'w') as f:
            json.dump(self.state, f)

    def clear_state(self):
        self.state = {'operation': None, 'goal_balance': None}
        self._save_state()

    def is_buy_goal_balance_reached(self):
        current_balance = self.get_value(0)
        goal_balance = self.state.get('goal_balance')
        return goal_balance - current_balance < self.PRECISION_THRESHOLD

    def is_sell_goal_balance_reached(self):
        current_balance = self.get_value(0)
        goal_balance = self.state.get('goal_balance')
        return current_balance - goal_balance < self.PRECISION_THRESHOLD

    def _execute_buy(self):
        goal_balance = self.state['goal_balance']

        for asset, data in self.alt_index.get_current_top_10_coins().items():
            weight = data["weight"]
            target_value = goal_balance * weight
            stock = self.stocks.get(asset, CryptoStock(asset))
            current_value = stock.get_value()

            delta = target_value - current_value
            if delta > self.PRECISION_THRESHOLD:
                available_cash = self.cash_reserve
                amount_to_buy = min(delta, available_cash)
                if amount_to_buy < self.PRECISION_THRESHOLD:
                    continue
                stock.buy(amount_to_buy)
                self.stocks[asset] = stock

    def execute_buy(self, share):
        self.discard_open_orders()
        self.state['operation'] = 'buy'

        if not self.state.get('goal_balance'):
            self.state['goal_balance'] = self.get_value(share)

        if not self.is_buy_goal_balance_reached():
            self._execute_buy()

        if self.is_buy_goal_balance_reached():
            self.state.update({'operation': None, 'goal_balance': None})

        self._save_state()

    def _execute_sell(self):
        goal_balance = self.state['goal_balance']

        for asset, stock in list(self.stocks.items()):
            data = self.alt_index.get_current_top_10_coins().get(asset)
            weight = data["weight"] if data else 0
            target_value = goal_balance * weight
            current_value = stock.get_value()

            delta = current_value - target_value
            if delta > self.PRECISION_THRESHOLD:
                stock.sell(delta / current_value)

            if weight == 0:
                del self.stocks[asset]

    def execute_sell(self, share, operation):
        self.discard_open_orders()
        self.state['operation'] = operation

        if not self.state.get('goal_balance'):
            goal = self.get_value(0) - share*self.get_value(0)
            if goal < self.PRECISION_THRESHOLD:
                goal = 0
            self.state['goal_balance'] = goal

        if not self.is_sell_goal_balance_reached():
            self._execute_sell()

        if self.is_sell_goal_balance_reached():
            self.state.update({'operation': None, 'goal_balance': None})

        self._save_state()

    def get_sl_signal(self, cfg, rsi):
        """
        Compute stop loss signal with is the current price divided by the
        maximum index price we got over the previous X days defined in
        cfg.win_sl
        """
        if rsi < cfg.rsi_min_thresh:
            return False

        # Load historical max prices if not already loaded
        max_prices_file = "data/daily_max_prices.json"
        if not hasattr(self, '_daily_max_prices'):
            if os.path.exists(max_prices_file):
                with open(max_prices_file, 'r') as f:
                    self._daily_max_prices = json.load(f)
            else:
                self._daily_max_prices = {}

        today = datetime.utcnow().date().isoformat()
        realtime_price = self.get_realtime_index_price()

        # Update today's max price
        previous_max = self._daily_max_prices.get(today, 0)
        if realtime_price > previous_max:
            self._daily_max_prices[today] = realtime_price
            with open(max_prices_file, 'w') as f:
                json.dump(self._daily_max_prices, f)

        # Compute maximum over the sliding window of win_sl days
        recent_dates = [
            (datetime.utcnow().date() - timedelta(days=i)).isoformat()
            for i in range(cfg.win_sl)
        ]
        recent_prices_from_history = self.alt_index.ticker[-cfg.win_sl:]['close'].to_dict()

        v_max = max(
            max(self._daily_max_prices.get(date, 0), recent_prices_from_history.get(pd.Timestamp(date), 0))
            for date in recent_dates
        )

        if v_max == 0:
            return False

        drop_ratio = realtime_price / v_max
        print(f"drop ratio={drop_ratio:.4f}")
        return drop_ratio < cfg.tresh

    def print(self):
        stock_details = ", ".join(f"{a}:{s.quantity:.03f}" for a, s in self.stocks.items())
        print(f"Portfolio({self.cash_reserve}, {self.get_value()}): [{stock_details}]")


if __name__ == "__main__":
    alt_index = AltIndex()
    portfolio = Portfolio("Binance", "binance_crypto_l3", alt_index)
    portfolio.fetch_balances()
    portfolio.print()
