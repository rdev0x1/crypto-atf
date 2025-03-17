import sys
import time
import json
import os
from datetime import datetime, timedelta
from config_atf import ConfigATF
from binance_client import bclient
from alt_index import AltIndex
from portfolio import Portfolio


class BotATF:
    STATE_FILE = "data/bot_state.json"

    def __init__(self, name, key_name):
        self.config = ConfigATF()
        self.alt = AltIndex()
        self.portfolio = Portfolio(name, key_name, self.alt)

        self.BD_in_cool = 0
        self.SL_in_cool = 0
        self.TP_in_cool = 0
        self.last_buy_price = sys.float_info.max
        self.last_update_date = datetime.utcnow().date()

    def _load_state(self):
        if os.path.exists(self.STATE_FILE):
            with open(self.STATE_FILE, 'r') as f:
                state = json.load(f)
        else:
            state = {}

        self.last_buy_price = state.get('last_buy_price', sys.float_info.max)
        self.BD_in_cool = state.get('BD_in_cool', 0)
        self.SL_in_cool = state.get('SL_in_cool', 0)
        self.TP_in_cool = state.get('TP_in_cool', 0)

        last_date_str = state.get('last_update_date')
        if last_date_str:
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            days_passed = (datetime.utcnow().date() - last_date).days
            self.BD_in_cool = max(0, self.BD_in_cool - days_passed)
            self.SL_in_cool = max(0, self.SL_in_cool - days_passed)
            self.TP_in_cool = max(0, self.TP_in_cool - days_passed)

            self.last_update_date = datetime.utcnow().date()
            self._save_bot_state()  # save after loading adjustments

    def _save_bot_state(self):
        state = {
            'last_buy_price': self.last_buy_price,
            'BD_in_cool': self.BD_in_cool,
            'SL_in_cool': self.SL_in_cool,
            'TP_in_cool': self.TP_in_cool,
            'last_update_date': datetime.utcnow().date().isoformat()
        }
        with open(self.STATE_FILE, 'w') as f:
            json.dump(state, f)

    def _save_all_states(self):
        self._save_bot_state()
        self.portfolio._save_state()

    def _should_buy(self, cfg, arsi):
        return (arsi <= cfg.buy_dip.rsi_max_thresh and
                self.SL_in_cool == 0 and
                self.BD_in_cool == 0 and
                self.TP_in_cool == 0)

    def _should_sell_tp(self, cfg, p, arsi):
        return (arsi > cfg.sell_tp.rsi_min_thresh and
                self.TP_in_cool == 0 and
                self.SL_in_cool == 0)

    def _should_sell_sl(self, cfg, p, arsi):
        return (p.get_sl_signal(cfg.sell_sl, arsi) and
                self.BD_in_cool == 0 and
                self.SL_in_cool == 0)

    def _resume_operation(self, cfg, p, arsi):
        operation = p.state.get('operation')

        if operation == 'buy':
            if arsi <= cfg.buy_dip.rsi_max_thresh:
                print("Resuming 'buy' operation...")
                p.execute_buy(cfg.buy_dip.share)
                if p.state.get('operation') is None:  # operation finished
                    self.BD_in_cool = cfg.buy_dip.cool
                    self._save_all_states()
            else:
                print("Conditions for 'buy' no longer valid. Clearing state.")
                self._clear_portfolio_state(p)

        elif operation == 'sell_tp':
            if self._should_sell_tp(cfg, p, arsi):
                print("Resuming 'sell_tp' operation...")
                p.execute_sell(cfg.sell_tp.share)
                if p.state.get('operation') is None:  # operation finished
                    self.TP_in_cool = cfg.sell_tp.cool
                    self._save_all_states()
            else:
                print("Conditions for 'sell' no longer valid. Clearing state.")
                self._clear_portfolio_state(p)

        elif operation == 'sell_sl':
            if self._should_sell_sl(cfg, p, arsi):
                print("Resuming 'sell_sl' operation...")
                p.execute_sell(cfg.sell_sl.share)
                self.SL_in_cool = cfg.sell_sl.cool
                self._save_all_states()
            else:
                print("Conditions for 'sell' no longer valid. Clearing state.")
                self._clear_portfolio_state(p)

    def _clear_portfolio_state(self, p):
        p.clear_state()
        self._save_bot_state()

    def _run(self, cfg, p, arsi):
        p.discard_open_orders()
        operation = p.state.get('operation')

        today = datetime.utcnow().date()
        days_passed = (today - self.last_update_date).days

        print(f"ARSI={arsi}")
        if days_passed > 0:
            self.BD_in_cool = max(0, self.BD_in_cool - 1)
            self.SL_in_cool = max(0, self.SL_in_cool - 1)
            self.TP_in_cool = max(0, self.TP_in_cool - 1)
            self.last_update_date = today
            self._save_all_states()

        if operation:
            self._resume_operation(cfg, p, arsi)
        else:
            index_price = self.alt.get_today_price()

            if self._should_buy(cfg, arsi):
                self.last_buy_price = index_price
                print("**** Buy the dip triggered")
                p.execute_buy(cfg.buy_dip.share)
                if p.state.get('operation') is None:
                    self.BD_in_cool = cfg.buy_dip.cool
                    self._save_all_states()

            elif index_price > self.last_buy_price and self._should_sell_sl(cfg, p, arsi):
                print("**** Sell_sl triggered")
                p.execute_sell(cfg.sell_sl.share, "sell_sl")
                self.SL_in_cool = cfg.sell_sl.cool
                self._save_all_states()

            elif index_price > self.last_buy_price and self._should_sell_tp(cfg, p, arsi):
                print("**** Sell_tp triggered")
                p.execute_sell(cfg.sell_tp.share, "sell_tp")
                self.TP_in_cool = cfg.sell_tp.cool
                self._save_all_states()

    def run(self):
        cfg = self.config
        p = self.portfolio
        alt = self.alt

        self._load_state()

        last_date_checked = None

        while True:
            now = datetime.utcnow().date()
            if now != last_date_checked:
                alt._build_market_cap_per_date()
                alt.compute_rsi()
                arsi = alt.get_ARSI()
                last_date_checked = now

            self._run(cfg, p, arsi)

            time.sleep(10)


if __name__ == '__main__':
    bot = BotATF("bot_name", "binance_crypto_l3")
    bot.run()
