import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from config_atf import ConfigATF
from binance_client import bclient
from alt_index import AltIndex


class BotATF:
    def __init__(self, start_date="2020-01-01", alt=None):
        self.config = ConfigATF()
        self.start_date = start_date

        if alt is None:
            # Build the alt index and compute RSI.
            self.alt = AltIndex()
            self.alt._build_market_cap_per_date()
            self.alt.compute_rsi()
        else:
            self.alt = alt
        self.ticker = self.alt.ticker.copy()

        # Initialize portfolio state:
        # alt_units: asset holdings (start at 0)
        # reserve: cash available (starts at config.funding.initial_fund)
        self._initialize_portfolio()
        # Compute trading signals (SL, BD, etc.)
        self._compute_signals()
        # Compute updated balances using only vectorized operations.
        self._compute_balances()

        # (Optional) Export full ticker for debugging.
        self.ticker.to_csv("titi.csv")

    def _initialize_portfolio(self):
        # Set initial balances.
        self.ticker['alt_units'] = 0.0
        self.ticker['reserve'] = self.config.funding.initial_fund
        # alt_value is simply alt_units * close price.
        self.ticker['alt_value'] = self.ticker['alt_units'] * self.ticker['close']
        self.ticker['total'] = 0

    def _compute_signals(self):
        cfg = self.config

        # Compute rolling maximum for the stop-loss rule.
        self.ticker['rolling_max'] = self.ticker['close'].rolling(window=cfg.sell_sl.win_sl, min_periods=1).max()

        # --- Compute Raw Signals (for debugging) ---
        # Raw Stop Loss: trigger if current close is below the threshold of the recent rolling maximum.
        #self.ticker['SL_signal_raw'] = self.ticker['close'] < (1 - cfg.sell_sl.tresh) * self.ticker['rolling_max']
        self.ticker['SL_signal_raw'] = (self.ticker['ARSI'] >= cfg.sell_sl.rsi_min_thresh) & (self.ticker['close'] < (1 - cfg.sell_sl.tresh) * self.ticker['rolling_max'])

        # Raw Buy Dip: trigger when RSI is at or below the configured max (lower bound is 0 by default).
        self.ticker['BD_signal_raw'] = (self.ticker['ARSI'] >= cfg.buy_dip.rsi_min_thresh) & (self.ticker['ARSI'] <= cfg.buy_dip.rsi_max_thresh)

        self.ticker['TP_signal_raw'] = self.ticker['ARSI'] > cfg.sell_tp.rsi_min_thresh

        # --- Compute Cooldown Masks ---
        # For stop-loss: if any raw SL occurred in the previous sell_sl.cool days, we're in a cool period.
        self.ticker['SL_in_cool'] = (
            self.ticker['SL_signal_raw']
            .rolling(window=cfg.sell_sl.cool, min_periods=1)
            .max()
            .shift(1)
            .fillna(False)
            .astype(bool)
        )

        # For buy dip: if any raw BD signal occurred in the previous buy_dip.cool days, we're in a cool period.
        self.ticker['BD_in_cool'] = (
            self.ticker['BD_signal_raw']
            .rolling(window=cfg.buy_dip.cool, min_periods=1)
            .max()
            .shift(1)
            .fillna(False)
            .astype(bool)
        )

        self.ticker['TP_in_cool'] = (
            self.ticker['TP_signal_raw']
            .rolling(window=cfg.sell_tp.cool, min_periods=1)
            .max()
            .shift(1)
            .fillna(False)
            .astype(bool)
        )

        # --- Final Signals after Applying Cooldown Rules ---
        # Allow an SL signal only if we are not already in its cool period.
        self.ticker['SL_signal'] = (self.ticker['SL_signal_raw'] &
                                    ~self.ticker['SL_in_cool'] &
                                    ~self.ticker['BD_in_cool'])

        self.ticker['TP_signal'] = self.ticker['TP_signal_raw'] & ~self.ticker['TP_in_cool']

        # For buy signals, disallow any buys if we are in an SL cool period.
        self.ticker['BD_signal'] = (self.ticker['BD_signal_raw'] &
                                    ~self.ticker['BD_in_cool'] &
                                    ~self.ticker['SL_in_cool'] &
                                    ~self.ticker['TP_in_cool'])

        # Enforce mutual exclusivity on the same day.
        # For example, if an SL occurs (final signal) then BD  are suppressed.
        self.ticker.loc[self.ticker['SL_signal'], ['BD_signal']] = False

    def _compute_balances(self):
        df = self.ticker[self.ticker.index >= self.start_date].copy()

        cfg = self.config
        init_funds = cfg.funding.initial_fund
        tp_share = cfg.sell_tp.tp_share
        dip_share = cfg.buy_dip.dip_share

        df['reserve'] = 0.0
        df['alt_value'] = 0.0
        df['nb_assets'] = 0.0
        df['total'] = 0.0
        df['buy_price'] = np.nan

        prev_nb_assets = 0.0
        last_buy_price = sys.float_info.max
        first_buy = True

        # Make the index start exactly as the portfolio value to make things
        # easier to compare
        df['close'] = df['close']/df.iloc[0]['close']*init_funds

        for i in range(len(df)):
            if i == 0:
                df.iloc[i, df.columns.get_loc('reserve')] = init_funds
                prev_nb_assets = 0.0
            else:
                df.iloc[i, df.columns.get_loc('reserve')] = df.iloc[i - 1]['reserve']
                prev_nb_assets = df.iloc[i - 1]['nb_assets']

            close_price = df.iloc[i]['close']

            buy_signal = df.iloc[i]['BD_signal']
            sl_signal = df.iloc[i]['SL_signal']
            tp_signal = df.iloc[i]['TP_signal']

            if buy_signal and df.iloc[i]['reserve'] > 0:
                _dip_share = 1.0 if first_buy else dip_share
                first_buy = False
                # if there is few money, invest it all
                if df.iloc[i]['reserve'] < 10:
                    _dip_share = 1.0
                buy_amount = df.iloc[i]['reserve'] * _dip_share
                df.iloc[i, df.columns.get_loc('reserve')] -= buy_amount
                df.iloc[i, df.columns.get_loc('buy_price')] = close_price
                df['buy_price'] = df['buy_price'].ffill()
                nb_assets = prev_nb_assets + buy_amount / close_price
                last_buy_price = close_price
            else:
                nb_assets = prev_nb_assets

            #if sl_signal and (close_price >= df.iloc[i]['buy_price']):
            if sl_signal and (close_price >= last_buy_price):
                df.iloc[i, df.columns.get_loc('reserve')] += close_price * nb_assets
                nb_assets = 0.0
                last_buy_price = sys.float_info.max

            if tp_signal:
                tp_amount = close_price * nb_assets * tp_share
                df.iloc[i, df.columns.get_loc('reserve')] += tp_signal * tp_share * close_price * nb_assets
                nb_assets -= nb_assets * tp_share

            df.iloc[i, df.columns.get_loc('alt_value')] = nb_assets * close_price
            df.iloc[i, df.columns.get_loc('nb_assets')] = nb_assets

            df.iloc[i, df.columns.get_loc('total')] = (
                df.iloc[i]['alt_value'] + df.iloc[i]['reserve']
            )

            prev_nb_assets = nb_assets

        df['ref'] = df['close']

        self.ticker = df

    def draw(self):
        # Create subplots: top for portfolio, bottom for RSI
        fig, (ax_price, ax_rsi) = plt.subplots(
            2,        # two rows (price on top, RSI on bottom)
            1,        # one column
            figsize=(14, 9),
            sharex=True
        )

        # ------ TOP AXES: MAIN PORTFOLIO PLOT ------
        ax_price.plot(self.ticker.index, self.ticker['total'], label='Total Portfolio Value', color='purple', linewidth=2)
        ax_price.plot(self.ticker.index, self.ticker['ref'], label='Alt Close Price', color='black', alpha=0.5)
        ax_price.plot(self.ticker.index, self.ticker['reserve'], label='Reserve', linestyle='--', color='blue')

        ax_price.scatter(self.ticker.index[self.ticker['SL_signal']], self.ticker['ref'][self.ticker['SL_signal']],
                        marker='x', color='red', s=100, label='SL')
        ax_price.scatter(self.ticker.index[self.ticker['TP_signal']], self.ticker['ref'][self.ticker['TP_signal']],
                        marker='o', color='blue', s=80, label='TP')
        ax_price.scatter(self.ticker.index[self.ticker['BD_signal']], self.ticker['ref'][self.ticker['BD_signal']],
                        marker='^', color='green', s=100, label='BD')

        ax_price.set_title('Portfolio Value and Signals')
        ax_price.set_ylabel('Value')
        ax_price.legend()
        ax_price.grid(True)

        # ------ BOTTOM AXES: RSI PLOT ------
        cfg = self.config
        rsi_dip = cfg.buy_dip.rsi_max_thresh
        rsi_tp = cfg.sell_tp.rsi_min_thresh
        ax_rsi.plot(self.ticker.index, self.ticker['ARSI'], label='ARSI', color='brown')
        ax_rsi.axhline(rsi_dip, linestyle='--', color='blue', alpha=0.5)
        ax_rsi.axhline(rsi_tp, linestyle='--', color='blue', alpha=0.5)
        ax_rsi.set_ylabel('ARSI')
        ax_rsi.set_xlabel('Date')
        ax_rsi.legend()
        ax_rsi.grid(True)

        # Layout
        plt.tight_layout()
        plt.show()

if __name__ == '__main__':
    bclient.set_key("binance_crypto_l3ro")
    bot = BotATF()
    bot.draw()
