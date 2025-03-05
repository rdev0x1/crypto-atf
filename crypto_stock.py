import os
import math
import logging
from typing import Tuple, Optional
from dateutil import parser
import datetime as dt
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np

from stock import Stock
from binance_client import bclient
from config_atf import ConfigATF as config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CryptoStock(Stock):
    """
    Extends Stock to handle crypto-specific operations with Binance data.
    """

    def __init__(self, asset: str, qty: float = 0.0) -> None:
        super().__init__(asset)
        self.quantity: float = qty
        self.symbol: str = self._get_symbol()
        self.ticker: Optional[pd.DataFrame] = None

        # Binance-specific trading parameters (will be updated later)
        self.step_quantity: Optional[float] = None  # Minimum quantity allowed
        self.step_price: Optional[float] = None       # Price tick size
        self.min_notional: Optional[float] = None     # Minimum notional value
        self.cache_exchange_info: Optional[list] = None

        self.fetch_tickers(asset)

    def _extract_lot_size(self, exchange_info_symbol: dict) -> None:
        """
        Extract and store lot size, price filter and notional values from exchange info.
        """
        for f in exchange_info_symbol.get('filters', []):
            filter_type = f.get('filterType')
            if filter_type == 'LOT_SIZE':
                self.step_quantity = float(f.get('stepSize'))
            elif filter_type == 'PRICE_FILTER':
                self.step_price = float(f.get('tickSize'))
            elif filter_type == 'NOTIONAL':
                self.min_notional = float(f.get('minNotional'))

    def _update_lot_size(self) -> None:
        """
        Update the Binance lot size parameters if they are not already set.
        """
        if all(param is not None for param in (self.step_price, self.step_quantity, self.min_notional)):
            return

        if self.cache_exchange_info is None:
            self.cache_exchange_info = bclient.client.get_exchange_info()['symbols']

        for symbol_info in self.cache_exchange_info:
            if self.symbol == symbol_info.get('symbol'):
                self._extract_lot_size(symbol_info)
                logger.info(f"Updated lot size for {self.symbol}")
                return

    def _get_symbol(self) -> str:
        """
        Construct the Binance trading pair symbol.
        Currently concatenates asset and underlying symbol (e.g. 'BTCUSDT').
        """
        return f"{self.name.upper()}{config.underlying_symb}"

    def _get_base_asset(self, asset: str) -> str:
        """
        Returns the base asset. If asset is in USD_coins, returns 'USD'; otherwise, the underlying symbol.
        """
        USD_coins = {"MIOTA"}
        if asset in USD_coins:
            return "USD"
        logger.debug(f"{asset}:{config.underlying_symb}")
        return config.underlying_symb

    def _minutes_of_new_data(self, symbol: str, kline_size: str, data: pd.DataFrame, source: str) -> Tuple[dt.datetime, dt.datetime]:
        """
        Calculate the time range for new data based on the latest timestamp in `data`
        and the most recent data from Binance.
        If the local history is empty, query Binance for the very first available kline
        (using an arbitrarily early start string) so that the start date reflects the actual history.
        """
        if source != "binance":
            raise ValueError(f"Unexpected data source {source}")

        if not data.empty:
            old = parser.parse(data["timestamp"].iloc[-1])
        else:
            # Query Binance for the earliest available kline for this symbol.
            first_klines = bclient.client.get_historical_klines(symbol, kline_size, "1 Jan 1970", limit=1)
            if first_klines and len(first_klines) > 0:
                old = pd.to_datetime(first_klines[0][0], unit="ms")
            else:
                raise ValueError(f"No historical data available for {symbol} from Binance.")

        # Get the latest available kline timestamp (assumed to be in milliseconds)
        klines = bclient.client.get_klines(symbol=symbol, interval=kline_size)
        new = pd.to_datetime(klines[-1][0], unit="ms")

        if old != new:
            logger.info(f"Time range for {symbol}: {old} to {new}")
        return old, new

    def _get_all_binance(self, symbol: str, kline_size: str, save: bool = False) -> pd.DataFrame:
        """
        Retrieve historical kline data from Binance and update with new data if available.
        Data is saved to CSV for the current symbol and kline size.
        """
        binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
        filename = f"data/bnc/{symbol}-{kline_size}-data.csv"

        if os.path.isfile(filename):
            data_df = pd.read_csv(filename)
        else:
            data_df = pd.DataFrame()

        oldest_point, newest_point = self._minutes_of_new_data(symbol, kline_size, data_df, source="binance")
        delta_min = (newest_point - oldest_point).total_seconds() / 60
        available_data = math.ceil(delta_min / binsizes[kline_size])

        if not data_df.empty:
            data_df["timestamp"] = pd.to_datetime(data_df["timestamp"], errors="coerce")
            if delta_min < 60:
                data_df.set_index("timestamp", inplace=True)
                return data_df

        if data_df.empty:
            logger.info(f"Downloading all available {kline_size} data for {symbol}. Be patient!")
        else:
            logger.info(f"Downloading {delta_min:.0f} minutes of new data for {symbol} (~{available_data} instances of {kline_size}).")

        klines = bclient.client.get_historical_klines(
            symbol,
            kline_size,
            oldest_point.strftime("%d %b %Y %H:%M:%S"),
            newest_point.strftime("%d %b %Y %H:%M:%S"),
        )
        ohlcv_columns = ["open", "high", "low", "close", "volume"]
        columns = ["timestamp"] + ohlcv_columns + ["close_time", "quote_av", "trades", "tb_base_av", "tb_quote_av", "ignore"]
        new_data = pd.DataFrame(klines, columns=columns)
        new_data["timestamp"] = pd.to_datetime(new_data["timestamp"], unit="ms")
        for col in columns:
            new_data[col] = pd.to_numeric(new_data[col], errors="coerce")

        if not data_df.empty:
            data_df["timestamp"] = pd.to_datetime(data_df["timestamp"])
            combined_df = pd.concat([data_df, new_data]).drop_duplicates(subset=["timestamp"], keep="last")
        else:
            combined_df = new_data.copy()

        combined_df.set_index("timestamp", inplace=True)
        if save and not bclient.is_test_mode:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            # Use date_format in to_csv to preserve human-readable timestamps.
            combined_df.index = pd.to_datetime(combined_df.index)
            combined_df.to_csv(filename, date_format="%Y-%m-%d %H:%M:%S")
            logger.info(f"Saved market data to {filename}")
        logger.info("Data retrieval complete.")
        return combined_df

    def fetch_tickers(self, asset: str) -> None:
        """
        Fetch ticker data from Binance for the asset using a 1-hour kline.
        """
        self.ticker = self._get_all_binance(self.symbol, "1h", save=True)
        # Ensure that the 'close' column is of float type.
        self.ticker["close"] = self.ticker["close"].astype(float)

    def get_coin_price(self, side: Optional[str] = None) -> float:
        """
        Retrieve the current coin price based on the order book.
        - For BUY: return the best bid.
        - For SELL: return the best ask.
        - For None: return the mid-price.
        """
        order_book = bclient.client.get_order_book(symbol=self.symbol)
        if side == "BUY":
            return float(order_book["bids"][0][0])
        elif side == "SELL":
            return float(order_book["asks"][0][0])
        elif side is None:
            bid = float(order_book["bids"][0][0])
            ask = float(order_book["asks"][0][0])
            return (bid + ask) / 2
        else:
            raise ValueError(f"Unexpected order side: {side}")

    def buy(self, amount: float) -> None:
        """
        Place a limit buy order for a given amount.
        Calculates the quantity using the current price and adjusts using lot size filters.
        """
        price0 = self.get_coin_price(side="BUY")
        qty0 = amount / price0
        self._update_lot_size()
        qty = round(qty0 - qty0 % self.step_quantity, 10)
        price = round(price0 - price0 % self.step_price, 10)
        notional = round(qty * price - (qty * price) % self.step_price, 10)
        if notional < self.min_notional:
            logger.info(f"*** BUY IGNORE {self.symbol}: notional too small ({notional} < {self.min_notional})")
            return
        price_str = np.format_float_positional(price, fractional=True)
        confirm = input(f"**** BUY {self.symbol}: {qty} at price = {price_str}, notional = {notional}? [y/n] ")
        if confirm.lower() != "y":
            return
        try:
            bclient.client.order_limit_buy(symbol=self.symbol, quantity=qty, price=price_str, timeInForce="GTC")
        except Exception as e:
            logger.exception(f"*** Failed to buy {self.symbol}: {e}")

    def sell(self, share: float) -> float:
        """
        Place a limit sell order for a given share of the holding.
        Returns the total sell value, or 0 if not executed.
        """
        if not (0 < share <= 1):
            raise ValueError("Share must be between 0 and 1.")
        qty0 = self.quantity * share
        price0 = self.get_coin_price(side="SELL")
        self._update_lot_size()
        qty = round(qty0 - qty0 % self.step_quantity, 10)
        price = round(price0 - price0 % self.step_price, 10)
        notional = qty * price
        if notional < self.min_notional:
            logger.info(f"*** SELL IGNORE {self.symbol}: notional too small ({notional} < {self.min_notional})")
            return 0.0
        price_str = np.format_float_positional(price)
        confirm = input(f"*** SELL {self.symbol}: {100 * share:.1f}% ({qty}) at price = {price_str}, notional = {notional}? [y/n] ")
        if confirm.lower() != "y":
            return 0.0
        ret = bclient.client.order_limit_sell(symbol=self.symbol, quantity=qty, price=price_str, timeInForce="GTC")
        return float(price) * qty

    def get_value(self) -> float:
        """
        Calculate the total current value of the holding.
        """
        return self.quantity * self.get_coin_price(side="BUY")


if __name__ == "__main__":
    bclient.set_key("binance_crypto_l3ro")
    s = CryptoStock("BTC", qty=0)
    fig, ax = plt.subplots(1, figsize=(10, 8))
    s.draw(ax)
    plt.show()
