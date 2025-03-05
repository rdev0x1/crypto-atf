import logging
from copy import deepcopy
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd

from config_atf import ConfigATF as config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Stock:
    """
    Represents a financial stock with associated OHLC and technical indicator data.
    """

    def __init__(self, name: str) -> None:
        self.name: str = name
        self.quantity: float = 0.0
        self.ticker: Optional[pd.DataFrame] = None
        self.curr_price: Optional[float] = None
        self.curr_date: Optional[pd.Timestamp] = None

    def load_csv(self, fname: str) -> None:
        """
        Load ticker data from a CSV file.
        Expected CSV columns: Date, open, high, low, close, etc.
        The CSV is reindexed to a continuous date range using forward fill.
        """
        self.ticker = pd.read_csv(fname, index_col=0, parse_dates=[0])
        # Ensure the index is datetime and reindex for continuous date range.
        self.ticker.index = pd.to_datetime(self.ticker.index, errors="coerce")
        first_date = self.ticker.index.min()
        last_date = self.ticker.index.max()
        full_range = pd.date_range(first_date, last_date)
        self.ticker = self.ticker.reindex(full_range, method="ffill")

    def to_csv(self, fname: str) -> None:
        if self.ticker is not None:
            self.ticker.to_csv(fname)
        else:
            logger.warning("Ticker data is empty. Nothing to save.")

    def clone(self) -> "Stock":
        return deepcopy(self)

    def compute_sl(self) -> None:
        """
        Compute Stop Loss (SL) based on the configuration.
        This computes:
          - SL: (low.shift(window) / low - 1)
          - sl_price: high.shift(window) * (1 + threshold)
          - sl_signal: whether SL is below the threshold
          - sl_close: adjusted close price based on stop loss condition.
        """
        win = config.win_sl
        high_shifted = self.ticker.high.shift(win)
        low = self.ticker.low
        self.ticker["SL"] = low / high_shifted - 1
        self.ticker["sl_price"] = high_shifted * (1 + config.tresh)
        cond = low < self.ticker["sl_price"]
        self.ticker["sl_signal"] = self.ticker["SL"] < config.tresh
        # Default sl_close equals close price; override where condition is met.
        self.ticker["sl_close"] = self.ticker.close
        self.ticker.loc[cond, "sl_close"] = self.ticker["sl_price"]

    def compute_rsi(self) -> None:
        """
        Compute the Relative Strength Index (RSI) using an exponential moving average
        over a 14-days window.
        """
        delta = self.ticker["close"].diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        # Use a smoothing constant (here 'com' parameter) of 13 to compute exponential moving averages.
        ema_up = up.ewm(com=13, adjust=False).mean()
        ema_down = down.ewm(com=13, adjust=False).mean()
        rs = ema_up / ema_down
        self.ticker["RSI"] = 100 - (100 / (1 + rs))

        # basic 5 days average RSI
        self.compute_average_rsi()

    def compute_average_rsi(self) -> None:
        self.ticker['ARSI'] = self.ticker['RSI'].rolling(window=5, min_periods=5).mean()


    def draw(
        self,
        ax: plt.Axes,
        ax_rsi: Optional[plt.Axes] = None,
        ax_sl: Optional[plt.Axes] = None,
        ref_stock: Optional["Stock"] = None,
    ) -> None:
        """
        Draw the stock's price and RSI charts.
        If a reference stock is provided, its data is overlaid.
        """
        # Ensure the index is datetime
        self.ticker.index = pd.to_datetime(self.ticker.index, errors="coerce")
        ax.plot(self.ticker.index, self.ticker.close, label="close")
        legends = ["close"]

        if ref_stock is not None and ref_stock.ticker is not None:
            ref_stock.ticker.index = pd.to_datetime(ref_stock.ticker.index, errors="coerce")
            ax.plot(ref_stock.ticker.index, ref_stock.ticker.close, label="ref_close")
            legends.append("ref_close")

        ax.legend(legends)

        if ax_rsi is not None:
            ax_rsi.plot(self.ticker.index, self.ticker.RSI, label="RSI")
            legends_rsi = ["RSI"]
            if ref_stock is not None and ref_stock.ticker is not None:
                ax_rsi.plot(ref_stock.ticker.index, ref_stock.ticker.RSI, label="Ref_RSI")
                legends_rsi.append("Ref_RSI")
            ax_rsi.legend(legends_rsi)

    def get_value(self) -> float:
        """Calculate the total value of the stock holding."""
        if self.curr_price is None:
            logger.error("Current price is not set.")
            return 0.0
        return self.quantity * self.curr_price

    def update(self, _date) -> None:
        """
        Update the current price based on a given date from the ticker data.
        """
        try:
            # Using "close" column (adjust if needed)
            self.curr_price = float(self.ticker.loc[_date, "close"])
        except Exception as e:
            logger.exception(f"Error updating price for date {_date}: {e}")
            raise

    def update_simulation(self, _date) -> None:
        """Update the simulation state based on a given date."""
        try:
            self.curr_price = self.ticker.loc[_date, "close"]
        except KeyError as e:
            logger.exception(f"Date {_date} not found in ticker data: {e}")
            raise StopIteration("Simulation ended due to missing date.")

        self.curr_date = _date
        if not isinstance(self.curr_price, float):
            logger.error("Current price is not a float.")
            raise TypeError("Invalid current price type.")


if __name__ == "__main__":
    # Test and compare rsi test reference with computed one
    s = Stock("trading_view_idx")
    s.load_csv("data/test/data_test.csv")
    ref = s.clone()
    # Use pre-computed RSI from the reference CSV.
    ref.ticker["RSI"] = ref.ticker["rsitest"]
    s.compute_rsi()

    # Create subplots for price and RSI
    fig, (ax, ax_rsi) = plt.subplots(2, figsize=(10, 8))
    s.draw(ax, ax_rsi=ax_rsi, ref_stock=ref)
    s.to_csv("data/tmp/verif_rsi.csv")
    plt.show()
