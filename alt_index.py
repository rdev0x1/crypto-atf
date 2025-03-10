import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
from stock import Stock
from binance_client import bclient
from coingecko import Coingecko
from crypto_stock import CryptoStock


class AltIndex(Stock):
    def __init__(self):
        self.nb_assets = 10
        self.cg = Coingecko()

    def _get_valid_top_coins(self):
        """
        build list of top 250 coins that are not stablecoin and not ETH
        """
        top_coins = self.cg.get_top_250_coins()
        return [
            coin['symbol'] for coin in top_coins
            if CryptoStock.is_tradable(coin['symbol']) and self.cg.is_valid_alt_coin(coin['symbol'])
        ]

    def _build_market_cap_per_date(self):
        """
        Builds a daily index DataFrame with:
          - 'top10_coins': list of 10 coin symbols used on that day.
            Updated on month-end dates and remains fixed until next update.
          - 'total_market_cap': sum of market caps for these 10 coins on that day.
          - 'close': artificial price series (today's total market cap / first day's total market cap).

        Process:
          - Retrieve historical market caps for valid tradable coins.
          - On month-end dates, select top-10 based on available market cap.
          - On other days, reuse previous month's top-10.
        Returns:
            pd.DataFrame: indexed by date with columns:
                          'top10_coins', 'total_market_cap', 'close'
        """
        market_caps_df = pd.DataFrame()
        valid_coins = self._get_valid_top_coins()

        # Collect daily market cap data for each tradable coin
        for coin in valid_coins:
            print(f"----{coin}")
            ticker = self.cg.get_marketcap_history(coin)
            ticker.index = pd.to_datetime(ticker.index)
            coin_caps = ticker[['market_cap']].rename(columns={'market_cap': coin})
            market_caps_df = coin_caps if market_caps_df.empty else market_caps_df.join(coin_caps, how='outer')

        # Build the daily index using the fixed top10 strategy
        records, current_top10 = [], []
        for date, row in market_caps_df.iterrows():

            # If no top10 yet or if date is month-end, update current_top10 based on today's data
            if date.is_month_end or not current_top10:
                daily_caps = row.dropna().sort_values(ascending=False)
                current_top10 = daily_caps.head(self.nb_assets).index.tolist()

            # Compute total market cap for today's fixed top10
            total_mc = row[current_top10].sum(skipna=True)
            records.append({
                'date': date,
                'top10_coins': current_top10.copy(),
                'total_market_cap': total_mc
            })

        # Build DataFrame and compute artificial price series
        df = pd.DataFrame(records).set_index('date').sort_index()
        df['close'] = df['total_market_cap'] / df.iloc[0]['total_market_cap']
        self.ticker = df


alt = AltIndex()

if __name__ == "__main__":
    bclient.set_key("binance_crypto_l3ro")
    alt._build_market_cap_per_date()
    fig, ax = plt.subplots(1, figsize=(10, 8))
    alt.draw(ax)
    plt.show()
