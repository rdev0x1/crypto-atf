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

    def _build_market_cap_per_date(self):
        """
        Builds a daily index DataFrame with:
          - 'top10_coins': the list of 10 coin symbols used on that day.
            The list is updated on month-end dates and remains fixed until the next update.
          - 'total_market_cap': the sum of market caps for these 10 coins computed for that day.
          - 'price': an artificial price series computed as (today's total market cap /
                     first day's total market cap), with the first day set to 1.

        Process:
          - For each coin in the top 250 from Coingecko, if tradable, retrieve its market cap history.
          - Combine all coins’ daily market cap data into a dict keyed by date.
          - For each day (in sorted order), if it’s a month‐end (or the first date), recalc the top‑10
            based on that day’s available market cap data. Otherwise, use the most recent top‑10.
          - Compute the total market cap for that day from the fixed top‑10 coins.
        Returns:
            pd.DataFrame: Daily DataFrame indexed by date with columns:
                          'top10_coins', 'total_market_cap', 'price'
        """
        cg = Coingecko()
        top_coins = cg.get_top_250_coins()
        # daily_marketcaps will map each date to a dict of {coin: market_cap}
        daily_marketcaps = {}

        # Collect daily market cap data for each tradable coin
        for coin in top_coins:
            asset = coin['symbol']
            if not CryptoStock.is_tradable(asset):
                print(f"Skipping {asset}: not tradable")
                continue
            if not cg.is_valid_alt_coin(asset):
                print(f"Skipping {asset}: not valid atl coin")
                continue
            print(f"==== Processing {asset} ====")

            ticker = cg.get_marketcap_history(asset)
            ticker.index = pd.to_datetime(ticker.index)
            for date, row in ticker.iterrows():
                # Create entry for date if not exists
                daily_marketcaps.setdefault(date, {})[asset] = row['market_cap']

        # Build the daily index using the fixed top10 strategy
        records = []
        current_top10 = None
        for date in sorted(daily_marketcaps.keys()):
            day_data = daily_marketcaps[date]

            # If no top10 yet or if date is month-end, update current_top10 based on today's data.
            if (current_top10 is None) or (pd.Timestamp(date).is_month_end):
                sorted_coins = sorted(day_data.items(), key=lambda x: x[1], reverse=True)
                current_top10 = [coin for coin, cap in sorted_coins[:self.nb_assets]]

            # Carry over missing coins from previous days
            filtered_top10 = [coin for coin in current_top10 if coin in day_data]
            if len(filtered_top10) < self.nb_assets:
                missing_coins = [coin for coin in current_top10 if coin not in filtered_top10]
                print(f"Missing {self.nb_assets-len(filtered_top10)} coins on {date}: {missing_coins}")  # Debugging output
                # Keep the previous month's coins if necessary
                filtered_top10 += [coin for coin in current_top10 if coin not in filtered_top10][:self.nb_assets - len(filtered_top10)]

            current_top10 = filtered_top10

            # Compute total market cap for today's fixed top10.
            # If a coin is missing on a day, default to 0.
            total_mc = sum(day_data.get(coin, 0) for coin in current_top10)
            records.append({
                'date': date,
                'top10_coins': current_top10.copy(),
                'total_market_cap': total_mc
            })

        # Build DataFrame and compute artificial price series.
        df = pd.DataFrame(records).set_index('date').sort_index()
        # Price is computed as today's total market cap relative to the first day's total market cap.
        df['close'] = df['total_market_cap'] / df.iloc[0]['total_market_cap']
        self.ticker = df
        df.to_csv("toto.csv")

alt = AltIndex()


if __name__ == "__main__":
    bclient.set_key("binance_crypto_l3ro")
    alt._build_market_cap_per_date()
    fig, ax = plt.subplots(1, figsize=(10, 8))
    alt.draw(ax)
    plt.show()
