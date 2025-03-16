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
        self.top10_coins = {}
        self.top10_date = None
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

    def get_previous_month_end_date(self, date=None):
        if date is None:
            date = dt.datetime.now()
        first_day_current_month = date.replace(day=1)
        return (first_day_current_month - dt.timedelta(days=1)).date()

    def _get_market_caps_for_date(self, date):
        market_caps = {}
        for coin in self._get_valid_top_coins():
            ticker = self.cg.get_marketcap_history(coin)
            ticker.index = pd.to_datetime(ticker.index)
            market_caps[coin] = ticker.loc[str(date), 'market_cap'] if str(date) in ticker.index else 0
        return market_caps

    def get_current_top_10_coins(self):
        top10_date = self.get_previous_month_end_date()

        if top10_date == self.top10_date and self.top10_coins:
            return self.top10_coins

        market_caps = self._get_market_caps_for_date(top10_date)
        sorted_coins = sorted(market_caps.items(), key=lambda x: x[1], reverse=True)[:self.nb_assets]

        total_market_cap = sum(cap for _, cap in sorted_coins)
        self.top10_coins = {
            coin: {'market_cap': cap, 'weight': cap / total_market_cap if total_market_cap else 0}
            for coin, cap in sorted_coins
        }

        self.top10_date = top10_date
        return self.top10_coins

    def get_today_price(self):
        today = pd.Timestamp(dt.datetime.now().date())

        if today not in self.ticker.index:
            raise ValueError(f"Today's date ({today.date()}) is not found in the ticker index. Data may be outdated or incomplete.")

        return self.ticker.loc[today, 'close']

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
