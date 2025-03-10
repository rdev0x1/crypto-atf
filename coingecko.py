import json
from pathlib import Path
import os
import logging
import datetime as dt
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)


class Coingecko:
    """A class for retrieving market data from the CoinGecko API.

    This class retrieves top-market coins, assigns categories (e.g., stablecoin,
    ETH, BTC), and fetches both current and historical market cap data. It also
    leverages a caching strategy (JSON files for coin metadata and CSV files for
    market-cap data) to reduce the number of API calls needed.
    """

    def __init__(self) -> None:
        self.api_base_url = "https://api.coingecko.com/api/v3"
        self.request_timeout = 120

        # Set up session with retry strategy.
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        # Use pathlib for directory handling.
        self.cache_dir = Path("data")
        self.marketcap_dir = self.cache_dir / "marketcap"
        self.marketcap_dir.mkdir(parents=True, exist_ok=True)

        self.mcp_dir = self.cache_dir / "mcp"
        self.mcp_dir.mkdir(parents=True, exist_ok=True)

        self._monthly_coins: Optional[List[Dict[str, Any]]] = None
        self.history_cache: Dict[str, pd.DataFrame] = {}

        # Fetch the top 250 coins on initialization.
        self.top_coins: List[Dict[str, Any]] = self.get_top_250_coins()

        self.categories_file = self.cache_dir / "coin_categories.json"
        self.coin_categories = self._load_coin_categories()

    def get_coin_id(self, symbol: str) -> Optional[str]:
        """Retrieve the CoinGecko coin ID for a given symbol from the top coins list."""
        symbol = symbol.upper()
        for coin in self.top_coins:
            if coin["symbol"] == symbol:
                return coin["id"]
        return None

    def _load_coin_categories(self) -> Dict[str, str]:
        """Load coin categories from coin_categories.json."""
        try:
            with self.categories_file.open("r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _save_coin_categories(self) -> None:
        """Save the coin categories cache to coin_categories.json."""
        with self.categories_file.open("w") as f:
            json.dump(self.coin_categories, f, indent=4)

    def _fetch_coin_info(self, coin_id: str) -> Optional[Dict[str, Any]]:
        """Fetch full coin details for a given CoinGecko coin ID."""
        url = f"{self.api_base_url}/coins/{coin_id}"
        try:
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching coin info for {coin_id}: {e}")
            return None

    def _extract_category(self, coin_data: Optional[Dict[str, Any]]) -> str:
        """Extract the category from the coin data."""
        if not coin_data:
            return ""
        cats = coin_data.get("categories", [])
        stable = {"bridged stablecoins", "stablecoins"}
        eth = {"liquid staked eth", "bridged weth", "bridged wsteth"}
        btc = {"bridged wbtc"}
        bridged = {"bridged-tokens", "wrapped-tokens"}

        for cat in cats:
            cl = cat.lower()
            if cl in stable:
                return "stablecoin"
            if cl in eth:
                return "ETH"
            if cl in btc:
                return "BTC"
            if cl in bridged:
                return "BRIDGET-XXX"
        return ""

    def is_valid_alt_coin(self, symbol: str) -> bool:
        """Return True if the coin is a valid altcoin (not a stablecoin, ETH, BTC, or wrapped token)."""
        return self.get_coin_category(symbol) == ""

    def get_coin_category(self, symbol: str) -> str:
        """Get a coin's category, using cache to avoid unnecessary API calls."""
        symbol = symbol.upper()
        # Check if category is already cached.
        if symbol in self.coin_categories:
            return self.coin_categories[symbol]

        coin_id = self.get_coin_id(symbol)
        if not coin_id:
            self.coin_categories[symbol] = ""
            self._save_coin_categories()
            return ""

        # For ETH and BTC, simply return the symbol.
        if symbol in {"ETH", "BTC"}:
            category = symbol
        else:
            coin_data = self._fetch_coin_info(coin_id)
            category = self._extract_category(coin_data)

        self.coin_categories[symbol] = category
        self._save_coin_categories()
        return category

    def get_top_250_coins(self) -> List[Dict[str, Any]]:
        """Load or fetch the monthly top 250 coins using file caching."""
        import datetime as dt
        if self._monthly_coins is not None:
            return self._monthly_coins

        now = dt.datetime.utcnow()
        cache_file = self.marketcap_dir / f"marketcap_{now.year}_{now.month:02d}.json"
        if cache_file.exists():
            logger.info(f"Loading cached market cap data from {cache_file}")
            with cache_file.open("r") as f:
                self._monthly_coins = json.load(f)
            return self._monthly_coins or []

        logger.info("Fetching monthly top-250 coins from CoinGecko ...")
        url = f"{self.api_base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": "false"
        }
        try:
            response = self.session.get(url, params=params, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()
            valid_coins = []
            seen_symbols = set()
            for coin in data:
                symbol = coin["symbol"].upper()
                if symbol in seen_symbols:
                    continue
                valid_coins.append({
                    "symbol": symbol,
                    "id": coin["id"],
                    "market_cap": coin.get("market_cap", 0)
                })
                seen_symbols.add(symbol)
            with cache_file.open("w") as f:
                json.dump(valid_coins, f, indent=4)
            logger.info(f"Saved new monthly marketcap data to {cache_file}")
            self._monthly_coins = valid_coins
            return self._monthly_coins
        except Exception as e:
            logger.error(f"Error fetching top coins: {e}")
            self._monthly_coins = []
            return self._monthly_coins

    def _load_history_csv(self, csv_file: Path) -> pd.DataFrame:
        """Load a CSV file and convert date columns appropriately."""
        df = pd.read_csv(csv_file)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
        elif "Unnamed: 0" in df.columns:
            df.rename(columns={"Unnamed: 0": "date"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
        return df

    def get_marketcap_history(self, symbol: str) -> pd.DataFrame:
        """Fetch or update historical market cap data for `symbol`."""
        symbol = symbol.upper()
        coin_id = self.get_coin_id(symbol)
        if not coin_id:
            logger.error(f"No coin id found for symbol {symbol}")
            return pd.DataFrame()

        csv_file = self.mcp_dir / f"{symbol.lower()}.csv"
        if symbol in self.history_cache:
            df = self.history_cache[symbol]
        elif csv_file.exists():
            df = self._load_history_csv(csv_file)
        else:
            df = pd.DataFrame()

        now = dt.datetime.utcnow()
        if not df.empty:
            last_date = df.index.max().to_pydatetime()
        else:
            last_date = now - dt.timedelta(days=364)

        delta_hours = (now - last_date).total_seconds() / 3600
        if delta_hours < 24:
            return df

        start_timestamp = int(last_date.timestamp())
        now_timestamp = int(now.timestamp())
        delta_months = delta_hours / (24 * 31)
        if delta_months > 12:
            # free tier only has 1 year
            start_timestamp = now_timestamp - 12 * 30 * 24 * 3600

        try:
            params = {"vs_currency": "usd", "from": start_timestamp, "to": now_timestamp}
            url = f"{self.api_base_url}/coins/{coin_id}/market_chart/range"
            resp = self.session.get(url, params=params, timeout=self.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            market_caps = data.get("market_caps", [])
            prices = data.get("prices", [])
            if market_caps and prices:
                df_caps = pd.DataFrame(market_caps, columns=["timestamp", "market_cap"])
                df_prices = pd.DataFrame(prices, columns=["timestamp", "price"])

                df_caps["date"] = pd.to_datetime(df_caps["timestamp"], unit="ms")
                df_prices["date"] = pd.to_datetime(df_prices["timestamp"], unit="ms")

                new_df = pd.merge(
                    df_caps.drop("timestamp", axis=1),
                    df_prices.drop("timestamp", axis=1),
                    on="date",
                    how="outer"
                )
                new_df.set_index("date", inplace=True)
                new_df.sort_index(inplace=True)
                new_df.ffill(inplace=True)

                # Take one entry per day (closest to midnight)
                new_df.reset_index(inplace=True)
                new_df["day"] = new_df["date"].dt.normalize()
                new_df["time_diff"] = (new_df["date"] - new_df["day"]).abs()
                selected = new_df.loc[new_df.groupby("day")["time_diff"].idxmin()]
                selected["date"] = selected["day"]
                selected.drop(columns=["day", "time_diff"], inplace=True)
                selected.set_index("date", inplace=True)
                new_df = selected

                if not df.empty:
                    combined = pd.concat([df.reset_index(), new_df.reset_index()])
                    combined.drop_duplicates(subset=["date"], keep="last", inplace=True)
                    combined["date"] = pd.to_datetime(combined["date"])
                    combined.set_index("date", inplace=True)
                    df = combined.sort_index()
                else:
                    df = new_df

        except Exception as e:
            logger.error(f"Error updating market cap history for {symbol}: {e}")

        df.to_csv(csv_file, date_format="%Y-%m-%d %H:%M:%S")
        self.history_cache[symbol] = df
        return df

    def get_all_marketcap_histories(self) -> Dict[str, pd.DataFrame]:
        """Retrieve the market cap history for all symbols."""
        histories = {}
        symbols = [coin["symbol"] for coin in self.top_coins]
        for sym in symbols:
            logger.info(f"======== {sym}")
            histories[sym] = self.get_marketcap_history(sym)
        return histories


def main():
    cg = Coingecko()

    logger.info(f"Top coins: {cg.top_coins}")
    for s in ['USDT', 'ADA', 'BUSD', 'ETH', 'WETH']:
        coin_id = cg.get_coin_id(s)
        cat = cg.get_coin_category(s)
        logger.info(f"{s} -> {coin_id}, category={cat}")

    top_coins = cg.get_top_250_coins()
    logger.info(f"Top Coins: {top_coins}")

    histories = cg.get_all_marketcap_histories()
    for sym, df in histories.items():
        logger.info(f"Market cap history for {sym}: {len(df)} records")


if __name__ == "__main__":
    main()
