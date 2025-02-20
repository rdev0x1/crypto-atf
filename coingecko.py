import json
import os
import datetime as dt
import logging
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
        """Initialize the Coingecko client with caching and session settings."""
        self.api_base_url: str = "https://api.coingecko.com/api/v3"
        self.request_timeout: int = 120

        # Set up a requests session with retries.
        self.session: requests.Session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        # Define cache directories and files.
        self.cache_dir = "data2"
        os.makedirs(self.cache_dir, exist_ok=True)
        self.coin_cache_file = os.path.join(self.cache_dir, "coin_cache.json")
        self.marketcap_dir = os.path.join(self.cache_dir, "marketcap")
        os.makedirs(self.marketcap_dir, exist_ok=True)
        self.mcp_dir = os.path.join(self.cache_dir, "mcp")
        os.makedirs(self.mcp_dir, exist_ok=True)

        # In-memory caches.
        # coin_cache: maps symbol (upper case) -> {"id": coin_id, "category": category}
        self.coin_cache: Dict[str, Dict[str, Any]] = self._load_coin_cache()
        # history_cache: maps symbol -> DataFrame with market cap history
        self.history_cache: Dict[str, pd.DataFrame] = {}

        # Load the full coins list from CoinGecko (cached for 30 days).
        self.all_coins: List[Dict[str, Any]] = self._load_all_coins()

    def _load_coin_cache(self) -> Dict[str, Dict[str, Any]]:
        """Load previously cached coin metadata from disk.

        Returns:
            A dictionary with coin symbol as key and a dict containing coin
            metadata (e.g., {"id": coin_id, "category": coin_category}) as value.
        """
        if os.path.exists(self.coin_cache_file):
            with open(self.coin_cache_file, "r") as f:
                return json.load(f)
        return {}

    def _save_coin_cache(self) -> None:
        """Save the in-memory coin cache dictionary to disk."""
        with open(self.coin_cache_file, "w") as f:
            json.dump(self.coin_cache, f, indent=4)

    def _load_all_coins(self) -> List[Dict[str, Any]]:
        """Load the full list of coins from CoinGecko or from local cache.

        Returns:
            A list of dictionaries, each representing one coin with keys
            such as "id" and "symbol".
        """
        coins_file = os.path.join(self.cache_dir, "coins_list.json")
        if os.path.exists(coins_file):
            mtime = os.path.getmtime(coins_file)
            # Use the cached coin list if it is less than 30 days old.
            if dt.datetime.utcnow().timestamp() - mtime < 30 * 24 * 3600:
                with open(coins_file, "r") as f:
                    return json.load(f)
        # Otherwise, fetch a fresh list
        url = f"{self.api_base_url}/coins/list"
        try:
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
            coins = response.json()
            with open(coins_file, "w") as f:
                json.dump(coins, f, indent=4)
            return coins
        except Exception as e:
            logger.error(f"Failed to load coins list: {e}")
            return []

    def get_coin_id(self, symbol: str) -> Optional[str]:
        """Retrieve the CoinGecko coin ID for a given symbol.

        Args:
            symbol: The coin symbol (case-insensitive).

        Returns:
            The CoinGecko coin ID if found, otherwise None.
        """
        symbol = symbol.upper()
        if symbol in self.coin_cache and "id" in self.coin_cache[symbol]:
            return self.coin_cache[symbol]["id"]

        for coin in self.all_coins:
            if coin["symbol"].upper() == symbol:
                self.coin_cache.setdefault(symbol, {})["id"] = coin["id"]
                self._save_coin_cache()
                return coin["id"]

        # Cache the miss as None to avoid repeated API calls for unknown symbols.
        self.coin_cache[symbol] = {"id": None}
        self._save_coin_cache()
        return None

    def _fetch_coin_info(self, coin_id: str) -> Optional[Dict[str, Any]]:
        """Fetch full coin details for a given CoinGecko coin ID.

        Args:
            coin_id: The CoinGecko coin ID.

        Returns:
            A dictionary containing the coin's info, or None if the request fails.
        """
        url = f"{self.api_base_url}/coins/{coin_id}"
        try:
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch coin info for {coin_id}: {e}")
            return None

    def _extract_category(self, coin_data: Optional[Dict[str, Any]]) -> str:
        """Identify a coin's category based on CoinGecko's 'categories' field.

        Args:
            coin_data: A dictionary containing the coin info from CoinGecko.

        Returns:
            A string representing the category, e.g. "stablecoin", "ETH", "BTC",
            "BRIDGET-XXX", or an empty string if no recognized category is found.
        """
        if not coin_data:
            return ""
        categories = coin_data.get("categories", [])
        stable = {"bridged stablecoins", "stablecoins"}
        eth = {"liquid staked eth", "bridged weth", "bridged wsteth"}
        btc = {"bridged wbtc"}
        bridged = {"bridged-tokens", "wrapped-tokens"}

        for cat in categories:
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

    def get_coin_category(self, symbol: str) -> str:
        """Get a coin's category from cache or from live CoinGecko data.

        Args:
            symbol: The coin symbol (case-insensitive).

        Returns:
            A string representing the category, such as "stablecoin", "ETH",
            "BTC", "BRIDGET-XXX", or "" if the category is unknown.
        """
        symbol = symbol.upper()
        if symbol in self.coin_cache and "category" in self.coin_cache[symbol]:
            return self.coin_cache[symbol]["category"]

        coin_id = self.get_coin_id(symbol)
        if not coin_id:
            self.coin_cache.setdefault(symbol, {})["category"] = ""
            self._save_coin_cache()
            return ""

        # Directly label ETH/BTC instead of fetching extra info.
        if symbol in {"ETH", "BTC"}:
            category = symbol
        else:
            coin_data = self._fetch_coin_info(coin_id)
            category = self._extract_category(coin_data)

        self.coin_cache[symbol]["category"] = category
        self._save_coin_cache()
        return category

    def get_top_250_coins(self) -> List[Dict[str, Any]]:
        """Fetch the top 250 coins by market cap, excluding certain categories.

        Categories excluded: stablecoin, ETH, BTC, or BRIDGET-XXX.

        Returns:
            A list of dictionaries, each containing "symbol", "id", and
            "market_cap" for the valid coins. The result is cached monthly.
        """
        now = dt.datetime.utcnow()
        cache_file = os.path.join(
            self.marketcap_dir, f"marketcap_{now.year}_{now.month:02d}.json"
        )

        if os.path.exists(cache_file):
            logger.info(f"Loading cached market cap data from {cache_file}")
            with open(cache_file, "r") as f:
                return json.load(f)

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
                logger.info(f"Processing {symbol}")

                if symbol in seen_symbols:
                    continue

                if symbol not in self.coin_cache:
                    self.coin_cache[symbol] = {"id": coin["id"]}
                    self._save_coin_cache()

                category = self.get_coin_category(symbol)
                if category in {"stablecoin", "ETH", "BTC", "BRIDGET-XXX"}:
                    continue

                valid_coins.append({
                    "symbol": symbol,
                    "id": coin["id"],
                    "market_cap": coin.get("market_cap", 0)
                })
                seen_symbols.add(symbol)

            with open(cache_file, "w") as f:
                json.dump(valid_coins, f, indent=4)

            logger.info(f"Saved market cap data to {cache_file}")
            return valid_coins

        except Exception as e:
            logger.error(f"Error fetching top coins: {e}")
            return []

    def get_marketcap_history(self, symbol: str) -> pd.DataFrame:
        """Fetch or update the historical market cap data for a given symbol.

        The method checks the local CSV cache and only fetches data from
        CoinGecko if newer data is available or if the local file is empty.

        Args:
            symbol: The coin symbol (case-insensitive).

        Returns:
            A pandas DataFrame containing the market cap history for the symbol,
            indexed by date with columns ["market_cap", "price"].
        """
        symbol = symbol.upper()
        coin_id = self.get_coin_id(symbol)
        if not coin_id:
            logger.error(f"No coin id found for symbol {symbol}")
            return pd.DataFrame()

        csv_file = os.path.join(self.mcp_dir, f"{symbol.lower()}.csv")
        # Load previously cached DataFrame if available
        if symbol in self.history_cache:
            df = self.history_cache[symbol]
        elif os.path.exists(csv_file):
            df = pd.read_csv(csv_file)
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)
            elif "Unnamed: 0" in df.columns:
                df.rename(columns={"Unnamed: 0": "date"}, inplace=True)
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)
        else:
            df = pd.DataFrame()

        if not df.empty:
            last_date = df.index.max().to_pydatetime()
        else:
            # Default start date if no local history
            last_date = dt.datetime(2024, 4, 4)
        now = dt.datetime.utcnow()

        delta_hours = (now - last_date).total_seconds() / 3600
        start_timestamp = int(last_date.timestamp())
        now_timestamp = int(now.timestamp())
        delta_months = delta_hours / (24 * 31)

        # If data is fresh within 24h, do not fetch again.
        if delta_hours < 24:
            return df
        # CoinGecko free tier typically only gives 1 year of data for /market_chart/range
        elif delta_months > 12:
            start_timestamp = now_timestamp - 12 * 30 * 24 * 3600

        try:
            params = {
                "vs_currency": "usd",
                "from": start_timestamp,
                "to": now_timestamp
            }
            url = f"{self.api_base_url}/coins/{coin_id}/market_chart/range"
            response = self.session.get(url, params=params, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()

            market_caps = data.get("market_caps", [])
            prices = data.get("prices", [])
            if market_caps and prices:
                df_caps = pd.DataFrame(market_caps, columns=["timestamp", "market_cap"])
                df_prices = pd.DataFrame(prices, columns=["timestamp", "price"])

                # Convert timestamp to datetime
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

                # Reduce to one entry per day by taking the data point closest to midnight.
                new_df.reset_index(inplace=True)
                new_df["day"] = new_df["date"].dt.normalize()
                new_df["time_diff"] = (new_df["date"] - new_df["day"]).abs()
                selected = new_df.loc[new_df.groupby("day")["time_diff"].idxmin()]
                selected["date"] = selected["day"]
                selected.drop(columns=["day", "time_diff"], inplace=True)
                selected.set_index("date", inplace=True)
                new_df = selected

                # Merge with existing data.
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

        # Save and cache result
        df.to_csv(csv_file, date_format="%Y-%m-%d %H:%M:%S")
        self.history_cache[symbol] = df
        return df

    def get_all_marketcap_histories(self, symbols: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """Retrieve the market cap history for multiple symbols.

        If no symbols are specified, retrieves the top-250 coin symbols.

        Args:
            symbols: An optional list of coin symbols.

        Returns:
            A dictionary mapping each symbol to its market cap history DataFrame.
        """
        histories = {}
        if symbols is None:
            top_coins = self.get_top_250_coins()
            symbols = [coin["symbol"] for coin in top_coins]

        for symbol in symbols:
            histories[symbol] = self.get_marketcap_history(symbol)
        return histories


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    cg = Coingecko()
    for sym in ['USDT', 'ADA', 'BUSD', 'ETH', 'WETH']:
        logger.info(f"{sym} category: {cg.get_coin_category(sym)}")

    top_coins = cg.get_top_250_coins()
    logger.info(f"Top Coins: {top_coins}")

    histories = cg.get_all_marketcap_histories()
    for sym, df in histories.items():
        logger.info(f"Market cap history for {sym}: {len(df)} records")


if __name__ == "__main__":
    main()
