import json
import os
import datetime as dt
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class Coingecko:
    def __init__(self):
        self.api_base_url = "https://api.coingecko.com/api/v3"
        self.request_timeout = 120
        self.session = requests.Session()

        # Setup retry mechanism for transient errors
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        self.api_coin_url = f"{self.api_base_url}/coins"
        self.api_marketcap_url = f"{self.api_base_url}/coins/markets"
        self.marketcap_dir = "data/marketcap"

        # Cache file now stores coin categories: stablecoin, ETH,BTC arfe
        # categories to ignore for our index
        self.categories_file = "data/categories.json"
        self.categories_cache = self._load_categories_cache()

    def _load_categories_cache(self):
        """Load coin categories from a cache file."""
        if os.path.exists(self.categories_file):
            with open(self.categories_file, "r") as f:
                return json.load(f)
        return {}

    def _save_categories_cache(self):
        """Persist the coin categories cache to a file."""
        os.makedirs(os.path.dirname(self.categories_file), exist_ok=True)
        with open(self.categories_file, "w") as f:
            json.dump(self.categories_cache, f, indent=4)

    def _fetch_coin_info(self, coin_id):
        """
        Retrieve detailed coin information from CoinGecko.
        :param coin_id: The unique CoinGecko ID.
        :return: JSON data or None on failure.
        """
        try:
            url = f"{self.api_coin_url}/{coin_id}"
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching coin data for {coin_id}: {e}")
            return None

    def _get_coingecko_id(self, coin_symbol):
        """
        Map a coin symbol to CoinGecko's unique coin ID.
        :param coin_symbol: e.g. "ADA"
        :return: CoinGecko ID or None.
        """
        try:
            url = f"{self.api_base_url}/coins/list"
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
            coins = response.json()
            for coin in coins:
                if coin["symbol"].upper() == coin_symbol:
                    return coin["id"]
        except requests.RequestException as e:
            print(f"Error fetching CoinGecko ID for {coin_symbol}: {e}")
        return None

    def _get_coin_category_from_info(self, coin_data):
        """
        Determine the category of a coin based on its 'categories' field.
        Recognized stablecoin categories: 'bridged stablecoins', 'stablecoins'
        Recognized ETH-derived category: 'liquid staked eth'
        :return: "stablecoin", "ETH", or "".
        """
        if not coin_data:
            return ""
        categories = coin_data.get("categories", [])
        stable_categories = {"bridged stablecoins", "stablecoins"}
        eth_categories = {"liquid staked eth", "bridged weth", "bridged wsteth"}
        btc_categories = {"bridged wbtc"}
        bridget_categories = {"bridged-tokens", "wrapped-tokens"} # Mpre generic wrapped coins
        for cat in categories:
            cat_lower = cat.lower()
            if cat_lower in stable_categories:
                return "stablecoin"
            if cat_lower in eth_categories:
                return "ETH"
            if cat_lower in btc_categories:
                return "BTC"
            if cat_lower in bridget_categories:
                return "BRIDGET-XXX"
        return ""

    def get_coin_category(self, coin_symbol):
        """
        Retrieve a coin's category.
        Uses the local cache if available; otherwise, fetches details from CoinGecko.
        Returns: "stablecoin", "ETH", or "".
        """
        coin_symbol = coin_symbol.upper()
        if coin_symbol in self.categories_cache:
            return self.categories_cache[coin_symbol]
        coin_id = self._get_coingecko_id(coin_symbol)
        if not coin_id:
            self.categories_cache[coin_symbol] = ""
            self._save_categories_cache()
            return ""
        if coin_symbol == "ETH":
            category = "ETH"
        elif coin_symbol == "BTC":
            category = "BTC"
        else:
            coin_data = self._fetch_coin_info(coin_id)
            category = self._get_coin_category_from_info(coin_data)
        self.categories_cache[coin_symbol] = category
        self._save_categories_cache()
        return category

    def _get_marketcap_filename(self):
        """Generate a filename for storing the current month's market cap data."""
        now = dt.datetime.utcnow()
        filename = f"marketcap_{now.year}_{now.month:02d}.json"
        return os.path.join(self.marketcap_dir, filename)

    def get_coin_per_marketcap(self):
        """
        Fetch the top cryptocurrencies by market cap (from CoinGecko), filtering out:
          - Stablecoins (category "stablecoin")
          - BTC and ETH-related tokens (category "ETH")
        If market cap data for the current month already exists in a file, load it instead of calling the API.
        """
        marketcap_file = self._get_marketcap_filename()
        nb_coins = 200

        # Use cached market cap data if available
        if os.path.exists(marketcap_file):
            with open(marketcap_file, "r") as f:
                print(f"Loading cached market cap data from {marketcap_file}")
                return json.load(f)

        try:
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": nb_coins,  # Get more coins to allow filtering
                "page": 1,
                "sparkline": "false"
            }
            response = self.session.get(self.api_marketcap_url, params=params, timeout=self.request_timeout)
            response.raise_for_status()
            data = response.json()

            print("get coingecko coins ... this may take time. Wait:")
            valid_coins = []
            for coin in data:
                symbol = coin["symbol"].upper()

                print(symbol,  end="")

                # Determine the coin's category using the cache / API
                category = self.get_coin_category(symbol)
                if category == "stablecoin":
                    print(":skip-stablecoin", end=", ")
                    continue
                if symbol == "ETH" or category == "ETH":
                    print(":skip-ETH", end=", ")
                    continue
                if symbol == "BTC" or category == "BTC":
                    print(":skip-BTC", end=", ")
                    continue
                if category == "BRIDGET-XXX":
                    print(":skip-BRIDGE", end=", ")
                    continue

                print("", end=", ")
                valid_coins.append(symbol)
                if len(valid_coins) >= nb_coins:
                    break

            with open(marketcap_file, "w") as f:
                json.dump(valid_coins, f, indent=4)
            print(f"Saved market cap data to {marketcap_file}")
            return valid_coins

        except requests.RequestException as e:
            print(f"Error fetching market cap data: {e}")
            return []


# Example Usage:
if __name__ == "__main__":
    cg = Coingecko()

    # Print coin categories as stored in the cache
    print(f"USDT category: {cg.get_coin_category('USDT')}")
    print(f"ADA category: {cg.get_coin_category('ADA')}")
    print(f"BUSD category: {cg.get_coin_category('BUSD')}")
    print(f"ETH category: {cg.get_coin_category('ETH')}")
    print(f"WETH category: {cg.get_coin_category('WETH')}")

    # Get top 10 altcoins by market cap (excluding stablecoins, BTC, and ETH-derived tokens)
    top_coins = cg.get_coin_per_marketcap()
    print("Top Cryptocurrencies by Market Cap:", top_coins)
