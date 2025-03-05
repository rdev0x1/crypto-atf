import os
import logging
from binance.client import Client as BinanceClientLib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceClient:
    def __init__(self) -> None:
        self._client: BinanceClientLib | None = None
        self._test_mode: bool = False

    def _get_keys(self, key_name: str) -> tuple[str, str]:
        """
        Retrieve the API key and secret from environment variables.
        Expects environment variables in the format: <key_name>_api and <key_name>_secret.
        """
        api_key = os.environ.get(f"{key_name}_api")
        api_secret = os.environ.get(f"{key_name}_secret")
        if not api_key or not api_secret:
            raise ValueError(f"API key or secret not found for key: {key_name}")
        return api_key, api_secret

    def set_key(self, key_name: str) -> None:
        """
        Initialize the Binance client using the provided key name.
        If the key name ends with '_test', the client is set to test mode.
        """
        self._test_mode = key_name.endswith("_test")
        self._init_client(key_name)

    def _init_client(self, key_name: str) -> None:
        api_key, api_secret = self._get_keys(key_name)
        self._client = BinanceClientLib(api_key, api_secret)
        if self._test_mode:
            # Point to Binance testnet if in test mode.
            self._client.API_URL = "https://testnet.binance.vision/api"

    @property
    def client(self) -> BinanceClientLib:
        """
        Get the underlying Binance client.
        Raises RuntimeError if the client has not been initialized.
        """
        if self._client is None:
            raise RuntimeError("Binance client not initialized. Please call set_key() first.")
        return self._client

    @property
    def is_test_mode(self) -> bool:
        """Return True if the client is in test mode."""
        return self._test_mode


bclient = BinanceClient()


if __name__ == "__main__":
    bclient.set_key("binance_crypto_l3ro")
    client = bclient.client
    logger.info(f"test_mode={bclient.is_test_mode} client={client}")
