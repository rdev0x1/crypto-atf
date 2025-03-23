from binance_client import bclient
from bot_atf import BotATF
from stock import Stock
from crypto_stock import CryptoStock


class AltRefIndex(Stock):
    def __init__(self):
        self.nb_assets = 10
        self.load_csv("data/test/data_test.csv")
        self.compute_rsi()

alt = AltRefIndex()
bot = BotATF(start_date="2018-01-01", alt=alt)
bot.draw()
