import sys
from enum import Enum
from dataclasses import dataclass, field

class CodeAct(Enum):
    BUY_PER = 1
    BUY_DIP = 2
    SELL_TP = 3
    SELL_SL = 4
    NONE = 0

@dataclass
class ConfigSellSL:
    """
    Configuration for stop loss (SL) selling.
    """
    rsi_min_thresh: float = 0.0
    rsi_max_thresh: float = 0.0  # Not used since SL is triggered differently
    cool: int = 7  # Cooldown period in days
    sl_share: float = 1.0  # Sell entire share on SL trigger
    win_sl: int = 5  # Window size for SL calculation
    tresh: float = 0.2  # Threshold for triggering SL
    enable: bool = True
    graph_code: CodeAct = CodeAct.SELL_SL
    graph_color: str = "#FF00FF"

class ConfigATF:
    """
    Main configuration container that aggregates all sub-configurations.
    The data_dir is adjusted based on whether the simulation mode is active.
    """
    def __init__(self) -> None:
        self.set_simulation(simulation=False)
        self.sell_sl: ConfigSellSL = ConfigSellSL()

    def set_simulation(self, simulation=True) -> None:
        base_dir = "./data_sim"
        self.data_dir: str = f"{base_dir}/"

config = ConfigATF()


if __name__ == "__main__":
    config = ConfigATF()
    config.set_simulation()
    print("Data directory:", config.data_dir)
