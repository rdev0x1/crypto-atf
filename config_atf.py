import sys
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)


class CodeAct(Enum):
    BUY_PER = 1
    BUY_DIP = 2
    SELL_TP = 3
    SELL_SL = 4
    NONE = 0


@dataclass
class ConfigBuyPER:
    """Configuration for periodic buys.

    Attributes:
        rsi_min_thresh: Minimum RSI threshold for periodic buys.
        rsi_max_thresh: Maximum RSI threshold for periodic buys.
        cool: Cooldown period in days between buys.
        min_cash_amount: Minimum cash to consider buying.
        enable: Whether periodic buys are enabled.
        graph_code: Associated action code for graphing or logging.
        graph_color: Hex color used for representing this action in graphs.
    """
    rsi_min_thresh: float = 60.0
    rsi_max_thresh: float = 76.5
    cool: int = 7
    min_cash_amount: float = 5.0
    enable: bool = True
    graph_code: CodeAct = CodeAct.BUY_PER
    graph_color: str = "#0000FF"


@dataclass
class ConfigBuyDIP:
    """Configuration for buying on dips.

    Attributes:
        rsi_min_thresh: As low as possible (ie: 0).
        rsi_max_thresh: Maximum RSI threshold to consider as a dip.
        cool: Cooldown period in days between buys.
        share_reserve_invested: Fraction of reserve capital to invest on dips.
        share_vault_invested: Fraction of vault capital to invest on dips.
        enable: Whether dip buys are enabled.
        graph_code: Associated action code for graphing or logging.
        graph_color: Hex color used for representing this action in graphs.
    """
    rsi_min_thresh: float = 0.0
    rsi_max_thresh: float = 30.0
    cool: int = 5
    share_reserve_invested: float = 0.33
    share_vault_invested: float = 0.2
    enable: bool = True
    graph_code: CodeAct = CodeAct.BUY_DIP
    graph_color: str = "#00FF00"


@dataclass
class ConfigSellTP:
    """Configuration for take-profit (TP) selling.

    Attributes:
        rsi_min_thresh: Minimum RSI threshold for selling.
        rsi_max_thresh: As high as possible. (ie: no limit).
        cool: Cooldown period in days between sells.
        tp_share: Portion of the asset to sell each time TP is reached.
        enable: Whether take-profit selling is enabled.
        graph_code: Associated action code for graphing or logging.
        graph_color: Hex color used for representing this action in graphs.
    """
    rsi_min_thresh: float = 76.5
    rsi_max_thresh: float = sys.float_info.max
    cool: int = 7
    tp_share: float = 0.1
    enable: bool = True
    graph_code: CodeAct = CodeAct.SELL_TP
    graph_color: str = "#FF0000"


@dataclass
class ConfigSellSL:
    """Configuration for stop-loss (SL) selling.

    Attributes:
        rsi_min_thresh: Not used.
        rsi_max_thresh: Not used.
        cool: Cooldown period in days between sells.
        sl_share: Fraction of the asset to sell if stop-loss triggers.
        win_sl: Window size for SL calculation.
        tresh: Threshold for triggering stop-loss.
        enable: Whether stop-loss selling is enabled.
        graph_code: Associated action code for graphing or logging.
        graph_color: Hex color used for representing this action in graphs.
    """
    rsi_min_thresh: float = 0.0
    rsi_max_thresh: float = 0.0
    cool: int = 7
    sl_share: float = 1.0
    win_sl: int = 5
    tresh: float = 0.2
    enable: bool = True
    graph_code: CodeAct = CodeAct.SELL_SL
    graph_color: str = "#FF00FF"


@dataclass
class ConfigFunding:
    """Configuration for funding and DCA (Dollar-Cost Averaging).

    Attributes:
        initial_fund: Initial amount of funding available.
        dca_enable: Whether automatic DCA is enabled.
        dca_fund: The amount of additional funding added at each DCA interval.
        cool: The DCA period in days.
    """
    initial_fund: float = 100.0
    dca_enable: bool = True
    dca_fund: float = 10.0
    cool: int = 30


class ConfigATF:
    """Main configuration container.

    Attributes:
        underlying_symbol: Symbol of the base asset (e.g., 'USDT').
        nb_assets: Number of assets tracked or allocated for trading.
    """

    underlying_symbol: str = "USDT"
    nb_assets: int = 10

    def __init__(self) -> None:
        """Initialize the ConfigATF with default sub-configuration classes."""
        self.buy_per: ConfigBuyPER = ConfigBuyPER()
        self.buy_dip: ConfigBuyDIP = ConfigBuyDIP()
        self.sell_tp: ConfigSellTP = ConfigSellTP()
        self.sell_sl: ConfigSellSL = ConfigSellSL()
        self.funding: ConfigFunding = ConfigFunding()


def main():
    cfg = ConfigATF()
    logger.info(f"Periodic Buy Config: {cfg.buy_per}")
    logger.info(f"Buy on Dip Config: {cfg.buy_dip}")
    logger.info(f"Take Profit Config: {cfg.sell_tp}")
    logger.info(f"Stop Loss Config: {cfg.sell_sl}")
    logger.info(f"Funding Config: {cfg.funding}")


if __name__ == "__main__":
    main()
