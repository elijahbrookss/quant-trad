import pandas as pd
from typing import List, Dict, Optional

from classes.engines.StrategyEngine import StrategyEngine
from classes.indicators.BaseIndicator import BaseIndicator

class Trade:
    def __init__(self, entry_time, entry_price, direction):
        self.entry_time = entry_time
        self.entry_price = entry_price
        self.direction = direction  # 'long' or 'short'
        self.exit_time: Optional[pd.Timestamp] = None
        self.exit_price: Optional[float] = None
        self.pnl: Optional[float] = None

    def close(self, exit_time, exit_price):
        self.exit_time = exit_time
        self.exit_price = exit_price
        if self.direction == 'long':
            self.pnl = exit_price - self.entry_price
        else:
            self.pnl = self.entry_price - exit_price


class Backtester:
    """
    Walk-forward backtester that uses StrategyEngine signals to simulate trades.

    Parameters
    ----------
    df : pd.DataFrame
        15-minute OHLC data with 'score' and 'direction'.
    engine : StrategyEngine
        Pre-wired strategy engine (with indicators already computed).
    entry_threshold : float
        Minimum score to enter a trade (0–1).
    stop_loss : float
        Stop-loss multiplier (ATR factor, e.g. 1.0 = 1*ATR).
    take_profit : float
        Take-profit multiplier (ATR factor).
    """
    def __init__(
        self,
        df: pd.DataFrame,
        engine: StrategyEngine,
        entry_threshold: float = 0.8,
        stop_loss: float = 1.0,
        take_profit: float = 2.0,
    ) -> None:
        self.df = df.copy()
        self.engine = engine
        self.entry_threshold = entry_threshold
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.trades: List[Trade] = []

    def run(self) -> pd.DataFrame:
        """Simulate trades over the DataFrame."""
        position: Optional[Trade] = None

        # ---- Wilder ATR (True Range w/ Wilder smoothing) ----
        hl          = self.df['High'] - self.df['Low']
        h_cp        = (self.df['High'] - self.df['Close'].shift()).abs()
        l_cp        = (self.df['Low']  - self.df['Close'].shift()).abs()
        tr          = pd.concat([hl, h_cp, l_cp], axis=1).max(axis=1)
        atr_period  = 14        # keep it configurable if you like
        atr         = tr.ewm(alpha=1/atr_period, adjust=False).mean()

        # # Precompute ATR series for stops/targets
        # atr = (self.df['High'] - self.df['Low']).rolling(14).mean()

        for idx, row in self.df.iterrows():
            score = row['score']
            direction = row['direction']
            price = row['Close']
            atr_val = atr.loc[idx]

            # Entry logic
            if position is None and score >= self.entry_threshold:
                position = Trade(idx, price, direction)
                self.trades.append(position)
                # Compute static stops/targets
                sl = self.stop_loss * atr_val
                tp = self.take_profit * atr_val
                if direction == 'long':
                    position.stop_price = price - sl
                    position.target_price = price + tp
                else:
                    position.stop_price = price + sl
                    position.target_price = price - tp

            # Exit logic
            if position is not None and position.exit_time is None:
                low, high = row['Low'], row['High']
                if position.direction == 'long':
                    if low <= position.stop_price:
                        position.close(idx, position.stop_price)
                    elif high >= position.target_price:
                        position.close(idx, position.target_price)
                else:
                    if high >= position.stop_price:
                        position.close(idx, position.stop_price)
                    elif low <= position.target_price:
                        position.close(idx, position.target_price)

        # Build results DataFrame
        records: List[Dict] = []
        for t in self.trades:
            records.append({
                'entry_time': t.entry_time,
                'exit_time': t.exit_time,
                'direction': t.direction,
                'entry_price': t.entry_price,
                'exit_price': t.exit_price,
                'pnl': t.pnl,
            })
        return pd.DataFrame(records)