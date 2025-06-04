############################################################
# vwap.py – session‑reset VWAP with volatility bands
############################################################

from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Literal, Tuple

from classes.indicators.BaseIndicator import BaseIndicator

ARTIFACT_ROOT = Path("artifacts/vwap")


class VWAPIndicator(BaseIndicator):
    """
    Session‑reset VWAP with ±kσ bands, for any session timeframe.

    Parameters
    ----------
    df : DataFrame
        OHLCV data indexed by timestamp (UTC).
    session_tf : Literal['D','M','W','H']
        Period code for VWAP reset: 'D'=daily, 'M'=monthly, etc.
    band_k : float
        Number of standard deviations for upper/lower bands.
    """

    NAME = "vwap"

    def __init__(
        self,
        df: pd.DataFrame,
        session_tf: Literal['D', 'W', 'M'] = 'D',
        band_k: float = 2.0,
    ):
        super().__init__(df)
        self.session_tf = session_tf
        self.band_k = band_k

    def compute(self) -> pd.DataFrame:
        df = self.df.copy()
        # Typical price and safe volume
        df['tp'] = (df['High'] + df['Low'] + df['Close']) / 3
        df['vol'] = df['Volume'].clip(lower=1e-9)

        # Identify session groups by period
        grp = df.index.to_period(self.session_tf)

        # Cumulative numerator and denominator per session
        df['cum_tp_vol'] = (
            df.groupby(grp)
              .apply(lambda g: (g.tp * g.vol).cumsum())
              .reset_index(level=0, drop=True)
        )
        df['cum_vol'] = df.groupby(grp)['vol'].cumsum()
        # For volatility band: E[X^2] term
        df['cum_tp2_vol'] = (
            df.groupby(grp)
              .apply(lambda g: (g.tp ** 2 * g.vol).cumsum())
              .reset_index(level=0, drop=True)
        )

        # VWAP and vwap_std per session
        vwap = df['cum_tp_vol'] / df['cum_vol']
        var = df['cum_tp2_vol'] / df['cum_vol'] - vwap**2
        vwap_std = np.sqrt(var.clip(lower=0))

        df['VWAP'] = vwap
        df['VWAP_STD'] = vwap_std
        self.result = df[['VWAP', 'VWAP_STD']]

        # Score = last deviation in ATR units
        atr14 = (df['High'] - df['Low']).rolling(14).mean()
        last_dev = df['Close'].iloc[-1] - vwap.iloc[-1]
        self.score = float(last_dev / atr14.iloc[-1]) if atr14.iloc[-1] > 0 else 0.0
        return self.result

    def plot(self) -> Path:
        if self.result is None:
            self.compute()
        fig, ax = self._init_price_ax(
            self.df,
            f"VWAP {self.session_tf} ±{self.band_k}σ"
        )
        idx = self.result.index
        ax.plot(idx, self.result['VWAP'], label='VWAP', color='yellow')
        ax.plot(
            idx,
            self.result['VWAP'] + self.band_k * self.result['VWAP_STD'],
            linestyle='--', label=f'+{self.band_k}σ', alpha=0.8
        )
        ax.plot(
            idx,
            self.result['VWAP'] - self.band_k * self.result['VWAP_STD'],
            linestyle='--', label=f'-{self.band_k}σ', alpha=0.8
        )
        ax.legend(facecolor='black', edgecolor='white', fontsize=8)
        path = ARTIFACT_ROOT / f"vwap_{self.session_tf}.png"
        return self._save_fig(fig, path.name)
    
    def get_vwap(self, timestamp: pd.Timestamp) -> Tuple[float, float]:
        """
        Return (VWAP, VWAP_STD) at the given timestamp.
        """
        if self.result is None:
            self.compute()
        # self.result is a DataFrame with columns ['VWAP','VWAP_STD']
        row = self.result.loc[timestamp]
        return float(row['VWAP']), float(row['VWAP_STD'])