import pandas as pd
from mplfinance.plotting import make_addplot
from matplotlib import patches
from classes.indicators.BaseIndicator import BaseIndicator
from classes.indicators.config import DataContext


class VWAPIndicator(BaseIndicator):
    """
    Computes anchored VWAP and its rolling standard-deviation bands (VWAP ± nσ).
    """
    NAME = "vwap_bands"

    def __init__(
        self,
        df: pd.DataFrame,
        stddev_window: int = 20,
        stddev_multipliers: list[float] = [1.0, 2.0],
        reset_by: str = "D"
    ):
        """
        :param df: OHLCV DataFrame with datetime index.
        :param stddev_window: window size for rolling std of typical price.
        :param stddev_multipliers: list of multipliers for band offsets.
        :param reset_by: 'D' to reset VWAP daily; any other value for cumulative.
        """
        self.df = df.copy()
        self.stddev_window = stddev_window
        self.stddev_multipliers = stddev_multipliers
        self.reset_by = reset_by
        self._compute()

    @classmethod
    def from_context(
        cls,
        provider,
        ctx: DataContext,
        stddev_window: int = 20,
        stddev_multipliers: list[float] = [1.0, 2.0],
        reset_by: str = "D"
    ):
        """
        Instantiate from a DataContext and data provider.
        Raises ValueError if no OHLCV data is returned.
        """
        df = provider.get_ohlcv(ctx)
        if df is None or df.empty:
            raise ValueError(
                f"Missing OHLCV for {ctx.symbol} from {ctx.start} to {ctx.end}"
            )
        return cls(
            df=df,
            stddev_window=stddev_window,
            stddev_multipliers=stddev_multipliers,
            reset_by=reset_by
        )

    def _compute(self):
        """
        Calculate VWAP and rolling standard-deviation bands.
        Adds columns:
          - 'vwap'
          - 'upper_{m}std' and 'lower_{m}std' for each multiplier m.
        """
        # typical price = (high + low + close)/3
        tp = (self.df['high'] + self.df['low'] + self.df['close']) / 3
        pv = tp * self.df['volume']

        # cumulative PV and volume, reset daily if requested
        if self.reset_by == 'D':
            cum_pv = pv.groupby(self.df.index.date).cumsum()
            cum_vol = self.df['volume'].groupby(self.df.index.date).cumsum()
        else:
            cum_pv = pv.cumsum()
            cum_vol = self.df['volume'].cumsum()

        self.df['vwap'] = cum_pv.values / cum_vol.values

        # rolling std of typical price
        tp_std = tp.rolling(window=self.stddev_window, min_periods=1).std()

        # compute bands
        for m in self.stddev_multipliers:
            self.df[f'upper_{int(m)}std'] = self.df['vwap'] + m * tp_std
            self.df[f'lower_{int(m)}std'] = self.df['vwap'] - m * tp_std

    def to_overlays(
        self,
        plot_df: pd.DataFrame,
        vwap_color: str = 'blue',
        band_color: str = 'gray'
    ) -> tuple[list, set]:
        """
        Generate mplfinance overlays for VWAP and its bands.

        :param plot_df: DataFrame to align overlay indices with plot.
        :param vwap_color: color for the VWAP line.
        :param band_color: color for the standard-deviation bands.
        :returns: (overlays, legend_entries)
        """
        overlays = []
        legend_entries: set[tuple[str, str]] = set()

        # VWAP line
        vwap_series = pd.Series(self.df['vwap'].values, index=plot_df.index)
        overlays.append(
            make_addplot(vwap_series, color=vwap_color, linestyle='solid', width=1)
        )
        legend_entries.add(("VWAP", vwap_color))

        # bands
        for m in self.stddev_multipliers:
            upper = pd.Series(
                self.df[f'upper_{int(m)}std'].values,
                index=plot_df.index
            )
            lower = pd.Series(
                self.df[f'lower_{int(m)}std'].values,
                index=plot_df.index
            )
            overlays.append(
                make_addplot(upper, color=band_color, linestyle='dashed', width=0.75)
            )
            overlays.append(
                make_addplot(lower, color=band_color, linestyle='dashed', width=0.75)
            )
            legend_entries.add((f"VWAP + {m}\u03c3", band_color))
            legend_entries.add((f"VWAP - {m}\u03c3", band_color))

        return overlays, legend_entries

    @staticmethod
    def build_legend_handles(legend_entries: set) -> list:
        """
        Convert legend_entries (label, color) tuples into matplotlib Patch handles.
        """
        return [patches.Patch(color=color, label=label) for label, color in sorted(legend_entries)]
