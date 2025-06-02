import pandas as pd
import mplfinance as mpf
from typing import Optional
from classes.Logger import logger
import os
from typing import List, Any
import matplotlib.pyplot as plt
from typing import Set, Tuple
from matplotlib import patches
from classes.indicators.config import DataContext

class ChartPlotter:
    @staticmethod
    def plot_ohlc(
        df: pd.DataFrame,
        title: str,
        ctx: DataContext,
        datasource: str,
        show_volume: bool = True,
        chart_type: str = "candle",
        output_base: str = "output",
        output_subdir: str = "misc",
        legend_entries: Set[Tuple[str, str]] = None,
        overlays: Optional[List[Any]] = None
    ):
        """
        Plots OHLC data using mplfinance, scoped to a given DataContext.
        """
        try:
            ctx.validate()
            start = pd.to_datetime(ctx.start).tz_localize("UTC")
            end = pd.to_datetime(ctx.end).tz_localize("UTC")

            logger.debug("Index sample: %s → %s", df.index.min(), df.index.max())

            if df is None or df.empty:
                logger.warning("No data to plot for given symbol and date range.")
                raise ValueError("Cannot plot: DataFrame is empty or None.")

            required_columns = {'timestamp', 'open', 'high', 'low', 'close'}
            if not required_columns.issubset(df.columns):
                raise ValueError(f"DataFrame must contain columns: {required_columns}")

            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            df.index = pd.to_datetime(df.index)

            logger.debug("Filtering for %s → %s", start, end)
            df = df[(df.index >= start) & (df.index <= end)]

            if df.empty:
                raise ValueError(f"No data to plot after filtering from {start} to {end}.")

            output_dir = os.path.join(output_base, output_subdir)
            os.makedirs(output_dir, exist_ok=True)

            file_name = f"chart_{datasource}_{ctx.symbol}_{ctx.interval}_{start.strftime('%Y%m%d')}_to_{end.strftime('%Y%m%d')}.png"
            file_path = os.path.join(output_dir, file_name)

            fig_width = min(10 + len(df.index) * 0.03, 30)
            figsize = (fig_width, 6)

            fig, axes = mpf.plot(
                df,
                type=chart_type,
                volume=show_volume and "volume" in df.columns,
                title=title,
                style="yahoo",
                addplot=overlays if overlays else [],
                returnfig=True,
                figsize=figsize
            )

            if legend_entries:
                handles = [
                    patches.Patch(color=color, label=label)
                    for label, color in sorted(legend_entries)
                ]
                axes[0].legend(handles=handles, loc="upper left", fontsize=8)

            fig.savefig(file_path, dpi=300, bbox_inches="tight")
            logger.info("Chart saved to %s", file_path)

        except IndexError:
            logger.warning("IndexError: No data points in filtered date range (%s to %s)", ctx.start, ctx.end)
            raise ValueError(f"No data available in the date range {ctx.start} to {ctx.end}.")

        except Exception as e:
            logger.exception("Charting failed: %s", str(e))