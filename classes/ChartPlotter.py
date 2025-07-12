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
from itertools import groupby
from classes.OverlayRegistry import get_overlay_handler
import classes.OverlayHandlers


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
        overlays: Optional[List[Any]] = None,
        file_name: Optional[str] = None
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
                raise ValueError(f"DataFrame must contain columns: {required_columns}, but found: {df.columns}")

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

            start_end_time= f"{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}"
            file_name = f"{file_name}_{start_end_time}.png" if file_name else f"{ctx.symbol}_{ctx.interval}_{start_end_time}.png"
            file_path = os.path.join(output_dir, file_name)

            fig_width = min(10 + len(df.index) * 0.03, 30)
            figsize = (fig_width, 6)

            addplot_specs, other_specs = ChartPlotter._split_overlays(overlays)

            fig, axes = mpf.plot(
                df,
                type=chart_type,
                volume=show_volume and "volume" in df.columns,
                title=title,
                style="yahoo",
                addplot=addplot_specs,
                returnfig=True,
                figsize=figsize
            )

            price_ax = axes[0]
            ChartPlotter._dispatch_overlays(df, price_ax, other_specs)

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

    @staticmethod
    def _split_overlays(overlays):
        """
        Splits the overlays into:
          - addplot_specs: list of mplfinance addplot objects
          - other_specs:   list of (kind, specs_list) for the rest
        """
        addplot_specs = []
        other_by_kind = {}

        for item in overlays or []:
            # Determine its kind
            if isinstance(item, dict) and "kind" in item:
                kind = item["kind"]
            else:
                kind = "addplot"

            if kind == "addplot":
                # Unwrap the real addplot object
                if isinstance(item, dict) and "plot" in item:
                    addplot_specs.append(item["plot"])
                else:
                    # raw mplfinance addplot passed straight through
                    addplot_specs.append(item)
            else:
                # Group the rest by kind
                other_by_kind.setdefault(kind, []).append(item)

        # Turn the dict into a list of tuples
        other_specs = list(other_by_kind.items())
        print("rect specs:", [group for group in other_specs if group[0]=="rect"])

        return addplot_specs, other_specs

    @staticmethod
    def _dispatch_overlays(df, price_ax, other_specs):
        """
        Dispatches overlays to the appropriate handler based on kind.
        """
        for kind, spec_list in other_specs:
            handler = get_overlay_handler(kind)
            if handler:
                handler(df, price_ax, spec_list)
            else:
                logger.warning("No handler found for overlay kind: %s", kind)