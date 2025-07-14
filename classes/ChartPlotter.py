import pandas as pd
import mplfinance as mpf
from typing import Optional, List, Any, Set, Tuple
from classes.Logger import logger
import os
import matplotlib.pyplot as plt
from matplotlib import patches
from classes.indicators.config import DataContext
from classes.OverlayRegistry import get_overlay_handler
import classes.OverlayHandlers
import numpy as np


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
        try:
            logger.info(
                "Starting plot_ohlc for symbol=%s, interval=%s, chart_type=%s",
                ctx.symbol, ctx.interval, chart_type
            )
            ctx.validate()
            start, end = ChartPlotter._get_plot_range(ctx)
            df = ChartPlotter._prepare_dataframe(df)
            ChartPlotter._log_index_info(df)

            df = ChartPlotter._filter_dataframe(df, start, end)
            ChartPlotter._ensure_not_empty(df, start, end)

            file_path = ChartPlotter._get_output_path(
                output_base, output_subdir, ctx, file_name, start, end
            )

            figsize = ChartPlotter._get_figsize(df)
            logger.debug("Figure size set to: %s", figsize)

            addplot_specs, other_specs = ChartPlotter._split_overlays(overlays)
            logger.debug("Addplot overlays: %d, Other overlays: %d", len(addplot_specs), len(other_specs))

            addplot_specs = ChartPlotter._clean_addplots(addplot_specs)

            ChartPlotter._log_addplot_details(addplot_specs)

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
            logger.info("mplfinance plot created.")

            price_ax = axes[0]
            ChartPlotter._dispatch_overlays(df, price_ax, other_specs)

            logger.debug("Axis limits after overlays: xlim=%s, ylim=%s", price_ax.get_xlim(), price_ax.get_ylim())

            if legend_entries:
                ChartPlotter._add_legend(axes[0], legend_entries)

            fig.savefig(file_path, dpi=300, bbox_inches="tight")
            logger.info("Chart saved to %s", file_path)

        except IndexError:
            logger.warning("IndexError: No data points in filtered date range (%s to %s)", ctx.start, ctx.end)
            raise ValueError(f"No data available in the date range {ctx.start} to {ctx.end}.")

        except Exception as e:
            logger.exception("Charting failed: %s", str(e))

    @staticmethod
    def _get_plot_range(ctx):
        start = pd.to_datetime(ctx.start).tz_localize("UTC")
        end = pd.to_datetime(ctx.end).tz_localize("UTC")
        return start, end

    @staticmethod
    def _prepare_dataframe(df):
        if df is None or df.empty:
            logger.warning("No data to plot for given symbol and date range.")
            raise ValueError("Cannot plot: DataFrame is empty or None.")

        required_columns = {'timestamp', 'open', 'high', 'low', 'close'}
        if not required_columns.issubset(df.columns):
            logger.error("Missing required columns. Required: %s, Found: %s", required_columns, df.columns)
            raise ValueError(f"DataFrame must contain columns: {required_columns}, but found: {df.columns}")

        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df.index = pd.to_datetime(df.index)
        return df

    @staticmethod
    def _log_index_info(df):
        logger.debug("DataFrame index type: %s", type(df.index))
        logger.debug("DataFrame index sample: %s", df.index[:5])

    @staticmethod
    def _filter_dataframe(df, start, end):
        logger.debug("Filtering DataFrame for range: %s → %s", start, end)
        return df[(df.index >= start) & (df.index <= end)]

    @staticmethod
    def _ensure_not_empty(df, start, end):
        if df.empty:
            logger.warning("No data to plot after filtering from %s to %s.", start, end)
            raise ValueError(f"No data to plot after filtering from {start} to {end}.")

    @staticmethod
    def _get_output_path(output_base, output_subdir, ctx, file_name, start, end):
        output_dir = os.path.join(output_base, output_subdir)
        os.makedirs(output_dir, exist_ok=True)
        logger.debug("Output directory ensured: %s", output_dir)
        start_end_time = f"{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}"
        file_name = f"{file_name}_{start_end_time}.png" if file_name else f"{ctx.symbol}_{ctx.interval}_{start_end_time}.png"
        file_path = os.path.join(output_dir, file_name)
        logger.info("Output file path: %s", file_path)
        return file_path

    @staticmethod
    def _get_figsize(df):
        fig_width = min(10 + len(df.index) * 0.03, 30)
        return (fig_width, 6)

    @staticmethod
    def _clean_addplots(addplot_specs):
        cleaned_addplots = []
        for i, spec in enumerate(addplot_specs):
            label = spec.get("label", f"Overlay #{i}")
            if spec.get("scatter", False):
                x, y = spec.get("x"), spec.get("y")
                if not x or not y:
                    logger.warning("Skipping scatter (%s) with missing or empty x/y.", label)
                    continue
            else:
                data = spec.get("data")
                if data is None or (isinstance(data, pd.Series) and (data.empty or data.isna().all())):
                    logger.warning("Skipping line (%s) with missing or empty data.", label)
                    continue
                if isinstance(data, (list, np.ndarray)) and (len(data) == 0 or all(pd.isna(v) for v in data)):
                    logger.warning("Skipping line (%s): empty or all-NaN array.", label)
                    continue
            cleaned_addplots.append(spec)
        logger.info("Final cleaned addplot overlays: %d", len(cleaned_addplots))
        return cleaned_addplots

    @staticmethod
    def _log_addplot_details(addplot_specs):
        for i, spec in enumerate(addplot_specs):
            label = spec.get("label", f"Overlay #{i}")
            if not spec.get("scatter", False):
                data = spec.get("data")
                if isinstance(data, pd.Series):
                    logger.debug("Line overlay %s head:\n%s", label, data.head(3))
                    logger.debug(
                        "Line overlay %s stats: len=%d, nulls=%d, isna.all=%s",
                        label, len(data), data.isna().sum(), data.isna().all()
                    )

    @staticmethod
    def _split_overlays(overlays):
        addplot_specs = []
        other_by_kind = {}
        logger.debug("Splitting overlays: total=%d", len(overlays) if overlays else 0)
        for item in overlays or []:
            kind = item.get("kind") if isinstance(item, dict) and "kind" in item else "addplot"
            if kind == "addplot":
                if isinstance(item, dict) and "plot" in item:
                    addplot_specs.append(item["plot"])
                else:
                    addplot_specs.append(item)
            else:
                other_by_kind.setdefault(kind, []).append(item)
        other_specs = list(other_by_kind.items())
        rect_count = len(other_by_kind.get("rect", []))
        logger.debug(
            "Overlay split: addplot=%d, rects=%d, other_kinds=%d",
            len(addplot_specs),
            rect_count,
            len(other_by_kind) - (1 if "rect" in other_by_kind else 0)
        )
        return addplot_specs, other_specs

    @staticmethod
    def _dispatch_overlays(df, price_ax, other_specs):
        logger.debug("Dispatching overlays: %d kinds", len(other_specs))
        for kind, spec_list in other_specs:
            handler = get_overlay_handler(kind)
            if handler:
                logger.debug("Dispatching %d overlays of kind '%s'", len(spec_list), kind)
                handler(df, price_ax, spec_list)
            else:
                logger.warning("No handler found for overlay kind: %s", kind)

    @staticmethod
    def _add_legend(ax, legend_entries):
        handles = [
            patches.Patch(color=color, label=label)
            for label, color in sorted(legend_entries)
        ]
        ax.legend(handles=handles, loc="upper left", fontsize=8)
        logger.debug("Legend added with %d entries.", len(handles))