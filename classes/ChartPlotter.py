import pandas as pd
import mplfinance as mpf
from typing import Optional
from classes.Logger import logger
import os

class ChartPlotter:
    @staticmethod
    def plot_ohlc(
        df: pd.DataFrame,
        title: str,
        symbol: str,
        datasource: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        show_volume: bool = True,
        chart_type: str = "candle",
        output_base: str = "output",
        output_subdir: str = "charts"  # Default folder for full chart output
    ):
        """
        Plots OHLC data using mplfinance.

        Parameters:
        - df: A pandas DataFrame with columns: ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        - title: Chart title
        - start: Optional start date as string ('YYYY-MM-DD')
        - end: Optional end date as string ('YYYY-MM-DD')
        - show_volume: Whether to include volume subplot
        - chart_type: 'candle', 'line', etc.
        """
        try:
            logger.debug("Index sample: %s → %s", df.index.min(), df.index.max())

            if df is None or df.empty:
                logger.warning("No data to plot for given symbol and date range.")
                raise ValueError("Cannot plot: DataFrame is empty or None.")

            if not {'timestamp', 'open', 'high', 'low', 'close'}.issubset(df.columns):
                raise ValueError("DataFrame must contain at least ['timestamp', 'open', 'high', 'low', 'close']")

            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            df.index = pd.to_datetime(df.index) 


            start = pd.to_datetime(start).tz_localize("UTC")  # makes the input string tz-aware
            end = pd.to_datetime(end).tz_localize("UTC")  # makes the input string tz-aware

            logger.debug("Filtering for %s → %s", start, end)

            if start:
                df = df[df.index >= pd.to_datetime(start)]
            if end:
                df = df[df.index <= pd.to_datetime(end)]

            if df.empty:
                raise ValueError(f"No data to plot after filtering from {start} to {end}.")

             # Construct output path
            output_dir = os.path.join(output_base, output_subdir)
            os.makedirs(output_dir, exist_ok=True)

            file_name = f"chart_{datasource}_{symbol}_{start}_to_{end}.png"
            file_path = os.path.join(output_dir, file_name)

            mpf.plot(
                df,
                type=chart_type,
                volume=show_volume and 'volume' in df.columns,
                title=title,
                style='yahoo',
                savefig=file_path
            )

            
        except IndexError:
            logger.warning("IndexError: No data points in filtered date range (%s to %s)", start, end)
            raise ValueError(f"No data available in the date range {start} to {end}.")

        except Exception as e:
            logger.exception("Charting failed: %s", str(e))
            raise RuntimeError(f"Chart plotting failed: {e}")