import matplotlib.pyplot as plt
import pandas as pd
import os
from typing import Dict, List, Tuple, Any

from classes.Logger import logger

class ChartPlotter:
    """
    A class for plotting stock charts with trendlines and pivot levels.

    Attributes:
        df (pd.DataFrame): DataFrame containing the stock data.
        pivots (Any): Pivot points data used for plotting (format may vary).
    """
    def __init__(self, df: pd.DataFrame, pivots: Any) -> None:
        self.df = df
        self.pivots = pivots

    def _save_plot(self, fig: plt.Figure, subdirectory: str, filename: str) -> None:
        """
        Save the given figure to the specified subdirectory and filename.

        Args:
            fig (plt.Figure): The matplotlib figure to save.
            subdirectory (str): Directory where the plot will be saved.
            filename (str): The name of the output file.
        """
        os.makedirs(subdirectory, exist_ok=True)
        path = os.path.join(subdirectory, filename)
        fig.savefig(path, dpi=300, bbox_inches='tight', facecolor='black', edgecolor='none')
        logger.info(f"Plot saved to {path}")
        plt.close(fig)

    def plot_trendlines(
        self,
        trendlines_by_threshold: Dict[int, List[Any]],
        filename: str = 'trendlines_regression.png',
        subdirectory: str = 'artifacts/trendlines/'
    ) -> None:
        """
        Plot trendlines on a stock chart with the closing price as background.

        Args:
            trendlines_by_threshold (Dict[int, List[Any]]): A dictionary mapping threshold values to lists of trendline objects.
            filename (str): The output filename for the plot.
            subdirectory (str): The directory to save the plot.
        """
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot the closing price
        ax.plot(self.df.index, self.df['Close'], label="Closing Price", color="cyan", alpha=0.6)

        colors = ['red', 'green', 'blue', 'orange', 'purple']
        unique_labels = set()

        # Create a stable color mapping for each threshold
        thresholds_sorted = sorted(trendlines_by_threshold.keys())
        threshold_color_map = {
            t: colors[i % len(colors)] for i, t in enumerate(thresholds_sorted)
        }

        for threshold, trendlines in trendlines_by_threshold.items():
            logger.debug(f"Plotting trendlines with {threshold} points, found {len(trendlines)} trendlines")
            color = threshold_color_map[threshold]

            for tl in trendlines:
                logger.debug(
                    f"Trendline: {tl.start_date} to {tl.end_date}, R²={tl.r_squared:.2f}, "
                    f"Score={tl.score:.2f}, Length={tl.length} days, Points={len(tl.points)}, "
                    f"Violations={tl.violations}, Violation Ratio={tl.violation_ratio:.2f}"
                )
                # Convert start and end dates to timestamps for calculation
                x_start = pd.Timestamp(tl.start_date).timestamp()
                x_end = pd.Timestamp(tl.end_date).timestamp()
                y_start = tl.slope * x_start + tl.intercept
                y_end = tl.slope * x_end + tl.intercept

                label = f'Trendline (Points={threshold}, R²={tl.r_squared:.2f})'
                if label not in unique_labels:
                    ax.plot([tl.start_date, tl.end_date], [y_start, y_end],
                            color=color, linestyle='--', alpha=0.8, label=label)
                    unique_labels.add(label)
                else:
                    ax.plot([tl.start_date, tl.end_date], [y_start, y_end],
                            color=color, linestyle='--', alpha=0.8)

                # Plot the pivot points used for this trendline
                x_pts = [pd.Timestamp(d).to_pydatetime() for d, _ in tl.points]
                y_pts = [v for _, v in tl.points]
                ax.scatter(x_pts, y_pts, color=color, marker='o', s=50, alpha=0.8)

        # Customize chart appearance
        ax.set_title("Price Action with Ranked Trendlines", color='white', size=14)
        ax.set_xlabel("Date", color='white')
        ax.set_ylabel("Price", color='white')
        ax.legend(facecolor='black', edgecolor='white', fontsize=8, loc='upper left')
        ax.grid(alpha=0.2, color='gray')

        self._save_plot(fig, subdirectory, filename)

    def plot_levels(
        self,
        lookbacks: Dict[Any, Tuple[List[Tuple[str, float]], List[Tuple[str, float]]]],
        filename: str = 'levels_plot.png',
        subdirectory: str = 'artifacts/levels/',
        min_price_distance: float = 1.0
    ) -> None:
        """
        Plot pivot level rays from significant pivot points across different lookback periods.
        Each ray starts at the pivot point and extends horizontally to the right.
        Different lookback periods are assigned different colors.

        Args:
            lookbacks (Dict[Any, Tuple[List[Tuple[str, float]], List[Tuple[str, float]]]]):
                A dictionary where each key is a lookback period and its value is a tuple containing two lists:
                one for high pivots and one for low pivots.
            filename (str): Name of the output file.
            subdirectory (str): Directory where the plot will be saved.
            min_price_distance (float): Minimum price difference to consider two levels distinct.
        """
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot the closing price
        ax.plot(self.df.index, self.df['Close'], label="Closing Price", color="cyan", alpha=0.6)

        colors = ['red', 'green', 'blue', 'orange', 'purple']

        # Create a stable color mapping for each lookback
        lookbacks_sorted = sorted(lookbacks.keys())
        lookback_color_map = {
            lb: colors[i % len(colors)] for i, lb in enumerate(lookbacks_sorted)
        }

        # Collect all pivot points with associated lookback and type (high/low)
        all_pivots: List[Tuple[str, float, Any, bool]] = []
        for lookback, (pivots_high, pivots_low) in lookbacks.items():
            for date, value in pivots_high:
                all_pivots.append((date, value, lookback, True))  # True for high pivot
            for date, value in pivots_low:
                all_pivots.append((date, value, lookback, False))  # False for low pivot

        # Sort pivots by price to filter out levels that are too close
        all_pivots.sort(key=lambda x: x[1])
        filtered_pivots: List[Tuple[str, float, Any, bool]] = []
        if all_pivots:
            filtered_pivots.append(all_pivots[0])
            for date, price, lookback, is_high in all_pivots[1:]:
                if abs(price - filtered_pivots[-1][1]) >= min_price_distance:
                    filtered_pivots.append((date, price, lookback, is_high))

        logger.info(f"Found {len(filtered_pivots)} distinct levels after filtering")

        unique_labels = set()
        last_date = self.df.index[-1]  # The rightmost date for the rays
        for date, price, lookback, is_high in filtered_pivots:
            pivot_dt = pd.Timestamp(date)
            color = lookback_color_map[lookback]
            marker = '^' if is_high else 'v'
            label = f'{"High" if is_high else "Low"} (Lookback={lookback})'

            # Plot the pivot point
            if label not in unique_labels:
                ax.scatter(pivot_dt, price, color=color, marker=marker, s=100, label=label)
                unique_labels.add(label)
            else:
                ax.scatter(pivot_dt, price, color=color, marker=marker, s=100)

            # Draw a ray from the pivot point to the last date
            ax.plot([pivot_dt, last_date], [price, price], color=color, linestyle='--', alpha=0.4)

        # Customize chart appearance
        ax.set_title("Price Action with Filtered Pivot Levels", color='white', size=14)
        ax.set_xlabel("Date", color='white')
        ax.set_ylabel("Price", color='white')
        ax.legend(facecolor='black', edgecolor='white', fontsize=8, loc='upper left')
        ax.grid(alpha=0.2, color='gray')

        self._save_plot(fig, subdirectory, filename)
