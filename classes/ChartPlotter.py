import matplotlib.pyplot as plt
import pandas as pd
import os

from classes.Logger import logger

class ChartPlotter:
    def __init__(self, df, pivots):
        self.df = df
        self.pivots = pivots

    def plot_trendlines(self, trendlines_by_threshold, filename='trendlines_regression.png', subdirectory='artifacts/trendlines/'):
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(12, 6))

        ax.plot(self.df.index, self.df['Close'], label="Closing Price", color="cyan", alpha=0.6)

        colors = ['red', 'green', 'blue', 'orange', 'purple']
        unique_labels = set()

        for threshold, trendlines in trendlines_by_threshold.items():
            logger.debug(f"Plotting trendlines with {threshold} points, found {len(trendlines)} trendlines")

            color = colors[(threshold - 2) % len(colors)]
            for tl in trendlines:
                logger.debug(f"Trendline: {tl.start_date} to {tl.end_date}, R²={tl.r_squared:.2f}, Score={tl.score:.2f}, Length={tl.length} days, Points={len(tl.points)}, Violations={tl.violations}, Violation Ratio={tl.violation_ratio:.2f}")
                
                x_start = pd.Timestamp(tl.start_date).timestamp()
                x_end = pd.Timestamp(tl.end_date).timestamp()
                y_start = tl.slope * x_start + tl.intercept
                y_end = tl.slope * x_end + tl.intercept

                label = f'Trendline (Points={threshold}, R²={tl.r_squared:.2f})'
                if label not in unique_labels:
                    ax.plot([tl.start_date, tl.end_date], [y_start, y_end], color=color, linestyle='--', alpha=0.8, label=label)
                    unique_labels.add(label)
                else:
                    ax.plot([tl.start_date, tl.end_date], [y_start, y_end], color=color, linestyle='--', alpha=0.8)

                x_pts = [pd.Timestamp(d).to_pydatetime() for d, _ in tl.points]
                y_pts = [v for _, v in tl.points]
                ax.scatter(x_pts, y_pts, color=color, marker='o', s=50, alpha=0.8)

        ax.set_title("Price Action with Ranked Trendlines", color='white', size=14)
        ax.set_xlabel("Date", color='white')
        ax.set_ylabel("Price", color='white')
        ax.legend(facecolor='black', edgecolor='white', fontsize=8, loc='upper left')
        ax.grid(alpha=0.2, color='gray')

        os.makedirs(subdirectory, exist_ok=True)
        path = os.path.join(subdirectory, filename)
        plt.savefig(path, dpi=300, bbox_inches='tight', facecolor='black', edgecolor='none')
        logger.info(f"Plot saved to {path}")
        plt.close()

    def plot_levels(self, lookbacks, filename='levels_plot.png', subdirectory='artifacts/levels/', min_price_distance=1.0):
        """
        Plot horizontal levels (rays) starting from significant pivot points for multiple lookbacks.
        
        Args:
            lookbacks: Dictionary of lookback periods with their pivot points
            filename: Name of the output file
            subdirectory: Directory to save the plot
            min_price_distance: Minimum price distance between levels
        """
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot the closing price
        ax.plot(self.df.index, self.df['Close'], label="Closing Price", color="cyan", alpha=0.6)

        # Define colors for each lookback value
        colors = ['red', 'green', 'blue', 'orange', 'purple']

        # Collect all pivot points with their prices and types
        all_pivots = []
        for lookback, (pivots_high, pivots_low) in lookbacks.items():
            for date, value in pivots_high:
                all_pivots.append((date, value, lookback, True))  # True for high pivot
            for date, value in pivots_low:
                all_pivots.append((date, value, lookback, False))  # False for low pivot

        # Sort pivots by price
        all_pivots.sort(key=lambda x: x[1])

        # Filter pivots that are too close in price
        filtered_pivots = []
        if all_pivots:
            filtered_pivots.append(all_pivots[0])
            for date, price, lookback, is_high in all_pivots[1:]:
                if abs(price - filtered_pivots[-1][1]) >= min_price_distance:
                    filtered_pivots.append((date, price, lookback, is_high))

        logger.info(f"Found {len(filtered_pivots)} distinct levels after filtering")

        # Track unique labels for the legend
        unique_labels = set()

        # Plot filtered pivot points and their levels
        for date, price, lookback, is_high in filtered_pivots:
            color = colors[(lookback - min(lookbacks.keys())) % len(colors)]
            
            marker = '^' if is_high else 'v'
            label = f'{"High" if is_high else "Low"} (Lookback={lookback})'
            
            if label not in unique_labels:
                ax.scatter(date, price, color=color, marker=marker, s=100, label=label)
                unique_labels.add(label)
            else:
                ax.scatter(date, price, color=color, marker=marker, s=100)

            # Plot horizontal ray (level)
            ax.axhline(y=price, xmin=0, xmax=1, color=color, linestyle='--', alpha=0.4)

        # Customize plot
        ax.set_title("Price Action with Filtered Pivot Levels", color='white', size=14)
        ax.set_xlabel("Date", color='white')
        ax.set_ylabel("Price", color='white')
        ax.legend(facecolor='black', edgecolor='white', fontsize=8, loc='upper left')
        ax.grid(alpha=0.2, color='gray')

        # Save the plot
        os.makedirs(subdirectory, exist_ok=True)
        path = os.path.join(subdirectory, filename)
        plt.savefig(path, dpi=300, bbox_inches='tight', facecolor='black', edgecolor='none')
        logger.info(f"Plot saved to {path}")
        plt.close()
