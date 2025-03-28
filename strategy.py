import yfinance as yf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import linregress
from datetime import datetime
import os
from collections import Counter


ARTIFACT_PREFIX = 'plotmulti'
SHOW_TRENDLINES = False
TRENDLINE_MIN_SCORE=0

def fetch_stock_data(symbol, start, end):
    print(f"Fetching stock data for symbol: {symbol} from {start} to {end}...")
    df = yf.download(symbol, start=start, end=end, auto_adjust=True)[['Open', 'High', 'Low', 'Close', 'Volume']]
    df = df.tz_localize(None)

    # Ensure the index is a Single index
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    print("Starting data health checks for stock data...")
    # Check if the index has unique values
    is_unique = df.index.is_unique
    print(f"[INFO] Index Unique: {is_unique}")

    # Confirm the type of the index
    index_type = type(df.index)
    print(f"[INFO] Index Type: {index_type}")

    # Count duplicate entries in the index
    duplicate_count = df.index.duplicated().sum()
    print(f"[INFO] Duplicate Index Entries: {duplicate_count}")

    print("Data health checks completed successfully.")
    
    return df

def find_trend_lines_with_regression(pivots, df, min_points=3, tolerance=0.001, max_trendlines=500):
    """
    Calculate trend lines using linear regression and rank them by significance.
    """
    if len(pivots) < min_points:
        return []

    trendlines = []
    n = len(pivots)

    for i in range(n - min_points + 1):
        for j in range(i + 1, n):
            # Get two points to form initial line
            x1 = pd.Timestamp(pivots[i][0]).timestamp()
            y1 = pivots[i][1]
            x2 = pd.Timestamp(pivots[j][0]).timestamp()
            y2 = pivots[j][1]

            # Skip if points have the same timestamp
            if x2 == x1:
                continue

            # Calculate initial slope and intercept
            slope = (y2 - y1) / (x2 - x1)
            intercept = y1 - slope * x1

            # Find all points that lie on this line
            points_on_line = []
            for date, price in pivots:
                x = pd.Timestamp(date).timestamp()
                predicted_price = slope * x + intercept
                if abs(predicted_price - price) / price <= tolerance:
                    points_on_line.append((date, price))

            # If we have enough points, create a Trendline object
            if len(points_on_line) >= min_points:
                points_on_line.sort(key=lambda x: x[0])  # Ensure chronological order
                trendline = Trendline(slope, intercept, points_on_line[0][0], 
                                      points_on_line[-1][0], points_on_line, df)

                # Check for overlap with existing trendlines
                is_overlapping = False
                for existing_trendline in trendlines:
                    shared_points = set(trendline.points) & set(existing_trendline.points)
                    # print( len(shared_points) / len(trendline.points), "shared points")
                    if len(shared_points) / len(trendline.points) > 0.1:  # Overlap threshold (50%)
                        is_overlapping = True
                        break

                if not is_overlapping and trendline.score > TRENDLINE_MIN_SCORE:
                    trendlines.append(trendline)

    # Sort trendlines by score and return top N
    return sorted(trendlines, key=lambda x: x.score, reverse=True)[:max_trendlines]

def find_pivots_multiple_lookbacks(data, lookbacks, price_threshold=0.005):
    """
    Detect significant pivot points for multiple lookback values.
    
    Args:
        data (pd.DataFrame): Stock data with High/Low prices.
        lookbacks (list): List of lookback values to use for pivot detection.
        price_threshold (float): Minimum percentage difference between pivot points.
    
    Returns:
        dict: A dictionary where keys are lookback values and values are tuples of (pivots_high, pivots_low).
    """
    results = {}

    def is_price_too_close(price, existing_pivots_high, existing_pivots_low):
        """Check if price is too close to any existing pivot (high or low)."""
        for _, p in existing_pivots_high + existing_pivots_low:
            if abs(price - p) / p < price_threshold:
                return True
        return False

    for lookback in lookbacks:
        pivots_high = []
        pivots_low = []

        for i in range(lookback, len(data) - lookback):
            current_date = data.index[i]
            start_date = data.index[i - lookback]
            end_date = data.index[i + lookback]

            # Get current values
            current_high = data.at[current_date, 'High']
            current_low = data.at[current_date, 'Low']

            # Get range values
            high_range = data.loc[start_date:end_date, 'High'].drop(index=current_date)
            low_range = data.loc[start_date:end_date, 'Low'].drop(index=current_date)
            high_max = high_range.max()
            low_min = low_range.min()

            # Check for high pivots
            if current_high > high_max:
                if not is_price_too_close(current_high, pivots_high, pivots_low):
                    pivots_high.append((current_date, current_high))

            # Check for low pivots
            elif current_low < low_min:
                if not is_price_too_close(current_low, pivots_high, pivots_low):
                    pivots_low.append((current_date, current_low))

        # Store results for this lookback
        results[lookback] = (pivots_high, pivots_low)

    return results

def save_plot_with_directories(filename, subdirectory):
    """
    Save the plot in a specific subdirectory, creating the directory if it doesn't exist.
    
    Args:
        filename (str): Name of the file to save (e.g., 'pivots.png').
        subdirectory (str): Subdirectory path (e.g., 'artifacts/levels/').
    """
    # Create the full path
    full_path = os.path.join(subdirectory, filename)
    
    # Create the subdirectory if it doesn't exist
    os.makedirs(subdirectory, exist_ok=True)
    
    # Save the plot
    plt.savefig(full_path, dpi=300, bbox_inches='tight', facecolor='black', edgecolor='none')
    print(f"Plot saved to {full_path}")
    plt.close()

def plot_pivots_multiple_lookbacks(df, pivots_by_lookback, filename='pivots.png', subdirectory='artifacts/levels/'):
    """
    Plot pivot points for multiple lookback values on the price chart and highlight key levels.
    
    Args:
        df (pd.DataFrame): DataFrame containing price data.
        pivots_by_lookback (dict): Dictionary of pivot points for each lookback value.
        filename (str): Name of the file to save the plot.
        subdirectory (str): Subdirectory to save the plot (e.g., 'artifacts/levels/').
    """
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot price data
    ax.plot(df.index, df['Close'], label="Closing Price", color="cyan", alpha=0.6)
    
    # Define colors for each lookback value
    colors = ['red', 'green', 'blue', 'orange', 'purple']
    
    # Combine all pivot points and track occurrences
    pivot_counts = {}
    for lookback, (pivots_high, pivots_low) in pivots_by_lookback.items():
        for date, value in pivots_high + pivots_low:
            if value not in pivot_counts:
                pivot_counts[value] = []
            pivot_counts[value].append(date)
    
    # Identify key levels (points that appear in multiple lookbacks)
    key_levels = {value: sorted(dates) for value, dates in pivot_counts.items() if len(dates) > 1}
    
    # Track unique labels for the legend
    unique_labels = set()
    
    # Plot pivot points for each lookback value
    for i, (lookback, (pivots_high, pivots_low)) in enumerate(pivots_by_lookback.items()):
        color = colors[i % len(colors)]  # Cycle through colors if more than 5 lookbacks
        
        # Plot high pivots
        for date, value in pivots_high:
            label = f'High (Lookback={lookback})'
            if label not in unique_labels:
                ax.scatter(date, value, color=color, marker='^', s=100, label=label)
                unique_labels.add(label)
            else:
                ax.scatter(date, value, color=color, marker='^', s=100)
        
        # Plot low pivots
        for date, value in pivots_low:
            label = f'Low (Lookback={lookback})'
            if label not in unique_labels:
                ax.scatter(date, value, color=color, marker='v', s=100, label=label)
                unique_labels.add(label)
            else:
                ax.scatter(date, value, color=color, marker='v', s=100)
    
    # Plot key levels as rays starting from the first occurrence
    for value, dates in key_levels.items():
        start_date = dates[0]  # First occurrence of the key level
        label = 'Key Level'
        if label not in unique_labels:
            ax.plot([start_date, df.index[-1]], [value, value], color='yellow', linestyle='--', alpha=0.8, label=label)
            unique_labels.add(label)
        else:
            ax.plot([start_date, df.index[-1]], [value, value], color='yellow', linestyle='--', alpha=0.8)
    
    # Customize plot
    ax.set_title("Price Action with Pivot Points (Multiple Lookbacks)", color='white', size=14)
    ax.set_xlabel("Date", color='white')
    ax.set_ylabel("Price", color='white')
    ax.legend(facecolor='black', edgecolor='white', fontsize=8, loc='upper left')
    ax.grid(alpha=0.2, color='gray')
    
    # Save the plot in the specified subdirectory
    save_plot_with_directories(filename, subdirectory)


def plot_trendlines_with_regression(df, pivots, filename='trendlines_regression.png', 
                                  subdirectory='artifacts/trendlines/', min_threshold=3):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot price data
    ax.plot(df.index, df['Close'], label="Closing Price", color="cyan", alpha=0.6)
    
    # Define colors for thresholds
    colors = ['red', 'green', 'blue', 'orange', 'purple']
    
    # Track unique labels
    unique_labels = set()
    
    # Loop through thresholds
    for min_points in range(min_threshold, min_threshold+10):
        color = colors[(min_points - 2) % len(colors)]
        
        # Find and rank trendlines for current threshold
        trendlines = find_trend_lines_with_regression(pivots, df, min_points=min_points)
        
        # Print detailed information about trendlines
        print(f"\nTrendlines with {min_points} points:")
        print(f"Total trendlines found: {len(trendlines)}")
        if trendlines:
            print(f"R² values: {[f'{t.r_squared:.2f}' for t in trendlines]}")
            print(f"Scores: {[f'{t.score:.2f}' for t in trendlines]}")
            print(f"Lengths (days): {[t.length for t in trendlines]}")
            print(f"Points: {[len(t.points) for t in trendlines]}")
            print(f"Violations: {[t.violations for t in trendlines]}")
            print(f"Violation Ratio: {[t.violation_ratio for t in trendlines]}")

        # Bucket pivot points by date
        pivot_dates = [pd.to_datetime(date) for date, _ in pivots]
        pivot_counts = Counter(pivot_dates)

        # Convert to DataFrame
        pivot_density_df = pd.DataFrame.from_dict(pivot_counts, orient='index', columns=['count'])
        pivot_density_df = pivot_density_df.resample('1D').sum().fillna(0)

        # Normalize for plotting (0 to 1 scale)
        norm_counts = pivot_density_df['count'] / pivot_density_df['count'].max()

        # Plot top trendlines
        for trendline in trendlines:
            x_start = pd.Timestamp(trendline.start_date).timestamp()
            x_end = pd.Timestamp(trendline.end_date).timestamp()
            y_start = trendline.slope * x_start + trendline.intercept
            y_end = trendline.slope * x_end + trendline.intercept
            
            # Plot the trendline
            label = f'Trendline (Points={min_points}, R²={trendline.r_squared:.2f})'
            if label not in unique_labels:
                ax.plot([trendline.start_date, trendline.end_date], [y_start, y_end],
                       color=color, linestyle='--', alpha=0.8, label=label)
                unique_labels.add(label)
            else:
                ax.plot([trendline.start_date, trendline.end_date], [y_start, y_end],
                       color=color, linestyle='--', alpha=0.8)
            
            # Plot the points on the trendline
            trendline_points_x = [pd.Timestamp(date).to_pydatetime() for date, _ in trendline.points]
            trendline_points_y = [price for _, price in trendline.points]
            ax.scatter(trendline_points_x, trendline_points_y, color=color, marker='o', s=50, alpha=0.8)

    # Customize plot
    ax.set_title("Price Action with Ranked Trendlines", color='white', size=14)
    ax.set_xlabel("Date", color='white')
    ax.set_ylabel("Price", color='white')
    ax.legend(facecolor='black', edgecolor='white', fontsize=8, loc='upper left')
    ax.grid(alpha=0.2, color='gray')
    
    save_plot_with_directories(filename, subdirectory)

class Trendline:
    def __init__(self, slope, intercept, start_date, end_date, points, df):
        self.slope = slope
        self.intercept = intercept
        self.start_date = start_date
        self.end_date = end_date
        self.points = points
        self.df = df
        self.violations = 0
        self.violation_ratio=0
        
        # Calculate metrics
        self.r_squared = self._calculate_r_squared()
        self.length = self._calculate_length()
        self.angle = self._calculate_angle()
        self.proximity = self._calculate_proximity()
        self.violations = self._calculate_violations()

        self.score = self._calculate_score()
    
    def _calculate_r_squared(self):
        x = np.array([pd.Timestamp(date).timestamp() for date, _ in self.points])
        y = np.array([price for _, price in self.points])
        _, _, r_value, _, _ = linregress(x, y)
        return r_value**2
    
    def _calculate_length(self):
        return (pd.Timestamp(self.end_date) - pd.Timestamp(self.start_date)).days
    
    def _calculate_angle(self):
        return abs(np.degrees(np.arctan(self.slope)))
    
    def _calculate_proximity(self):
        current_price = self.df['Close'].iloc[-1]
        current_x = pd.Timestamp(self.df.index[-1]).timestamp()
        predicted_price = self.slope * current_x + self.intercept
        return abs(current_price - predicted_price) / current_price
    
    def _calculate_score(self):
        # Weights for different metrics
        weights = {
            'r_squared': 0.1,
            'points': 0.4,
            'length': 0.3,
            'angle': 0.2
        }
        
        # Normalize length to 0-1 range (assuming max length is 365 days)
        normalized_length = min(1.0, self.length / 365)
        
        # Normalize angle (prefer angles between 30 and 60 degrees)
        normalized_angle = 1.0 - min(abs(self.angle - 45) / 45, 1.0)
        
        # Calculate weighted score
        score = (
            weights['r_squared'] * self.r_squared +
            weights['points'] * (len(self.points) / 10) +  # Normalize by assuming max 10 points
            weights['length'] * normalized_length +
            weights['angle'] * normalized_angle
        )
        
        # # Penalize if too far from current price
        if self.proximity.item() > 0.01:  # More than 10% away from current price
            score *= 0.5
            
        # Penalize for violations
        violation_ratio = self.violations / max(1, self.length)
        self.violation_ratio = violation_ratio
        if violation_ratio > 0.25:  # More than 25% of candles violate the trendline
            score *= 0.5
        if violation_ratio > .5:
            score = 0 # Trash the trendline

        return score
    
    def _calculate_violations(self, tolerance=0.01):
        """
        Count how many candles (dates) violate the trendline.
        A violation is when the actual price deviates more than the tolerance.
        """
        violations = 0
        for date in self.df.index:
            if self.start_date <= date <= self.end_date:
                x = pd.Timestamp(date).timestamp()
                predicted = self.slope * x + self.intercept
                # print(f"[DEBUG] Type of date: {type(date)} — Value: {date}")
                actual = self.df.at[date, 'Close']

                if abs(predicted - actual) / actual > tolerance:
                    violations += 1
        return violations


def main():
    df = fetch_stock_data("AAPL", "2024-01-01", "2025-01-01")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Define lookback values to test
    lookbacks = [5, 10, 15, 20, 25]

    # Find pivot points for multiple lookbacks
    pivots_by_lookback = find_pivots_multiple_lookbacks(df, lookbacks, price_threshold=0.005)

    # Combine all pivot points
    pivots = []
    for pivots_high, pivots_low in pivots_by_lookback.values():
        pivots.extend(pivots_high + pivots_low)

    # Plot trendlines using linear regression
    trendlines_filename = f"{ARTIFACT_PREFIX}_trendlines_regression_{timestamp}.png"
    plot_trendlines_with_regression(df, pivots, filename=trendlines_filename, subdirectory='artifacts/trendlines/')


if __name__ == "__main__":
    main()
