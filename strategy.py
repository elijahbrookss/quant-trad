import yfinance as yf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import linregress
from datetime import datetime

ARTIFACT_PREFIX = 'artifacts/plotmulti'
SHOW_TRENDLINES = False

def fetch_stock_data(symbol, start, end):
    df = yf.download(symbol, start=start, end=end, auto_adjust=True)[['Open', 'High', 'Low', 'Close', 'Volume']]
    return df.tz_localize(None)

def find_trend_lines(pivots, tolerance=0.01):
    """
    Calculate trend lines that connect 3 or more pivot points.
    Each trendline is validated by checking if additional pivot points lie on or near the line.
    
    Args:
        pivots (list): List of (date, price) tuples representing pivot points.
        tolerance (float): Maximum allowed deviation (as a percentage of price) for a point to lie on the trendline.
    
    Returns:
        list: A list of (slope, intercept, start_date, end_date) for each valid trendline.
    """
    if len(pivots) < 3:
        return []

    trendlines = []
    n = len(pivots)

    # Iterate through all pairs of pivot points to calculate potential trendlines
    for i in range(n - 1):
        for j in range(i + 1, n):
            # Get the two pivot points
            x1 = pd.Timestamp(pivots[i][0]).timestamp()
            y1 = pivots[i][1]
            x2 = pd.Timestamp(pivots[j][0]).timestamp()
            y2 = pivots[j][1]

            # Calculate slope and intercept of the line
            slope = (y2 - y1) / (x2 - x1)
            intercept = y1 - slope * x1

            # Check how many points lie on or near the line
            count = 0
            for date, price in pivots:
                x = pd.Timestamp(date).timestamp()
                predicted_price = slope * x + intercept
                if abs(predicted_price - price) / price <= tolerance:
                    count += 1

            # Only keep the trendline if it connects 3 or more points
            if count >= 3:
                trendlines.append((slope, intercept, pivots[i][0], pivots[j][0]))

    return trendlines

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
            current_high = np.array(data.loc[current_date, 'High']).item()
            current_low = np.array(data.loc[current_date, 'Low']).item()

            # Get range values
            high_range = data.loc[start_date:end_date, 'High'].drop(index=current_date)
            low_range = data.loc[start_date:end_date, 'Low'].drop(index=current_date)
            high_max = np.array(high_range.max()).item()
            low_min = np.array(low_range.min()).item()

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

def plot_pivots_multiple_lookbacks(df, pivots_by_lookback, filename='pivots.png'):
    """
    Plot pivot points for multiple lookback values on the price chart and highlight key levels.
    
    Args:
        df (pd.DataFrame): DataFrame containing price data.
        pivots_by_lookback (dict): Dictionary of pivot points for each lookback value.
        filename (str): Name of the file to save the plot.
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
    
    plt.savefig(filename, dpi=300, bbox_inches='tight', 
                facecolor='black', edgecolor='none')
    plt.close()

def main():
    df = fetch_stock_data("AAPL", "2024-01-01", "2025-01-01")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{ARTIFACT_PREFIX}_{timestamp}.png"

    # Define lookback values to test
    lookbacks = [5, 10, 15, 20, 25]

    # Find pivot points for multiple lookbacks
    pivots_by_lookback = find_pivots_multiple_lookbacks(df, lookbacks, price_threshold=0.005)

    # Plot the pivot points
    plot_pivots_multiple_lookbacks(df, pivots_by_lookback, filename=filename)


if __name__ == "__main__":
    main()
