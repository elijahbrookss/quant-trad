import yfinance as yf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import linregress


def fetch_stock_data(symbol, start, end):
    df = yf.download(symbol, start=start, end=end, auto_adjust=True)[['Open', 'High', 'Low', 'Close', 'Volume']]
    return df.tz_localize(None)

def find_trend_lines(pivots):
    """
    Calculate multiple trend lines using linear regression.
    Each trendline connects at least two consecutive pivot points.
    
    Args:
        pivots (list): List of (date, price) tuples representing pivot points.
    
    Returns:
        list: A list of (slope, intercept, start_date, end_date) for each trendline.
    """
    if len(pivots) < 2:
        return []

    trendlines = []
    for i in range(len(pivots) - 1):
        # Get two or more consecutive pivot points
        x = np.array([pd.Timestamp(date).timestamp() for date, _ in pivots[i:i+2]])
        y = np.array([price for _, price in pivots[i:i+2]])
        
        # Perform linear regression
        slope, intercept, _, _, _ = linregress(x, y)
        
        # Store the trendline with its start and end dates
        trendlines.append((slope, intercept, pivots[i][0], pivots[i+1][0]))
    
    return trendlines

def find_pivots(data, lookback=3, price_threshold=0.02):
    """
    Detect significant pivot points with minimum price difference from all existing pivots.
    """
    pivots_high = []
    pivots_low = []

    def is_price_too_close(price, existing_pivots_high, existing_pivots_low):
        """Check if price is too close to any existing pivot (high or low)."""
        # Check against all existing pivots (both highs and lows)
        for _, p in existing_pivots_high + existing_pivots_low:
            if abs(price - p) / p < price_threshold:
                return True
        return False

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
                # Remove any existing pivots that are too close
                pivots_high = [p for p in pivots_high 
                             if abs(p[1] - current_high) / p[1] >= price_threshold]
                pivots_high.append((current_date, current_high))

        # Check for low pivots
        elif current_low < low_min:
            if not is_price_too_close(current_low, pivots_high, pivots_low):
                # Remove any existing pivots that are too close
                pivots_low = [p for p in pivots_low 
                            if abs(p[1] - current_low) / p[1] >= price_threshold]
                pivots_low.append((current_date, current_low))

    return pivots_high, pivots_low

def plot_pivots(df, pivots_high, pivots_low, filename='pivots.png'):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot price data
    ax.plot(df.index, df['Close'], label="Closing Price", color="cyan", alpha=0.6)
    
    # Combine all pivots (highs and lows) for trendline calculation
    all_pivots = sorted(pivots_high + pivots_low, key=lambda x: x[0])  # Sort by date
    
    # Plot pivot points
    for date, value in all_pivots:
        ax.scatter(date, value, color='gold', marker='o', s=100)
    
    # Calculate and plot all trendlines
    trendlines = find_trend_lines(all_pivots)
    for slope, intercept, start_date, end_date in trendlines:
        # Generate x-values for the trendline (between start_date and end_date)
        x_vals = np.array([pd.Timestamp(start_date).timestamp(), pd.Timestamp(end_date).timestamp()])
        y_vals = slope * x_vals + intercept
        
        # Convert x_vals back to datetime for plotting
        x_dates = [pd.Timestamp.fromtimestamp(x) for x in x_vals]
        ax.plot(x_dates, y_vals, color='red', linestyle='-', alpha=0.8, linewidth=2)
    
    # Customize plot
    ax.set_title("Price Action with Trendlines", color='white', size=14)
    ax.set_xlabel("Date", color='white')
    ax.set_ylabel("Price", color='white')
    ax.legend(facecolor='black', edgecolor='white')
    ax.grid(alpha=0.2, color='gray')
    
    plt.savefig(filename, dpi=300, bbox_inches='tight', 
                facecolor='black', edgecolor='none')
    plt.close()
    
def main():
    df = fetch_stock_data("AAPL", "2024-01-01", "2025-01-01")
    pivots_high, pivots_low = find_pivots(df, lookback=3, price_threshold=.03)
    plot_pivots(df, pivots_high, pivots_low)

if __name__ == "__main__":
    main()
