import yfinance as yf

# Download Apple stock data for the past 5 years
data = yf.download("AAPL", start="2020-01-01", end="2025-01-01", interval="1d")
print(data.head())