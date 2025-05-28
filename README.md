# Quant-Trad

A modular, extensible quantitative trading research and execution system designed for flexibility, clarity, and strategy experimentation.

---

## 🔧 Features

### ✅ Core Architecture
- **Modular Indicator Classes** – each strategy component (Levels, VWAP, Market Profile) is encapsulated in its own class.
- **ChartPlotter** – plots OHLCV with optional overlays and volume.
- **Backtester & Strategy Engine** – plug-in architecture for strategy evaluation (under development).
- **PostgreSQL with TimescaleDB** – efficient storage & retrieval of time-series data.

---

## 📈 Indicators

### Pivot Level Indicator
- Detects support and resistance using high/low pivots.
- Supports role-based and timeframe-based color modes.
- **Touchpoints plotted as dots** at each level where the price tested the level.
- **Customizable loopback periods** for detection resolution.

### LevelsIndicator (Daily, H4)
- Wraps PivotLevelIndicator with preset lookbacks for daily and H4 intervals.
- Merges close levels intelligently using volatility bandwidth.

### Market Profile Indicator
- Computes **POC**, **VAH**, and **VAL** for each day using 30-minute candles.
- Uses **volume-based profiling** (TPO-based planned).
- Overlays each session's profile using correct trading chart index.
- Configurable `bin_size`.

### VWAP Indicator *(in progress)*
- Will support daily Value Areas and multi-session merge logic.
- Will enable parameter sweep for optimal configuration testing.

---

## 🛠️ Charting Enhancements

- **Legend Auto-Building** – based on overlay role (`support`, `resistance`) or source (`daily`, `h4`, `market_profile`).
- **Flexible Overlay System** – indicators provide `to_overlays()` methods that return standardized mplfinance-compatible overlays.
- **Session-Aware Plotting** – overlays align to the active chart index, not just the indicator’s internal data.
- **Dynamic Figure Sizing** – adjusts width based on number of data points.

---

## 📦 Project Structure

```
quant-trad/
├── classes/
│   ├── indicators/
│   │   ├── PivotLevelIndicator.py
│   │   ├── MarketProfileIndicator.py
│   │   └── VWAPIndicator.py
│   ├── ChartPlotter.py
│   ├── Logger.py
│   ├── DataLoader.py
│   └── engines/
├── data_providers/
│   ├── alpaca.py
│   ├── yahoo.py
│   └── base.py
├── main.py
├── grid_search.py
└── requirements.txt
```

---

## 🧪 Testing and Visualization

You can use `main.py` to run various indicator tests.

### Run Market Profile Chart

```python
from classes.indicators.MarketProfileIndicator import run_market_profile_test
run_market_profile_test(DataLoader, AlpacaProvider(), symbol="CL")
```

### Enable/Disable Individual Tests

Inside `main.py` or `grid_search.py`, toggle specific indicators:

```python
# Toggle support/resistance overlays
# daily_overlays = DailyLevelsIndicator(df).to_overlays(plot_index=df.index)
```

---

## 🔄 Data Ingestion

Supports historical backfill via:

```python
provider.ingest_history(symbol="CL", interval="1h", start="2023-01-01", end="2024-01-01")
```

---

## 🚀 Upcoming

- VWAP Value Area merging logic
- Walk-forward backtesting engine
- Strategy optimization loop using YAML config + grid search
- Dashboard UI with real-time overlay sync

---

## 🤝 Contributing

This repo is in active development. PRs and feedback are welcome once version 1 is released.

---