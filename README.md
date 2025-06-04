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

## 🔄 Data Ingestion

Supports historical backfill via:

```python
provider.ingest_history(symbol="CL", interval="1h", start="2023-01-01", end="2024-01-01")
```

---