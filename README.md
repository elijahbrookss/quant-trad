# Quant-Trad  
*A modular, test-driven quantitative trading bot with strategy orchestration and live chart overlays*

---

## Vision

Quant-Trad is being built to **trade autonomously**, combining clean data ingestion, flexible indicators, stateless signal rules, and configurable strategies.  
Current focus is on structured feature extraction and signal generation, with backtesting and execution infrastructure in progress.

---

## Core Architecture

| Layer           | Key Components | Notes |
|----------------|----------------|-------|
| **Data**        | `BaseProvider`, `AlpacaProvider`, `YahooProvider` | Unified OHLCV schema, optional TimescaleDB caching |
| **Indicators**  | `PivotLevelIndicator`, `MarketProfileIndicator`, `TrendlineIndicator`, `VWAPIndicator` | Modular, composable, overlay-capable |
| **Signals**     | Stateless signal rules (e.g. `breakout_rule`, `bounce_rule`) | Operate on indicator output + context |
| **Strategies**  | `BaseStrategy`, `ReversalStrategy` | Orchestrates indicators and rules, produces structured signals |
| **Visualization** | `ChartPlotter`, `OverlayRegistry`, `OverlayHandlers` | Candlesticks with high-signal overlays |
| **Backtesting** | `Backtester`, `StrategyEngine` | Simulate strategy decisions over historical data |
| **Monitoring**  | Loki (logs), Grafana (dashboards) | Docker services for system observability |
| *(Planned)*     | Live Execution, Parameter Sweeps | Hooks for automated live trading and optimization |

---

## Strategy Framework Overview

| Component | Purpose |
|----------|---------|
| `Indicator` | Extracts features from OHLCV (levels, trendlines, VAH/VAL/POC) |
| `SignalRule` | Stateless logic to evaluate market conditions |
| `Strategy` | Registers indicators and rules, manages context, emits trade signals |
| `DataContext` | Defines timeframe and range for each indicator instance |
| `Signal` | Output object enriched with strategy and indicator metadata |

Strategies can register the same indicator multiple times with different configurations and rules to support confluence across timeframes or techniques.

---

## Indicators Summary

| Indicator | Status | Purpose | Overlay Features |
|-----------|--------|---------|------------------|
| **Pivot Level** | Complete | Convert swing highs/lows into support/resistance zones | Timeframe coloring, touchpoint dots |
| **Market Profile (TPO)** | Complete | Compute value areas and merged sessions | VA bands, dashed session overlays |
| **Trendline** | Complete | Auto-detect dynamic trendlines from pivots | Line overlays, breakout regions |
| **VWAP** | Complete | Compute value areas from intraday volume | Rolling session anchors, POC tracking |

---

## Makefile Commands

| Target            | Description |
|-------------------|-------------|
| `make setup`      | Start TimescaleDB, pgAdmin, Grafana, and Loki containers |
| `make shutdown`   | Stop all containers |
| `make db_cli`     | Open a `psql` shell to TimescaleDB |
| `make run`        | Run the app with `PYTHONPATH` set to the project root |
| `make test`       | Run all tests |
| `make test-unit`  | Run only unit tests |
| `make test-integration` | Run only integration tests |
| `make status`     | Show status of running containers |
| `make dev`        | Run the local dev startup script (`scripts/dev_startup.sh`) |

---

## Quick Start

**Prerequisites**
- Python 3.10+
- Docker Desktop
- GNU Make (comes with macOS/Linux; Windows users can use Git Bash or WSL)

```bash
# Clone the repo
git clone --branch develop https://github.com/elijahbrookss/quant-trad.git
cd quant-trad

# Start core services (TimescaleDB, pgAdmin, Grafana, Loki)
make setup 

# Create and activate virtual environment
python -m venv env && source env/bin/activate
pip install -r requirements.txt

# Run tests
make test            # or: make test-unit / make test-integration

# Launch TimescaleDB CLI (optional)
make db_cli

# Shut down services when done
make shutdown