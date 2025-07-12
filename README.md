

# Quant-Trad 🚀  
*A work-in-progress quantitative **trading bot** (autonomous execution coming soon)*


---

## ✨ Vision

Quant-Trad is being built to **trade autonomously**.  
Right now it focuses on clean data ingestion, robust indicator generation, and high-signal chart overlays.  
Next milestones add strategy orchestration, parameter sweeps, and a live execution pipeline.

---

## 🏗️ Core Architecture (current)

| Layer | Key Components | Notes |
|-------|----------------|-------|
| **Data** | `BaseDataProvider`, `AlpacaProvider`, `YahooProvider` | Uniform OHLCV schema; optional TimescaleDB cache |
| **Indicators** | `PivotLevelIndicator`, `MarketProfileIndicator` (TPO) | Implemented & tested |
| *(Coming)* | `TrendlineIndicator`, `VWAPIndicator` | In development |
| **Visualization** | `ChartPlotter` | Candles + volume + overlay lines |
| *(Road-map)* | Strategy, Back-testing, Live Execution | Foundations laid, wiring next |

---

## 📈 Indicators At-a-Glance

| Indicator | Status | Purpose | Overlay Goodies |
|-----------|--------|---------|-----------------|
| **Pivot Level** | ✅ | Convert swing highs/lows into S/R levels | Role & timeframe colors, touch-points |
| **Market Profile (TPO)** | ✅ | POC / VAH / VAL per session, VA merges | Dashed VA bands per session |
| **Trendline** | 🔨 | Auto-detect dynamic trendlines | Continuous lines, breakout flags |
| **VWAP** | 🔨 | 30-min volume profile & VA merges | Session bands, rolling POC anchor |

---

## ⚡ Makefile Commands

| Target               | Description                                                             |
|----------------------|-------------------------------------------------------------------------|
| `make db_up`         | Spin up **TimescaleDB** and **pgAdmin** containers and wait until ready |
| `make db_down`       | Stop the TimescaleDB / pgAdmin containers                               |
| `make db_logs`       | Tail TimescaleDB logs (`Ctrl-C` to quit)                                |
| `make db_cli`        | Open a `psql` shell at `postgres://postgres:postgres@localhost:5432/postgres` |
| `make test`          | Run the full pytest suite                                               |
| `make test-unit`     | Run only unit tests (`-m "not integration"`)                           |
| `make test-integration` | Run only tests tagged `@pytest.mark.integration`                     |


## ⚡ Quick Start-up

> **Prerequisites**  
> • Python 3.10+ with `venv`  
> • [Docker Desktop](https://www.docker.com/products/docker-desktop/) running (needed for the TimescaleDB + pgAdmin containers)  
> • GNU Make (pre-installed on macOS/Linux; Windows users can use the Git-Bash version or **WSL**)

```bash
# 0) clone the repo
git clone --branch develop https://github.com/elijahbrookss/quant-trad.git
cd quant-trad

# 1) spin-up TimescaleDB (+ pgAdmin) in Docker
make db_up 

# 2) create / activate virtual-env and install deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3) run the full test-suite
make test            # or: make test-unit / make test-integration

# 4) (optional) open a psql shell
make db_cli          # \q to exit

# 5) shut containers down when finished
make db_down