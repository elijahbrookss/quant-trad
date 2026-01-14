# quant-trad

A modular quantitative trading platform for automated strategy development, backtesting, and live execution across equities, futures, and crypto markets.

## What is this?

**quant-trad** is a framework for building and running algorithmic trading strategies with:

- **Multi-asset support** - Trade equities (Alpaca), futures/options (Interactive Brokers), and crypto (CCXT exchanges)
- **Modular indicators** - Composable technical indicators with incremental caching for performance
- **Signal-driven architecture** - Define trading rules as signals that trigger on market conditions
- **Walk-forward backtesting** - Test strategies on historical data with realistic execution simulation
- **Live execution** - Deploy validated strategies to paper or live trading accounts
- **Observability stack** - Grafana dashboards, Loki logs, and TimescaleDB for performance tracking

This is a **work in progress** but contributions are welcome from frontier developers comfortable with evolving APIs.

## Quick Start

**Prerequisites:** Docker, Python 3.10+, Make

```bash
# Clone and setup
git clone https://github.com/elijahbrookss/quant-trad.git
cd quant-trad
cp secrets.env.example secrets.env  # Add your API keys

# Start the stack
make build up

# View logs
make logs SERVICE=backend
```

**Services:**
- Frontend: http://localhost:5173
- Backend API: http://localhost:8000
- Grafana: http://localhost:3000
- pgAdmin: http://localhost:8080

## Core Concepts

### 1. **Indicators**
Technical indicators compute market signals from OHLCV data. They support:
- **Incremental caching** - Reuse computed results across runs (e.g., cache daily profiles, only compute new days)
- **Overlay rendering** - Generate chart overlays (boxes, markers, lines) for visualization
- **Composition** - Combine simple indicators into complex strategies

Example indicators: Market Profile (TPO), VWAP, Moving Averages, RSI

### 2. **Signals**
Signals represent trading events triggered by indicator conditions:
- `breakout` - Price breaks through a key level
- `retest` - Price returns to test a previous breakout level
- `reversal` - Trend change detected
- Custom signal types for strategy-specific logic

### 3. **Strategies**
Strategies orchestrate indicators and signals into complete trading systems:
- Define entry/exit rules
- Manage position sizing and risk
- Handle multi-timeframe analysis
- Track performance metrics

### 4. **Data Providers**
Pluggable data sources for market data:
- **Alpaca** - Equities and crypto (US markets)
- **Interactive Brokers** - Futures, options, global equities
- **CCXT** - 100+ cryptocurrency exchanges
- **TimescaleDB** - Local OHLCV storage with time-series optimization

## Development Workflow

This project uses **Docker-based development** for consistency:

```bash
# Daily workflow
make build up          # Build images and start stack
make logs              # Watch all logs
make restart BUILD=1   # Rebuild and restart
make down              # Stop everything

# Testing
make test              # Run test suite
make fmt               # Format code
make lint              # Check code quality

# Database
make ps                # Show running containers
```

For detailed commands: `make help`

## Project Structure

```
quant-trad/
├── src/
│   ├── indicators/          # Technical indicators with caching
│   ├── signals/             # Signal generation and overlays
│   ├── strategies/          # Trading strategy implementations
│   ├── data_providers/      # Market data integrations
│   └── core/                # Shared utilities
├── portal/
│   ├── backend/             # FastAPI application
│   └── frontend/            # React/Vite UI
├── docker/                  # Docker Compose services
├── tests/                   # Test suite
└── docs/                    # Additional documentation
```

## Configuration

Create `secrets.env` with your API credentials:

```bash
# Required for equities
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret

# Optional: Interactive Brokers
IBKR_TWS_USERNAME=your_username
IBKR_TWS_PASSWORD=your_password
IBKR_TRADING_MODE=paper  # or 'live'

# Optional: Crypto exchanges (CCXT)
CCXT_BINANCE_API_KEY=your_key
CCXT_BINANCE_API_SECRET=your_secret
```

See [secrets.env.example](secrets.env.example) for all options.

## Documentation

For detailed guides on architecture, indicator development, backtesting, and deployment:

**📚 [quant-trad.gitbook.io/docs](https://quant-trad.gitbook.io/docs/)**

Topics covered:
- Indicator caching system
- Signal overlay rendering
- Walk-forward backtesting methodology
- Strategy development guide
- Data provider integration
- Production deployment

## Contributing

Contributions are welcome! This project is in active development and APIs may change.

**Good first contributions:**
- New indicator implementations
- Data provider integrations
- Strategy templates
- Documentation improvements
- Bug fixes and tests

Before contributing:
1. Read the [GitBook docs](https://quant-trad.gitbook.io/docs/) to understand the architecture
2. Check existing issues and PRs
3. Open an issue to discuss major changes
4. Follow the existing code style (`make fmt lint`)
5. Add tests for new features (`make test`)

## Status

⚠️ **Active Development** - APIs are stabilizing but expect breaking changes. Not recommended for production trading without thorough testing.

Current focus areas:
- Strategy backtesting framework
- Real-time signal processing
- Performance optimization (caching, database queries)
- Production deployment tooling

## License

MIT License - See [LICENSE](LICENSE) for details

---

**Questions?** Check the [GitBook docs](https://quant-trad.gitbook.io/docs/) or open an issue.
