# Quant-Trad

A practical quantitative trading bot with modular data pipelines, indicators, and strategy orchestration. This README focuses on getting the stack running quickly and keeping day-to-day workflows consistent.

## Prerequisites

Install the following tools before you start:

- Python 3.10 or newer
- Docker Desktop (or Docker Engine + Docker Compose plugin)
- GNU Make

## Initial Setup

1. Clone the repository and enter the project directory.
   ```bash
   git clone https://github.com/elijahbrookss/quant-trad.git
   cd quant-trad
   ```
2. Create your secrets file from the provided template and add credentials (see [Secrets](#secrets)).
   ```bash
   cp secrets.env.example secrets.env
   ```

## Local (non-Docker) workflow

Use this flow when you want to develop against the Python sources directly on your machine.

```bash
# Install Python and frontend dependencies in a virtual environment
make local-setup

# Start TimescaleDB + pgAdmin in Docker
make local-db-up

# Launch the FastAPI backend and Vite frontend locally
make api-start
make frontend-start
```

Services run on:

- API: http://localhost:8000
- Frontend: http://localhost:5173
- TimescaleDB: `localhost:15432` (configurable via `TSDB_PORT`)

When you are done, stop everything with:

```bash
make frontend-stop
make api-stop
make local-db-stop
```

`make local-up` and `make local-stop` are convenience shortcuts that bundle the commands above.

## Docker Compose workflow

Run the entire stack in containers when you want an isolated environment:

```bash
# Start backend, frontend, TimescaleDB, pgAdmin, Grafana, and Loki
make stack-up                # or STACK_PROFILES=core/database/observability make stack-up

# Tail container logs as needed
make stack-logs              # optional helper defined in the Makefile

# Stop or tear down the stack
make stack-stop              # keep containers
make stack-down              # remove containers
```

Docker Compose publishes the services on the same ports listed in the local workflow. Override `TSDB_PORT` if you need a different TimescaleDB port on the host.

The core profile now bundles an Interactive Brokers (IBKR) gateway container that runs IB Gateway in headless mode using Xvfb and IBC. When you start the stack the backend can immediately reach `ibkr-gateway.quanttrad` on the internal network; no local TWS instance is required. The container exposes port `4002` for paper trading and `4001` for live trading, forwarding credentials and login automation parameters from `secrets.env`.

The image is built locally from an Ubuntu base—no external container registry access is required. During the build Docker downloads the IBC bundle and the IB Gateway installer from their public release URLs; the defaults track IBC `3.19.1` and IB Gateway `10.25.2`. If Interactive Brokers publishes a newer build (or retires the defaults) override the URLs and versions via the optional environment variables surfaced in `docker/docker-compose.yml` (for example `IBKR_GATEWAY_DOWNLOAD_URL` or `IBKR_IBC_VERSION`) before running `make stack-build`.

> **Weekly 2FA reminder** – IBKR still requires a mobile approval every seven days. Launch the mobile app and confirm the login prompt after the container boots or restarts, otherwise historical and live data requests will fail.

### When to rebuild containers

Use the rebuild flow whenever you need fresh Docker images that include new dependencies or base image updates:

```bash
make stack-rebuild           # rebuild images with --no-cache and restart
```

Trigger this after changing `requirements.txt`, updating frontend dependencies in `package.json`, or modifying Dockerfiles. For routine code edits that do not touch dependencies or build configuration, a normal `make stack-restart` is usually sufficient.

## Secrets

`secrets.env` is not committed to version control but is required for anything that touches live market data.

| Variable | Required | Description |
| --- | --- | --- |
| `ALPACA_API_KEY` | ✅ | Alpaca trading API key for equities data/execution |
| `ALPACA_SECRET_KEY` | ✅ | Alpaca secret key |
| `CCXT_API_KEY` | Optional | Shared CCXT API key for crypto exchanges |
| `CCXT_API_SECRET` | Optional | Shared CCXT secret |
| `CCXT_PASSWORD` | Optional | Some exchanges (e.g., BitMEX) require an API password |
| `CCXT_<EXCHANGE>_*` | Optional | Exchange-specific overrides (e.g., `CCXT_BINANCE_API_KEY`) |
| `IB_HOST` | Optional | Hostname or IP for the Interactive Brokers gateway (defaults to `ibkr-gateway.quanttrad`) |
| `IB_PORT` | Optional | IBKR API port (defaults to `4002` when using the managed gateway) |
| `IB_CLIENT_ID` | Optional | Client identifier used when establishing the IBKR session |
| `IBKR_TWS_USERNAME` | Optional | Username forwarded to the managed IB Gateway container |
| `IBKR_TWS_PASSWORD` | Optional | Password forwarded to the managed IB Gateway container |
| `IBKR_TRADING_MODE` | Optional | `paper` or `live`; forwarded to the Gateway container and backend |
| `IBKR_READONLY_API` | Optional | `yes`/`no` toggle passed to the gateway controller (defaults to `yes`) |
| `IBKR_ACCEPT_INCOMING_ACTION` | Optional | Duplicate session handling strategy (`accept` by default) |
| `IBKR_EXISTING_SESSION_ACTION` | Optional | Action when another session is active (`primary` by default) |
| `IBKR_GATEWAY_PORT` | Optional | Host port to expose for the paper gateway (defaults to `4002`) |
| `IBKR_GATEWAY_LIVE_PORT` | Optional | Host port to expose for the live gateway (defaults to `4001`) |

The file is mounted into the backend container automatically when you use Docker Compose.

## Running tests and checks

```bash
# Full suite
make test

# Narrow scope
make test-unit
make test-integration
```

## Useful Make targets

| Target | Purpose |
| --- | --- |
| `make deps` | Install Python dependencies into `.venv` |
| `make reset-venv` | Recreate the virtual environment |
| `make db_cli` | Open a psql shell against TimescaleDB |
| `make stack-ps` | Show running containers |
| `make stack-restart` | Restart the selected Docker profiles |

For more commands, run `make help`.
