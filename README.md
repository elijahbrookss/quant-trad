# quant-trad

Quant-trad is a quantitative trading platform for research, strategy evaluation, execution realism, and playback inspection.

The repo is organized around one core idea: trading behavior should be explainable from a single runtime timeline, not reconstructed later from loosely related artifacts.

## What This Repo Is

Quant-trad separates responsibilities into explicit layers:

- QuantLab: research and indicator exploration
- Strategy: decision logic from indicator outputs
- Bot: execution realism, fills, costs, risk, and lifecycle outcomes
- Playback / BotLens: audit and debugging surfaces for what the runtime actually did

The system contract is strict about live-equivalent sequencing. Derived outputs are valid only when they respect known-at timing and can be explained by sequential candle arrival.

## Platform Guarantees

These are the semantics the repo is built around:

- Live-equivalent evaluation: logic must hold under sequential market-data arrival
- Known-at causality: artifacts are usable only when `known_at <= evaluation_time`
- Determinism: fixed inputs, params, and versions should produce stable outputs
- Layer integrity: research, decision, execution, and playback stay separated
- Single runtime path: `initialize -> apply_bar -> snapshot`
- Playback is an audit surface, not a demo layer

If code conflicts with these semantics, the contracts in [`docs/agents/`](docs/agents/) are the source of truth.

## Main Components

- `src/engines/indicator_engine`: indicator execution and snapshot flow
- `src/engines/bot_runtime`: bot runtime engine and execution semantics
- `src/indicators`: indicator implementations and runtime-facing payloads
- `src/signals`: signal rules, overlays, and runtime signal plumbing
- `src/strategies`: strategy logic built on indicator and signal outputs
- `portal/backend`: FastAPI services for bots, data, storage, reports, and APIs
- `portal/frontend`: React/Vite UI including bot cards, BotLens, and operational views
- `docker/`: compose stack, observability services, database, and broker support

## Quick Start

Prerequisites:

- Docker
- GNU Make
- Python 3.12+ if you want to run local tooling outside Docker

Create local secrets:

```bash
cp secrets.env.example secrets.env
```

Config file roles:

- `.env`: checked-in local defaults for root Python tooling and test bootstrap
- `.env.test`: docker-compose test defaults
- `secrets.env`: private credentials and operator overrides
- `portal/frontend/.env`: frontend Vite defaults

Bring up the core stack:

```bash
make up BUILD=1 STACK_PROFILES=core
```

This starts:

- Frontend: `http://localhost:5173`
- Backend API: `http://localhost:8000`
- TimescaleDB: `localhost:15432`
- pgAdmin: `http://localhost:8080`

If you want observability as well:

```bash
make up BUILD=1 STACK_PROFILES=all
```

That adds:

- Grafana: `http://localhost:3000`
- Loki: `http://localhost:3100`

## Daily Workflow

Common commands:

```bash
make up BUILD=1 STACK_PROFILES=core   # build and start the core stack
make logs SERVICE=backend             # tail backend logs
make restart BUILD=1                  # rebuild/restart the current stack
make ps                               # inspect running services
make down                             # stop and remove containers
make test                             # run tests
make fmt                              # format code
make lint                             # lint code
make sync-docs                        # sync docs to your Obsidian/docs target
```

Run `make help` for the full command set.

## Configuration

Runtime configuration is split across a few files on purpose:

- `.env`: tracked local defaults for Python tooling, local DB wiring, and root test bootstrap
- `.env.test`: tracked defaults for `docker/docker-compose.test.yml`
- `secrets.env`: untracked private credentials and machine-specific overrides
- `portal/frontend/.env`: frontend API base defaults for Vite

Tests load `.env` and `secrets.env`. The Docker test stack uses `.env.test`.

Common integrations in this repo include:

- Alpaca
- Interactive Brokers
- CCXT-backed crypto exchanges
- TimescaleDB/Postgres
- Grafana / Loki

See [`secrets.env.example`](secrets.env.example) for the available settings and operational knobs. The platform uses a single database DSN: `PG_DSN`.

## Repository Map

```text
quant-trad/
├── src/
│   ├── engines/            # indicator and bot runtime engines
│   ├── indicators/         # indicator implementations
│   ├── signals/            # signal rules and overlays
│   ├── strategies/         # strategy definitions
│   ├── data_providers/     # provider integrations
│   └── core/               # shared runtime utilities
├── portal/
│   ├── backend/            # FastAPI backend and services
│   └── frontend/           # React/Vite frontend
├── docker/                 # compose stack and service images
├── docs/
│   ├── agents/             # canonical system/runtime contracts
│   └── architecture/       # focused architecture notes
└── tests/                  # test coverage
```

## Recommended Reading

Start here if you are new to the repo:

1. [`docs/agents/README.md`](docs/agents/README.md)
2. [`docs/agents/00_system_contract.md`](docs/agents/00_system_contract.md)
3. [`docs/agents/01_runtime_contract.md`](docs/agents/01_runtime_contract.md)
4. [`docs/agents/02_execution_playback_contract.md`](docs/agents/02_execution_playback_contract.md)
5. [`docs/agents/03_engineering_contract.md`](docs/agents/03_engineering_contract.md)

Then use these architecture docs for current implementation details:

- [`docs/architecture/ENGINE_OVERVIEW.md`](docs/architecture/ENGINE_OVERVIEW.md)
- [`docs/architecture/SIGNAL_PIPELINE_ARCHITECTURE.md`](docs/architecture/SIGNAL_PIPELINE_ARCHITECTURE.md)
- [`docs/architecture/BOT_RUNTIME_DOCS_HUB.md`](docs/architecture/BOT_RUNTIME_DOCS_HUB.md)
- [`docs/architecture/RUNTIME_EVENT_MODEL_V1.md`](docs/architecture/RUNTIME_EVENT_MODEL_V1.md)
- [`docs/architecture/WALLET_GATEWAY_ARCHITECTURE.md`](docs/architecture/WALLET_GATEWAY_ARCHITECTURE.md)

## Current State

This is an active development repo, not a polished end-user product.

That means:

- architecture and APIs are still evolving
- correctness and semantic consistency are prioritized over convenience
- logs are treated as part of the product
- invalid runtime states should fail loudly, not be hidden

Use caution before pointing this at real capital. The repo is built to be explainable first, optimized second.

## Contributing

Before making non-trivial changes:

1. Read the system and runtime contracts in [`docs/agents/`](docs/agents/)
2. Preserve the layer boundaries between research, strategy, execution, and playback
3. Prefer extending canonical snapshot/runtime contracts over adding alternate reconstruction paths
4. Add tests or targeted verification when behavior changes
5. Run `make sync-docs` after doc updates

## License

MIT. See [`LICENSE`](LICENSE).
