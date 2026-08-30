# Quant-Trad

Quant-Trad (QT) is a local quantitative-research platform for turning trading
ideas into repeatable evidence. It collects and normalizes market data, freezes
exact research inputs, runs Checks and Strategies in time order, simulates
execution and accounting, and preserves results for inspection and comparison.

QT supports research, backtesting, walk-forward evaluation, reporting, and
bounded paper simulation or observe-only operation. **It does not submit orders
to external venues and is not live-capital trading infrastructure.**

## Quick Start

### Prerequisites

- Bash on Linux or WSL
- Docker Engine or Docker Desktop with Compose v2
- GNU Make
- Python 3.12+

From the repository root:

```bash
make deps
./scripts/qt setup env
make up BUILD=1 STACK_PROFILES=core
./scripts/qt setup doctor
```

If Python 3.12 is not your default interpreter, use
`make deps PY=python3.12`. The `./scripts/qt` wrapper uses the
repository-managed virtual environment, so activating `.venv` is optional.

Provider credentials are not required to start QT or work with existing frozen
Datasets. Configure them only for external acquisition or streaming; the
[getting-started guide](docs/getting-started.md) covers that path.

### Local Surfaces

| Surface | Address |
| --- | --- |
| Frontend V2 | <http://localhost:5174> |
| Backend API | <http://localhost:8000> |
| TimescaleDB / PostgreSQL | `localhost:15432` |
| pgAdmin | <http://localhost:8080> |

Useful first checks:

```bash
./scripts/qt setup doctor
make ps
make logs SERVICE=backend
```

Start the optional Grafana and Loki services with
`make up BUILD=1 STACK_PROFILES=all`. Stop the local stack with `make down`.

## How QT Fits Together

```text
external market state
  -> provider adapters
  -> canonical Facts
  -> frozen Datasets
  -> Indicators, Checks, and Strategies
  -> simulated execution and accounting
  -> BotLens and reports
```

- Provider adapters translate outside data into QT's canonical model, and
  frozen Datasets identify the exact evidence used by durable research.
- Indicators measure, Checks evaluate bounded questions, and Strategies decide.
  The bot runtime owns simulated fills, fees, wallet state, and order lifecycle.
- The CLI, API, frontend, reports, and MCP adapter share the same backend
  contracts; presentation surfaces do not create alternate runtime truth.

For implemented behavior and current limits, read
[QT Today](docs/current-system.md).

## Repository Map

| Path | Responsibility |
| --- | --- |
| `src/` | Core domain, data, research, indicator, and runtime code |
| `portal/backend/` | HTTP application interface and service orchestration |
| `portal/frontend/` | Frontend V2 operation, visualization, BotLens, and reports |
| `tests/` | Contract, unit, integration, database, and boundary tests |
| `docker/` | Local, test, observability, and server compositions |
| `scripts/` | Repository CLI, CI, setup, and operational helpers |
| `docs/` | Guides, contracts, architecture, engineering, and operator material |

Use `./scripts/qt --help` to discover CLI commands and `make help` for stack,
test, and repository operations.

## Developer Checks

Run checks for the area you changed:

```bash
make backend-check
make frontend-check
make validate-docs
```

Run the normal combined validation before handing off a broad change:

```bash
make check-all
```

For database-related changes, use the disposable isolated route:

```bash
./scripts/ci/run_test_suite.sh db
```

Never point test configuration at a development, shared, server, or production
database. See the [testing strategy](docs/engineering/testing/testing-strategy.md)
for suite selection and isolation details.

## Safety Notes

- Keep provider secrets out of repository files, logs, research artifacts, and
  bot configuration. QT stores credential references instead of plaintext
  provider secrets.
- Backtests and durable Checks use frozen evidence. A historical result is
  evidence under declared inputs and assumptions, not a forecast.
- Network-backed acquisition is explicit; replay, tests, and canonical reads
  must not contact a provider implicitly.
- External order submission is closed, and normal validation must not use
  production or live systems.

## Where To Go Next

| Goal | Read |
| --- | --- |
| Install QT or run a first backtest | [Getting started](docs/getting-started.md) |
| Test a trading idea | [Core research workflow](docs/guides/research-workflow.md) |
| Understand the implemented system | [Current system](docs/current-system.md) |
| Understand the boundaries worth protecting | [Six core promises](docs/core-promises.md) and [platform contracts](docs/contracts/README.md) |
| Change code or a subsystem boundary | [Developer workflow](docs/engineering/developer-workflow.md) and [architecture guide](docs/architecture/README.md) |
| Look up an exact QT term | [Platform glossary](docs/contracts/platform/04_glossary.md) |
| Operate or deploy a durable host | [Operator handbook](docs/operators/README.md) |

The [documentation home](docs/index.md) provides the complete role-based reading
map. Platform contracts are authoritative when explanatory prose and code
disagree.
