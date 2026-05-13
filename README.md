# Quant-Trad

Quant-Trad is a deterministic trading engine for research, strategy evaluation, execution realism, and runtime inspection.

It is built around one question:

> **What happened during a trade -- and why?**

Every indicator output, strategy decision, fill, fee, wallet update, trade lifecycle event, BotLens view, and research dataset is derived from one walk-forward runtime timeline. The goal is not just to produce backtest results, but to make those results explainable from what the system knew at the time.

<p align="center">
  <img src="docs/assets/quant-trad-platform-flow.svg" alt="Quant-Trad platform flow" width="100%">
</p>

---

## What Quant-Trad Does

Quant-Trad connects research, strategy logic, execution modeling, and inspection into one runtime-driven system.

```text
Data -> Indicators -> Signals -> Decisions -> Execution -> BotLens / Reports
```

At a high level:

- **Data providers** supply market candles and source metadata.
- **Indicators** advance through the runtime timeline and publish typed outputs.
- **Strategies** consume typed outputs and produce decision artifacts.
- **The bot runtime** executes decisions with deterministic ordering, fees, margin, wallet state, settlement, and trade lifecycle tracking.
- **BotLens** replays and inspects what the runtime actually did.
- **RunResearchDataset** turns runtime truth into comparison-ready research data.

Reports and visualizations are views over runtime truth. They do not create alternate execution logic.

---

## Core Runtime Principle

The core runtime model is:

```text
initialize -> apply_bar -> snapshot
```

This means:

- **initialize** prepares the runtime, symbols, strategies, indicators, wallet, and execution context.
- **apply_bar** advances time one market event at a time.
- **snapshot** captures the resulting state for inspection, reporting, and debugging.

The runtime is the source of truth.

---

## Execution Modes

Quant-Trad separates execution semantics from UI playback.

### FAST

FAST mode uses the strategy timeframe candle directly.

- Uses strategy timeframe OHLC.
- Does not use intrabar execution data.
- If take-profit and stop-loss are both touched in the same candle, the pessimistic outcome wins.

FAST is useful for quick, conservative approximation.

### FULL

FULL mode uses lower-timeframe intrabar data when available.

- Uses 1-minute intrabar candles to resolve execution order.
- Falls back to pessimistic behavior when intrabar data is missing, incomplete, or still ambiguous.
- Keeps frontend animation separate from execution truth.

FULL is the higher-fidelity execution mode.

---

## What Makes This System Different

Quant-Trad is designed around a few core constraints:

- **Deterministic execution**
  Same inputs should produce the same decisions, trades, and results.

- **Known-at correctness**
  Strategies can only act on data available at that point in the runtime timeline.

- **Execution realism**
  Fees, margin, wallet state, collateral, settlement, terminal closes, and intrabar behavior are modeled explicitly.

- **Inspection-first design**
  BotLens exists to explain trades, not just display charts.

- **Dataset-first reporting**
  Research and comparison are built from canonical runtime data, not ad hoc report files.

- **Separation of concerns**
  Strategy logic, execution behavior, visualization, and reporting are separate layers.

---

## Core Components

### Strategy Layer

Strategies consume typed indicator outputs and produce decisions. Decisions can be accepted into execution or rejected with explicit reasons.

### Indicator Engine

Indicators advance through runtime time and publish typed outputs such as `signal`, `context`, and `metric`.

### Bot Runtime

The runtime owns walk-forward execution, deterministic ordering, fills, fees, margin, wallet state, trade lifecycle, and terminal run state.

### BotLens

BotLens is the inspection layer. It shows what the runtime knew and did: selected-symbol state, trade overlays, decision context, lifecycle facts, and diagnostics.

### RunResearchDataset

RunResearchDataset is the canonical research output. It summarizes decisions, trades, fees, PnL, execution mode, fallbacks, close reasons, per-symbol performance, and LLM-ready insights.

---

## Quick Start

### Prerequisites

- Docker
- GNU Make
- Python 3.12+ for local tooling outside Docker

### Create local secrets

```bash
cp secrets.env.example secrets.env
```

### Start the core stack

```bash
make up BUILD=1 STACK_PROFILES=core
```

Open:

- Frontend: `http://localhost:5173`
- Backend API: `http://localhost:8000`
- TimescaleDB: `localhost:15432`
- pgAdmin: `http://localhost:8080`

### Add observability

```bash
make up BUILD=1 STACK_PROFILES=all
```

Open:

- Grafana: `http://localhost:3000`
- Loki: `http://localhost:3100`

---

## Documentation

Start here:

- [Documentation homepage](docs/index.md)
- [Overview](docs/overview.md)
- [Getting started](docs/getting-started.md)

Core concepts:

- [Runtime timeline](docs/concepts/runtime-timeline.md)
- [Execution model](docs/concepts/execution-model.md)
- [Strategies and signals](docs/concepts/strategies-and-signals.md)
- [BotLens](docs/concepts/botlens.md)
- [Reporting datasets](docs/concepts/reporting-datasets.md)

Engineering:

- [Engineering architecture](docs/engineering/architecture.md)
- [Data layer](docs/engineering/data-layer.md)
- [Observability](docs/engineering/observability.md)
- [Architecture component index](docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md)

Guides:

- [Creating an indicator](docs/guides/creating-an-indicator.md)
- [Creating a strategy](docs/guides/creating-a-strategy.md)
- [Adding a provider](docs/guides/adding-a-provider.md)

Contracts:

- [System contract](docs/contracts/platform/00_system_contract.md)
- [Runtime contract](docs/contracts/platform/01_runtime_contract.md)
- [Execution and playback contract](docs/contracts/platform/02_execution_playback_contract.md)
- [Engineering contract](docs/contracts/platform/03_engineering_contract.md)

Contracts are the source of truth when code and explanatory docs disagree.

---

## Useful Commands

```bash
make help                            # list available commands
make up BUILD=1 STACK_PROFILES=core  # build and start core services
make up BUILD=1 STACK_PROFILES=all   # build and start core + observability
make ps                              # inspect running services
make logs SERVICE=backend            # tail backend logs
make restart BUILD=1                 # rebuild/restart current stack
make test                            # run tests
make check                           # run standard developer/audit checks
make down                            # stop and remove containers
```

---

## Project Status

Quant-Trad is in active development.

The runtime, execution semantics, reporting datasets, BotLens inspection, provider behavior, and operator workflows are still evolving. The system is intended for research, backtesting, paper trading, and controlled environments unless you have independently reviewed the execution path, provider configuration, and risk controls for your use case.

Do not treat this as production trading infrastructure without your own validation.

---
