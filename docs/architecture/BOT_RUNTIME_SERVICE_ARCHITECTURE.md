# Bot Runtime Service Architecture

## Documentation Header

- `Component`: Bot runtime service orchestration (API -> runner -> container runtime -> storage/telemetry)
- `Owner/Domain`: Bot Runtime / Portal Backend
- `Doc Version`: 1.0
- `Related Contracts`: `docs/agents/01_runtime_contract.md`, `docs/architecture/BOT_RUNTIME_ENGINE_ARCHITECTURE.md`, `docs/architecture/BOT_RUNTIME_SYMBOL_SHARDING_ARCHITECTURE.md`, `docs/architecture/BOTLENS_LIVE_DATA_ARCHITECTURE.md`, `portal/backend/service/bots/runtime_control_service.py`, `portal/backend/service/bots/runner.py`, `portal/backend/service/bots/container_runtime.py`

## 1) Problem and scope

This document defines how a bot run is started, executed, observed, and stopped at the service layer.

In scope:
- bot API + service start/stop lifecycle,
- runner target resolution,
- container runtime process model,
- snapshot and event persistence boundaries,
- watchdog ownership/heartbeat expectations.

### Non-goals

- signal/indicator algorithm internals,
- strategy authoring semantics,
- exchange-specific live order execution details.

Upstream assumptions:
- bot config, strategy config, and instrument metadata are valid,
- `PG_DSN` is configured and reachable,
- `PROVIDER_CREDENTIAL_KEY` is configured identically for backend and bot runtime containers,
- runtime event/snapshot contracts are stable.

## 2) Architecture at a glance

Boundary:
- inside: portal bot services, runner, container runtime, storage repositories, telemetry ingest
- outside: UI/API clients, Docker daemon, exchange/provider services

```mermaid
flowchart LR
    A[API /bots/{id}/start] --> B[BotRuntimeControlService]
    B --> C[Runner Target Resolution]
    C --> D[DockerBotRunner]
    D --> E[container_runtime.py process]
    E --> F[BotRuntime workers]
    F --> G[Snapshot + Runtime Event persistence]
    F --> H[Telemetry WS ingest]
    G --> I[BotLens bootstrap/stream]
```

## 3) Startup sequence

1. API start request enters `runtime_control_service.start_bot`.
2. Service validates:
- wallet config,
- strategy existence,
- instrument policy,
- runtime readiness.
3. Runner target is resolved from `BOT_RUNTIME_TARGET`.
4. Current implementation supports only `docker` target.
5. `DockerBotRunner.start_bot(...)` launches `python -m portal.backend.service.bots.container_runtime` in a bot container.
6. Container runtime creates a `run_id`, assigns one symbol per worker process, and starts workers.
7. Container runtime emits periodic merged snapshots to storage and telemetry.
8. Bot status is updated as `running`, then `stopped`/`failed` at terminal boundaries.

## 4) Runner types and runtime modes

Two different axes exist and should not be conflated.

Runner target (infrastructure):
- `BOT_RUNTIME_TARGET=docker` -> process is launched in a Docker container.
- Additional targets are not implemented yet.

Run type (execution mode passed to runtime):
- API currently accepts `backtest` and `sim_trade`.
- Runtime policy treats `sim_trade`/`paper`/`live` as live-refresh-capable modes.
- Strategy execution adapter mapping supports `backtest`, `paper`, and `live` modes internally.

## 5) Worker model inside container runtime

- One container run can manage multiple symbols for one strategy.
- Symbols are assigned one-per-worker process (`process-per-series`).
- Each worker runs a `BotRuntime` instance scoped to exactly one symbol.
- Runtime uses inline series execution inside each worker (no pool runner).
- Container runtime merges worker snapshots into one bot-level snapshot envelope.
- Worker failures degrade symbol subsets while healthy symbols continue.

## 6) Persistence and telemetry

Writes:
- snapshots -> `portal_bot_run_snapshots` via `record_bot_run_snapshot(...)`
- runtime events -> `portal_bot_run_events` via `record_bot_runtime_event(...)`
- bot/run status -> bot + run records via `update_bot_runtime_status(...)`

Transport:
- optional WS telemetry push to backend ingest endpoint,
- BotLens consumers read via bootstrap + stream.

Durability model:
- writes are primary source of truth,
- websocket delivery is supplemental real-time transport.

## 7) Connection liveness guards and retry behavior

Database engine liveness guards are enabled by default:
- `pool_pre_ping`: validates pooled connection health before use,
- `pool_recycle`: rotates long-lived pooled connections,
- TCP keepalive connect args: detects dead sockets earlier.

Runtime write retry behavior:
- snapshot/event/status writes perform bounded retry for transient connection-loss errors,
- retries reset DB connection state and re-open clean connections,
- non-transient or exhausted retries still fail loud.

Config knobs:
- `PG_POOL_PRE_PING` (default: enabled)
- `PG_POOL_RECYCLE_SECONDS` (default: 900)
- `PG_POOL_TIMEOUT_SECONDS` (default: 30)
- `PG_CONNECT_TIMEOUT_SECONDS` (default: 5)
- `PG_TCP_KEEPALIVE_ENABLED` (default: enabled)
- `PG_TCP_KEEPALIVE_IDLE_SECONDS` (default: 30)
- `PG_TCP_KEEPALIVE_INTERVAL_SECONDS` (default: 10)
- `PG_TCP_KEEPALIVE_COUNT` (default: 3)
- `PORTAL_DB_WRITE_RETRY_ATTEMPTS` (default: 2)

## 8) Watchdog responsibilities

- assigns stable `runner_id` for service instance,
- emits bot heartbeats,
- detects stale heartbeats/orphaned runs,
- verifies running bot rows still map to live containers,
- marks orphaned/crashed bots and clears ownership when needed.

## 9) Strict contract

- Start/stop operations must remain explicit and auditable.
- Runtime bootstrap/prepare is explicit (`warm_up`/`start`) and single-flight; read paths must not trigger prepare as a side effect.
- Runtime status transitions must be persisted with `run_id` context.
- Snapshot/event sequencing must remain monotonic and causal.
- Degrade behavior must isolate failed symbols without inventing success state.
- Transient DB connectivity may be retried, but terminal failures must still surface as errors.

## 10) Operational notes

- If bot start fails before container launch, bot status is set to `error` with artifact context.
- If DB disconnect occurs during runtime writes, bounded retry attempts recovery.
- If retries fail, the run fails loud (container runtime exits and status transitions to failed/crashed path).

## 11) Runtime Capacity Endpoint

- `GET /api/bots/runtime-capacity` exposes host CPU core count and estimated active bot worker usage.
- Response includes: `host_cpu_cores`, `workers_in_use`, `workers_requested`, `running_bots`, `over_capacity_workers`, `in_use_pct`, `updated_at`.
- Frontend Bot panel renders a compact `CPU in_use/max` indicator near bot search/count controls.
