# Engineering Architecture

This page is a short implementation map. Use it before jumping into component-level architecture docs.

## High-Level Shape

```text
Providers -> Market data services -> Indicator runtime -> Strategy evaluator
          -> Bot runtime -> Storage/read models -> Backend API
          -> Frontend/BotLens/Reports
          -> Logs, metrics, runtime events, Grafana, Loki
```

## Main Layers

- `src/data_providers/`: provider adapters, venue metadata, credential access, and provider factory wiring.
- `src/engines/indicator_engine/`: typed indicator runtime contract and per-bar execution.
- `src/indicators/`: indicator implementations, manifests, overlays, and runtime output builders.
- `src/strategies/`: strategy contracts, compiler, templates, and evaluator.
- `src/engines/bot_runtime/`: walk-forward execution, wallet, fees, margin, settlement, runtime events, and read models.
- `portal/backend/`: FastAPI controllers, runtime orchestration, storage repositories, BotLens projection, reports, and provider services.
- `portal/frontend/`: operator UI, BotLens, bot fleet views, strategy/report surfaces.
- `docker/`: local stack, database, observability, and service containers.

## Runtime Relationship

The backend starts and supervises runtime work. The runtime owns execution semantics. Storage keeps durable facts and read models. Frontend surfaces inspect those facts through API and stream contracts.

Do not move execution truth into the frontend, report builders, or playback views.

## Storage Relationship

Runtime persistence uses the shared `PG_DSN`. New persistence layers should use that DSN directly and should not introduce extra DSN environment variables or mapper layers.

## Observability Relationship

Runtime diagnostics are product behavior. Logs, metrics, runtime events, BotLens diagnostics, Grafana, and Loki should make it possible to trace QuantLab -> Strategy -> Bot -> Trades -> Playback without hiding failures.

## Next

- Runtime internals: [runtime engine](runtime-engine.md).
- Data/provider behavior: [data layer](data-layer.md).
- Observability behavior: [observability](observability.md).
- System model: [system architecture model](../architecture/system/SYSTEM_MODEL.md).
- Deep component lookup: [architecture component index](../architecture/ARCHITECTURE_COMPONENT_INDEX.md).
