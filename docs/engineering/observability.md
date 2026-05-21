# Observability

Observability is part of Quant-Trad product behavior. It is how operators trace research, decisions, execution, BotLens, and reports.

## What It Is

Quant-Trad uses structured logs, runtime events, metrics, diagnostics, BotLens read models, Grafana, and Loki to explain system behavior.

Instrumentation should answer concrete questions:

- What run, bot, strategy, symbol, timeframe, provider, or trade was affected?
- Which lifecycle phase did the system enter?
- Why was a decision accepted or rejected?
- Why did execution fall back or degrade?
- Which projection, queue, provider fetch, or stream became unhealthy?

## Logs, Metrics, Events

- Logs describe lifecycle boundaries, decisions, warnings, and errors.
- Metrics describe counts, depth, latency, payload size, and throughput.
- Runtime events describe canonical runtime facts for BotLens, reporting, and replayable inspection.

Do not emit a log or metric because it might be useful someday. Tie it to a contract or a concrete diagnostic question.

## BotLens Diagnostics

BotLens is a primary inspection surface. Projection readiness, selected-symbol state, trade overlays, runtime events, execution mode, fallback warnings, and candle continuity summaries should stay visible and explicit.

## Grafana And Loki

Grafana and Loki provide stack-level dashboards and log inspection. They complement BotLens but do not replace durable runtime facts or report datasets.

## Error Posture

Quant-Trad should fail loudly for invalid states. A fallback is allowed only when it is modeled, visible, and logged with enough context to investigate.

## Next

- Full doctrine: [observability doctrine](observability-doctrine.md).
- Engineering contract: [engineering contract](../contracts/platform/03_engineering_contract.md).
- Runtime event storage: [persistence boundary](../architecture/persistence/PERSISTENCE_BOUNDARY.md).
- BotLens diagnostics: [BotLens projection boundary](../architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md).
- Observability boundary: [observability boundary](../architecture/observability/OBSERVABILITY_BOUNDARY.md).
