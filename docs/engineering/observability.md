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

Application logs enter Loki through Docker stdout/stderr scraped by Promtail.
Backend and bot-runtime processes should write structured logs to their normal
process stream; ordinary runtime logging must not post directly to Loki from the
hot path.

Bot-runtime log lines include process context from environment variables:
`request_id`, `bot_id`, `run_id`, `service=bot-runtime`, `runtime=bot`, and
`source_revision` when available. Promtail indexes bounded routing labels such
as `job`, `service`, `runtime`, `container`, and `compose_service`. It does not
index `run_id` or `bot_id`; those remain searchable structured fields in the log
line.

Use `qt logs run <run_id>` for run-centered Loki inspection. The command
queries Loki, parses Quant-Trad structured log context into fields, and also
pulls nearby bot lifecycle lines when it can derive or is given `--bot-id`.
Use `qt logs query '<logql>'` when the investigation needs a raw LogQL shape.
Use `qt logs doctor` to check Loki readiness and whether recent backend,
bot-runtime, and Docker-event streams are visible.

If Loki misses a bot-runtime failure, check whether the observability profile
was running before the bot container exited. Promtail discovers live Docker
containers; it cannot scrape stdout/stderr from a removed short-lived runtime
container after the fact.

Local Loki is intentionally bounded. The dev stack keeps Loki data on a named
volume and defaults to a seven-day retention/query lookback window. If log volume
starts to pressure Loki, prefer these levers in order:

1. Cull noisy source logs and lower DEBUG usage around hot loops.
2. Add Promtail pipeline filtering for known low-value lines.
3. Tune retention/lookback and the Docker volume size.
4. Increase Loki resources or move to a larger Loki deployment.
5. Add labels only for stable routing dimensions; avoid `run_id` and `bot_id`
   labels unless measured query needs justify the cardinality cost.

## Error Posture

Quant-Trad should fail loudly for invalid states. A fallback is allowed only when it is modeled, visible, and logged with enough context to investigate.

## Next

- Full doctrine: [observability doctrine](observability-doctrine.md).
- Engineering contract: [engineering contract](../contracts/platform/03_engineering_contract.md).
- Runtime event storage: [persistence boundary](../architecture/persistence/PERSISTENCE_BOUNDARY.md).
- BotLens diagnostics: [BotLens projection boundary](../architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md).
- Observability boundary: [observability boundary](../architecture/observability/OBSERVABILITY_BOUNDARY.md).
