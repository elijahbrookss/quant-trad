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

Application logs enter Loki through Docker stdout/stderr and one
out-of-process shipper. The native-Linux server uses Grafana Alloy; the local
development composition uses its supported Promtail service. Each topology
runs exactly one of those shippers. Never run Promtail and Alloy against the
same application-container streams or otherwise double scrape them. Backend
and bot-runtime processes write structured logs to their normal process stream;
ordinary runtime logging must not post directly to Loki from the hot path, and
shipper absence must not activate an in-process fallback.

Bot-runtime log lines include process context from environment variables:
`request_id`, `bot_id`, `run_id`, `service=bot-runtime`, `runtime=bot`, and
`source_revision` when available. The shipper indexes bounded routing labels
such as `job`, `service`, `runtime`, `container`, and `compose_service`. It does
not index `run_id` or `bot_id`; those remain searchable structured fields in
the log line.

Use `qt logs run <run_id>` for run-centered Loki inspection. The command
queries Loki, parses Quant-Trad structured log context into fields, and also
pulls nearby bot lifecycle lines when it can derive or is given `--bot-id`.
Use `qt logs query '<logql>'` when the investigation needs a raw LogQL shape.
Use `qt logs doctor` to check Loki readiness and whether recent backend,
bot-runtime, and Docker-event streams are visible.

If Loki misses a bot-runtime failure, check whether the log shipper was running
before the bot container exited. Docker discovery cannot recover stdout/stderr
from a removed short-lived runtime container after the fact.

Local Loki is intentionally bounded. The dev stack keeps Loki data on a named
volume and defaults to a seven-day retention/query lookback window. If log volume
starts to pressure Loki, prefer these levers in order:

1. Cull noisy source logs and lower DEBUG usage around hot loops.
2. Add shipper pipeline filtering for known low-value lines.
3. Tune retention/lookback and the Docker volume size.
4. Increase Loki resources or move to a larger Loki deployment.
5. Add labels only for stable routing dimensions; avoid `run_id` and `bot_id`
   labels unless measured query needs justify the cardinality cost.

## Capacity And Growth Dashboard

The observability profile provisions **QuantTrad Capacity & Database Growth**
at Grafana UID `quanttrad-capacity-growth`. It includes:

- PostgreSQL database size, growth rate, WAL rate, connections, cache hit, and
  sampler cost;
- logical schema and table/hypertable size, row estimates, insert rate, and
  growth leaderboards with schema/relation filters;
- per-container CPU and memory plus authority-labeled Docker engine filesystem
  used/free capacity from the `docker-stats` sidecar;
- optional physical Docker Desktop backing-volume capacity, VHDX allocation
  growth, reserve, and projected days-to-reserve from
  `scripts/reporting/host_capacity_sampler.ps1` (Docker settings determine the
  volume; no drive letter is hardcoded).

Database and logical-relation samples run every five minutes and retain 30 days
by default. Docker samples run every 15 seconds and follow Loki's local
seven-day retention. The two cadences intentionally answer different questions:
short spikes stay visible in Loki, while table growth remains cheap enough to
retain for planning.

Before creating alerts, measure at least one representative workload. Never use
Docker Desktop/WSL guest free space as physical-host headroom; check the
dashboard authority panel and require `physical_host_visible=true`.
Useful starting candidates are filesystem free below 20%, database connections
above 70% of `max_connections`, cache hit below 95%, and sustained database or
relation growth above the measured storage budget. These are starting points,
not universal thresholds; tune them from the observed workload.

On Windows Docker Desktop, install the authority sampler for continuous use
from PowerShell with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/reporting/host_capacity_sampler.ps1 -InstallScheduledTask
```

The user-level task starts immediately, starts again at logon, restarts after
failure, writes one bounded daily NDJSON file under `logs/host-capacity`, and is
removed with the same command plus `-RemoveScheduledTask`.

## Grafana Dashboard Source And Recovery

Checked-in JSON under `docker/grafana/provisioning/dashboards/` is the supported
dashboard source. Grafana polls that directory every 10 seconds with
`allowUiUpdates: false`, so UI edits to provisioned dashboards are not the
durable workflow. In local development, allow one polling interval for a
checked-in JSON change; if the observability stack is stopped or needs to be
reconciled, run `make STACK_PROFILES=observability stack-restart`. On a native
server, deliver dashboard changes through
`bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>` so the
mounted provisioning source comes from the reviewed release.

The tracked `scripts/backup-grafana-dashboards.sh` script can export dashboard
models from a reachable Grafana API when Bash, `curl`, `jq`, valid Grafana
credentials, and a writable output directory are available. Its output is a
candidate source change that requires diff review; it does not delete stale
JSON, automate scheduling, or prove that a restore succeeded. There are no
supported `make grafana-backup` or `make grafana-restore` targets. See the
[dashboard provisioning workflow](../../docker/grafana/provisioning/dashboards/README.md)
for the exact backup and reload boundaries.

## Error Posture

Quant-Trad should fail loudly for invalid states. A fallback is allowed only when it is modeled, visible, and logged with enough context to investigate.

## Next

- Historical context: [retained observability doctrine](observability-doctrine.md).
- Engineering contract: [engineering contract](../contracts/platform/03_engineering_contract.md).
- Runtime event storage: [persistence boundary](../architecture/persistence/PERSISTENCE_BOUNDARY.md).
- BotLens diagnostics: [BotLens projection boundary](../architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md).
- Observability boundary: [observability boundary](../architecture/observability/OBSERVABILITY_BOUNDARY.md).
