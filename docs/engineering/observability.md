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

## Alerts And Notifications

Grafana is the alert engine and notification router. Quant-Trad does not turn
every log row into a page or maintain a parallel delivery service. A rule
evaluates logs, database state, or metrics into Normal, Pending, Firing, and
Resolved state; the provisioned policy groups that state and sends it through
the optional `qt-operator-email` contact point.

This distinction matters operationally: a disconnect is an event, sustained
coverage loss is an alert, an email is a notification, and an incident is the
response to material impact. Rule thresholds should describe actionable bad
state and recovery, not raw event volume.

Email delivery uses an operator-selected managed transactional relay. Grafana
OSS still needs one installation-owned sender credential, but operators do not
run an SMTP server or use personal mailbox credentials. After one-time setup,
recipient ownership is the single `QT_ALERT_EMAILS` environment value.

Provisioned rules, contact points, and policies are reviewed source. The
bounded V1 standard and full setup/test/rollback procedure are in the
[operator email alerting runbook](../operators/alerting.md).

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

## Storage Closeout Operations

The capacity dashboard (`/d/quanttrad-capacity-growth`) starts with physical
SSD/HDD use, publication/verification/retirement windows, eligible backlog,
measurement availability, canonical read errors and latency, database allocation,
and the last completed paired recovery point. The existing Workers and Runtime
Control Tower dashboards show collection, acquisition, gaps and research jobs.
No Prometheus service is required: PostgreSQL rollups and Loki/Alloy remain the
existing monitoring path.

Every five minutes, the existing capacity sampler records bounded catalog
metadata and fixed-label gauges through the existing observer exporter. It does
not walk archive objects or count historical fact rows. Unknown counts remain
unknown; a known backlog lower bound is distinct from a complete estimate.
`storage_admitted_revisions_available=0` explicitly marks the missing universal
post-commit admission counter. Sequence advances and PostgreSQL tuple inserts
must not be presented as exact accepted revisions. Staging remains visible in
maintenance phases/catalog state rather than a fabricated exact admission rate.

Published/verified byte windows describe manifest objects. Retired relation-byte
windows describe completed retirement records. Neither is net physical SSD
reclaim: use the independently observed mount bytes for that. Allocation by
family/tablespace is inferred main-fork allocation and excludes unmeasured
forks; it is not a filesystem total. Missing or clipped snapshots do not become
zero. Rolling five-minute windows must not be summed as disjoint counters.

Canonical read counters and timings use fixed tier, operation and outcome
labels. Iterator timing excludes time the caller spends between requests for
rows. Mean latency is weighted by sample counts; maxima remain maxima. Percentile
rollups cannot be merged into a global percentile. The research worker starts
and drains the same exporter used by the API. Existing capacity records retain
30 days and Loki retains seven days. Backend metric rollups have no automatic
retention job at present; do not claim a 30-day bound for those rows.

Storage safety alerts include the 60% SSD warning and 65% normal operating
ceiling, archival/verified-retirement stalls with eligible work, paired recovery
freshness after 25 hours, and repeated collection/acquisition failures. Existing
missing-device, collector safety and higher-capacity alerts remain active.
Missing essential evidence alerts rather than reporting healthy zero progress.
A recovery snapshot reads completed receipt metadata before pruning; it does
not certify that the current backup has completed. Same-HDD copies protect
rollback but remain exposed to HDD loss.

Interactive research uses one serialized research worker at lower CPU priority
(nice +10); backtest containers and API work use reduced CPU shares. This is
relative priority under contention, not a hard throughput or memory guarantee.
Native recovery tools receive explicit Go GC/scheduler targets (`GOMEMLIMIT=4GiB`,
`GOMAXPROCS=2`); maintenance retains its existing 8 GiB container limit. These Go
targets do not cap subprocess RSS. Check actual memory events, normal recovery
completion and collector publication after deployment. High-volume autonomous
research schedules remain paused until explicitly resumed.

After changing provisioning files on the native server, deploy the reviewed
feature commit normally, then use `server_deploy.sh apply-alerts` to remount and
reload the existing Grafana/Alloy configuration. Verify current samples through
Grafana, actual rule evaluation state, research-worker metric export and normal
maintenance progress; source changes alone are not operational acceptance.

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
