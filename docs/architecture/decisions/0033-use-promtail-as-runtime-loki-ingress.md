---
component: adr-runtime-loki-ingress
subsystem: observability
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - observability
  - loki
  - promtail
  - runtime
  - logging
code_paths:
  - src/core/logger.py
  - src/utils/logging_utils.py
  - cli/logs.py
  - docker/docker-compose.yml
  - docker/docker-compose.server.yml
  - docker/promtail/config.yml
  - docker/alloy/config.alloy
  - docker/loki/config.yml
  - docker/loki/server-config.yml
  - docs/architecture/observability/OBSERVABILITY_BOUNDARY.md
  - docs/engineering/observability.md
---
# ADR 0033: Use Promtail as Runtime Loki Ingress

## Status

Accepted on 2026-06-01.

Amended for the native-Linux server on 2026-08-20. Grafana Promtail reached end
of life on 2026-03-02, so Grafana Alloy now owns the same single Docker-to-Loki
ingress role in `docker-compose.server.yml`. The architectural decision remains
one out-of-process stdout/stderr shipper and no in-process Loki hot path; the
local development composition retains Promtail only as historical tooling.

Clarified on 2026-08-24: the 2026-08-20 description of local Promtail as
historical tooling is superseded. Promtail remains supported for the local
development composition, while Grafana Alloy is the supported native-Linux
server shipper. Promtail-specific statements in the original decision therefore
apply to local development; Alloy occupies the same role on the native server.
The invariant is unchanged: one normal out-of-process shipper per topology and
no in-process Loki hot path.

## Context

Runtime incident investigation depends on Loki, but the runtime had two
competing paths into it:

- Python processes could post each log line directly to Loki through an
  in-process handler.
- Docker stdout/stderr could be scraped by Promtail into Loki.

The direct handler was fragile for bot-runtime failures. It posted
synchronously from the logging path, duplicated Promtail-ingested lines when
both paths were active, and disabled itself after a transport error. If Loki was
down during process startup, the handler could disappear for the rest of that
process. It also encouraged high-volume runtime code to know about Loki
directly.

The failed runtime investigation showed the cleaner boundary: Docker already
owns process stdout/stderr, Promtail owns shipping container logs, and Loki owns
queryable retention. If Promtail is not running while a short-lived bot
container starts and exits, and that container is later removed, Docker service
discovery cannot retroactively discover it.

## Decision

Promtail is the primary and only normal Loki ingress for backend and bot runtime
application logs.

Runtime and backend processes write structured logs to stdout/stderr. The
runtime logger appends process-level context from environment variables to every
bot-runtime log line:

- `request_id`,
- `bot_id`,
- `run_id`,
- `service=bot-runtime`,
- `runtime=bot`,
- `source_revision` when available.

Promtail scrapes Docker logs for containers labeled `loki.job=quanttrad`.
Promtail indexes stable routing labels such as `job`, `service`, `runtime`,
`container`, and `compose_service`. It does not index `run_id` or `bot_id` as
Loki labels. Run identity remains in the structured log line and is searched by
`qt logs`.

The in-process Loki handler is no longer configured by the default runtime or
dev stack. If `logging.loki_url` is set, the logger emits a warning that direct
Loki logging is disabled because Promtail stdout ingestion is the contract.

Loki data uses a named Docker volume. Local Loki retention and query lookback
are bounded to seven days by default. Promtail keeps its current positions path;
moving that path requires an explicit positions migration because an empty
positions file makes Promtail reread old Docker logs.

`qt logs` is the operator surface:

- `qt logs run <run_id>` searches runtime, backend, and Docker-event streams by
  structured run context,
- `qt logs query '<logql>'` runs raw LogQL,
- `qt logs doctor` checks Loki readiness and recent Quant-Trad label visibility.

## Consequences

- Bot-runtime logging no longer performs synchronous Loki HTTP calls from the
  runtime process.
- Loki ingestion has one normal path, which removes duplicate app/runtime log
  ingestion.
- Run investigations stay Loki-centered without teaching operators ad hoc curl
  and label-discovery steps.
- `run_id` and `bot_id` remain searchable without becoming high-cardinality
  Loki stream labels.
- Observability must be running before short-lived runtime containers fail if
  their stdout/stderr logs are expected in Loki.
- If runtime log volume grows again, the first levers are source-level log
  culling, log-level tightening, Promtail pipeline filtering, retention/window
  tuning, and then Loki resource scaling.

## References

- [Grafana Promtail deprecation and EOL](https://grafana.com/docs/loki/latest/send-data/promtail/)
- [Monitor Docker containers with Grafana Alloy](https://grafana.com/docs/alloy/latest/monitor/monitor-docker-containers/)
- [Observability Boundary](../observability/OBSERVABILITY_BOUNDARY.md)
- [Engineering Observability](../../engineering/observability.md)
- [ADR 0022: Capture Docker Container Lifecycle as Runner-Agnostic Observability](0022-capture-docker-container-lifecycle-as-runner-agnostic-observability.md)
