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
# ADR 0033: Use an Out-of-Process Loki Ingress

Accepted June 1, 2026. The original implementation selected Promtail.
The native-Linux server composition now uses Grafana Alloy; local development
retains Promtail. The durable decision is one out-of-process stdout/stderr shipper
per topology, rather than an application-owned Loki transport.
Use [observability](../observability/OBSERVABILITY_BOUNDARY.md) for current wiring.

## Keep logging out of execution transport

The previous in-process Loki handler synchronously posted application log lines,
duplicated Docker scraping when both were active, and could disappear after an
early delivery failure. Runtime code consequently paid observability transport
cost and knew too much about Loki availability.

Application processes write structured stdout/stderr. Docker owns those streams;
the configured shipper owns collection and Loki delivery. A direct Loki URL does
not enable a second normal application transport.

Stable routing labels identify service and container concerns. Run and bot IDs
remain structured log fields rather than high-cardinality Loki stream labels.
The operator uses the existing `qt logs` surface for correlated searches.

## Collection lifetime is an operational limitation

A shipper cannot retroactively discover stdout from a short-lived container that
was removed before discovery. Start the observability composition before relying
on collection and make gaps explicit. Persistent Loki data and shipper positions
have distinct lifetimes; changing positions requires an explicit migration to
avoid rereading old logs.

Source log volume, filtering, retention and bounded labels remain the first
controls when pressure grows. Increasing Loki capacity cannot repair missing
source evidence or make logs canonical trading truth.

The server's Alloy adoption changes the shipper implementation, not these
ownership rules. The original Promtail choice remains historical context rather
than a claim that every topology uses that product.

References: [observability](../../engineering/observability.md),
[Docker lifecycle evidence](0022-capture-docker-container-lifecycle-as-runner-agnostic-observability.md).
