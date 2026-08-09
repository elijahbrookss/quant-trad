---
component: coinbase-perpetual-trade-fleet-validation-2026-08-06
subsystem: data
layer: operations
doc_type: validation
status: historical
tags:
  - market-data
  - collectors
  - coinbase
  - continuous
  - recovery
  - safety
  - validation
code_paths:
  - config/market_data/coinbase_perpetual_trade_fleet.v1.json
  - src/market_data/stream_enrollment.py
  - portal/backend/service/market/collector_safety.py
  - portal/backend/service/market/collector_supervisor.py
  - portal/backend/service/market/continuous_stream_collector.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/storage/repos/market_structure.py
  - docker/grafana/provisioning/alerting/collector-safety.yml
---
# Coinbase Perpetual Trade Fleet Validation — 2026-08-06

## Scope

This record proves declarative, indefinite trade collection for the Coinbase
products `BIP-20DEC30-CDE`, `ETP-20DEC30-CDE`, and `SLP-20DEC30-CDE`. It does
not authorize live order submission, derivative-performance claims, or
continuous Level 2 collection.

The applied enrollment manifest hash was
`87e20e1b8e1aa611cc66f0387d15cfde3f2da0cf7510ff95596db51aa2b1809b`.
The pinned collector-safety policy hash was
`fd37c37cfd3346f1a50c3a85d02c6d648ca28d90b9b76ad343610bf778d31aa1`.

## Enrollment Idempotency

The collector service was stopped before migrating the existing runtime
configuration to stable manifest material. The first application advanced the
three definition generations to `5`, `4`, and `4`. An immediate second
application returned the same generations. Every definition had:

- `mode: continuous`;
- `stop_at: null`;
- the same manifest and safety-policy hashes;
- no time-varying value in definition material.

The row observation timestamp may advance when an operator reapplies a
manifest, but the semantic definition generation does not.

## Controlled Restart And Recovery

At `2026-08-07T00:07:54Z`, the restarted supervisor reconstructed all three
desired tasks from the database. Each task connected to Coinbase and subscribed
to `heartbeats` followed by `market_trades` for its exact product. ETP and SLP
recovered sealed spool segments before opening their new connections.

A final image rollout at `2026-08-07T00:25:39Z` repeated the proof. All three
tasks were reconstructed with `stop_at=None`; BIP and SLP recovered sealed spool
work; and BIP, ETP, and SLP all connected and subscribed. No manifest
reapplication or generation change was required.

Recovery retained the original session evidence, published archive mappings
idempotently, and opened new causal coverage rather than bridging collector
downtime.

## Archive Progress

The first post-restart sample and a later sample produced the following
canonical counters:

| Product | Initial manifests | Later manifests | Initial records | Later records | Later archive bytes | Mapping lag |
|---|---:|---:|---:|---:|---:|---:|
| BIP-20DEC30-CDE | 286 | 302 | 22,539 | 23,071 | 4,712,475 | 0 |
| ETP-20DEC30-CDE | 7 | 24 | 3,913 | 4,533 | 660,875 | 0 |
| SLP-20DEC30-CDE | 7 | 23 | 3,879 | 4,401 | 643,124 | 0 |

At the later sample, all leases were current, all three collector heartbeats
were advancing, and every stream remained `continuous` with `stop_at: null`.

## Safety And Alerting

Persistent safety-state database tests proved idempotent halt and explicit
acknowledgement behavior. The live fleet had no active safety latch. Grafana
loaded the warning and critical rules from
`docker/grafana/provisioning/alerting/collector-safety.yml`; its startup log
recorded successful alert provisioning from `2026-08-07T00:09:11Z` through
`00:09:14Z`.

Warnings remain visible without stopping collection. Critical evidence latches
the applicable stream, fleet, or global scope and drains work through the normal
collector shutdown boundary. Restart does not clear a latch.

## Validation Results

- Repository suite without opt-in database tests: `1,822 passed`, `48 warnings`.
- Market-structure database integration module: `4 passed`, `6 warnings`.
- Frontend market-posture Node tests: `3 passed`.
- Capability-nomenclature contract: passed across active `src`, backend,
  frontend source, CLI, and configuration paths.
- Documentation architecture-index contract: passed as part of the repository
  suite.

Warnings were dependency deprecations already present in the repository; no
test or validation failure remained.
