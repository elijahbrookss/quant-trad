---
title: Collector Operations Discovery
status: historical
last_verified: 2026-08-10
---

# Collector Operations Discovery

> Historical evidence record. "Current," "live," and "production" descriptions
> below are observations from the 2026-08-10 pre-implementation inventory, not
> current fleet state.

## Scope and evidence boundary

This report is the pre-implementation inventory for the collector operations
campaign. It traces the current source at `52ebb6a`, the live PostgreSQL catalog
and evidence as observed on 2026-08-10, the stopped collector container's final
logs, the `qt`/HTTP/MCP surfaces, and Frontend V2.

Implementation and live failure/recovery evidence are recorded separately in
the [collector operations validation](collector-operations-validation.md). This
document remains the immutable before-state audit.

The collector control plane covers durable, registered market-data producers.
It does not absorb every provider connection:

- bounded candle, numeric-Fact, and market-structure acquisitions remain
  explicitly authorized acquisition jobs;
- bounded Level 2 capture remains a capture operation because there is no
  registered continuous L2 adapter;
- paper and observe-only streams are run-scoped Bot runtime components and
  remain under run/BotLens lifecycle ownership;
- provider smoke probes are diagnostics, not collectors;
- the co-located storage-lifecycle supervisor is a maintenance service, not a
  Fact producer.

These exclusions must remain visible in diagnostics, but the collector console
must not acquire authority over them by relabeling them as fleet collectors.

## Implemented collector inventory

### Scheduled canonical-Fact collectors

All scheduled definitions use `market.collection_definitions`, one serial
`market_data_collector` worker loop, PostgreSQL claims and provider pacing,
`market.collection_attempts`, the canonical `market.fact_versions` store, and
`market.gap_evidence`.

| Implementation | Provider and access | Canonical output and subjects | Cadence/history | Startup, retry, recovery, and gaps |
| --- | --- | --- | --- | --- |
| Coinbase open interest | Coinbase Advanced Trade public product read | `derivatives.open_interest.v1`; one explicitly mapped Coinbase instrument/product per definition | Default 60-second poll; current-only; history starts at enablement | `enabled=true` makes due work claimable; three attempts by default with bounded exponential backoff; expired claims are fenced; missed schedules collapse into explicit missed-attempt and gap evidence; restart resumes from the durable next schedule |
| Coinbase funding | Coinbase Advanced Trade public product read | `derivatives.funding_rate.v1`; one explicitly mapped funding-capable instrument/product per definition | Default 60-second poll; current-only; history starts at enablement | Same scheduler contract as OI; provider funding time is retained but QT receipt/acceptance owns `known_at` |
| Chainlink MVR reserve state | Chainlink public Arbitrum RPC, endpoint supplied by `CHAINLINK_ARBITRUM_RPC_URL` only at acquisition | `asset.reserve_state.v1`; checked-in `nxtAssets` DE000NXTA018 binding, atomic BTC reserve report | Manifest polls hourly, expects 12-hour updates, and treats the feed as current-only | Same fenced scheduled worker; identical latest bundle is an idempotent no-op; stale/provider failures become attempts and gap evidence; no pre-collector MVR history is claimed |

The implementation has no generic scalar/structured split after
canonicalization. The split is operational only: provider construction and
normalization differ at the acquisition boundary.

Current live catalog evidence contains ten enabled scheduled definitions: the
six intended Coinbase BIP/ETP/SLP OI/funding schedules and four historical DB
test definitions. No Chainlink structured definition is currently installed.
The test definitions and a stopped `test.v1` worker row show that prior database
tests used the operational database instead of an isolated database. A future
worker start would treat all ten enabled rows as desired work.

### Continuous Coinbase trade collectors

The only registered indefinite adapter is
`coinbase.market_structure_trades.v1`. It supports exactly ordered
`market_trades,heartbeats` definitions. The checked-in fleet manifest declares
BIP-20DEC30-CDE, ETP-20DEC30-CDE, and SLP-20DEC30-CDE.

| Concern | Current contract |
| --- | --- |
| Canonical outputs | `market.trade.v1`, `market.trade_flow.v1` at 1s/60s, and derived `market.trade_flow_feature.v1` when its series mapping exists |
| Ownership | One supervisor task and one PostgreSQL stream lease per definition; definition generation plus owner/token/lease generation fence every durable write |
| Startup | The worker reads enabled definitions whose `collector_runtime.mode` is `validation` or `continuous`, evaluates safety qualification, resolves exactly one adapter, recovers orphaned spool segments, then opens a new provider session |
| Stop/restart | Stop changes desired configuration; the task closes the socket, seals and drains finalizers, records terminal evidence, then releases the lease. Failed continuous tasks restart with bounded exponential delay |
| Persistence | Provider frames are fsynced to a bounded spool, archived immutably, mapped, canonicalized, and covered by append-only session, quality, archive, and coverage evidence |
| Gaps and recovery | Connection epochs and coverage intervals prevent invented continuity. Recovery reclaims durable spool work idempotently and closes prior coverage at its last proven event |
| Enablement | `stream_definitions.enabled` plus the code-owned runtime mode; system-derived safety latches can disable desired work and require explicit acknowledgement |
| Historical support | Forward collection plus durable spool recovery only; bounded capture/replay is a separate explicitly invoked operation |

The live database contains three enabled continuous production definitions,
two stopped spot trade definitions, additional unconfigured bounded trade/L2
definitions, and many historical test definitions. Level 2 definitions are not
continuous collectors because the adapter registry intentionally rejects them.

### Other live provider paths that remain outside collector fleet ownership

`PaperMarketStreamRunner` and the observe-only runtime open Coinbase streams for
one Bot run. They have run-scoped reconnect/status evidence and do not own a
durable collector definition. Historical candle reads, Chainlink
AggregatorV3 numeric acquisition, bounded trade/L2 capture, archive replay, and
provider stream smoke checks are bounded operations. They must be linkable from
diagnostics where useful but must not receive fleet start/stop semantics.

## Current operational surfaces

| Surface | Scheduled facts | Continuous trades | Gap/recovery evidence | Mutations/audit |
| --- | --- | --- | --- | --- |
| Worker | One heartbeat row plus active scheduled definition/attempt; continuous supervisor snapshot embedded in worker JSON context | In-memory task state copied into the worker heartbeat every ten seconds | Scheduled gaps and attempts; continuous session/quality/coverage/archive evidence | Worker logs only for lifecycle transitions |
| HTTP API | catalog, snapshot/SSE, enable/disable, attempts, bounded recent facts | separate definition/session/status snapshot/SSE, validate/start/stop, safety, capture/replay/reconcile | split across fact history, attempts, market-structure status, sessions, safety, and retention routes | Definition toggles and continuous start/stop overwrite mutable state; only safety events have an immutable actor/request record |
| `qt` CLI | create Coinbase/structured definitions, list, enable/disable, attempts | enroll, validate/start/stop, evidence, safety, capture/replay/reconcile | available through several provider/data-family-shaped commands | no common action vocabulary or common operation history |
| MCP | no collector resources or tools | no collector resources or tools | none | none |
| Frontend V2 | stream-first scheduled snapshot, grouped table, read-only lens with status/facts/attempts/quality | separate market-structure posture table only | scheduled lens says the gap catalog is unavailable; continuous evidence has no collector lens | GET-only; no safe collector action surface |
| Metrics/logs | attempt timing contains schedule, pacing, provider, validation, normalization, heartbeat, and persistence stages; structured lifecycle logs | counters exist in task memory/result logs; safety Grafana alerts exist | raw durable evidence is rich but not aggregated into one operator contract | no canonical operation event |

## Operational issues proven by the audit

1. **No canonical fleet contract.** Scheduled and continuous definitions expose
   different identities, lifecycle vocabularies, health calculations, detail
   reads, and controls. Frontend code currently derives scheduled health itself.
2. **Continuous fleet restart storm.** The three production trade definitions
   were enabled but lacked `flow_feature_series_ids`. The current enrollment
   writer provisions only aggregate series even though the continuous finalizer
   requires both aggregate and flow-feature series. Logs show repeated
   `continuous_collector_flow_config_missing`, finalizer failure, recovery, and
   restart cycles (restart counts above 440 per definition near shutdown).
3. **Malformed observations can poison finalization.** Coinbase emitted
   `UNKNOWN_ORDER_SIDE`; translation failed during terminal drain. The archive
   survived, but there is no canonical reject counter/diagnostic or bounded
   quarantine decision visible to an operator.
4. **The worker is stopped while desired work remains enabled.** The durable
   heartbeat expired on 2026-08-08, while all ten scheduled definitions and
   three production continuous definitions still declare desired work. Existing
   screens show pieces of this disagreement but no one explicit actual state.
5. **Recovery evidence is noisy rather than explanatory.** The live database
   contains thousands of recovery/failure session events. There is no folded
   incident, active retry state, likely failing boundary, or recommended safe
   action.
6. **No mutation audit for normal operations.** Scheduled enable/disable and
   continuous validate/start/stop retain at most the latest requester inside a
   mutable config. They do not preserve prior state, result, request identity,
   evidence, or failure.
7. **No pause/resume/restart semantic.** Scheduled disable and continuous stop
   are family-specific approximations. Neither the API nor operator sees a
   common graceful lifecycle command.
8. **No unified telemetry.** Throughput, accepts/noops/rejects, freshness,
   provider delay, gaps, retries, restart count, storage growth, and write
   latency can often be reconstructed from different tables/logs but are not a
   bounded canonical projection.
9. **No first-class diagnostics.** There is no operation that probes worker,
   scheduler, provider, canonicalization/schema, persistence, fencing, gap, and
   freshness boundaries and reports the likely failing boundary.
10. **Frontend coverage is partial.** The existing Market Lens covers scheduled
    facts only, derives business health in JavaScript, cannot inspect structured
    payloads generically, cannot inspect gaps/events/diagnostics, and cannot
    operate collectors. Continuous definitions have no routed detail view.
11. **MCP and common CLI operations are absent.** Agent workflows must know the
    two legacy command families and cannot inspect the same contract the UI
    needs.
12. **Catalog hygiene is unproven.** Historical test definitions and worker rows
    remain in the live database. The console needs honest code-owned/registered
    identity and tests must use isolated database state; UI filtering must not
    hide arbitrary rows as a substitute for cleanup.

## Required cut line for implementation

The next implementation must introduce one backend-owned collector identity,
lifecycle, telemetry, diagnostics, event, and safe-action contract over both
durable implementations. Frontend, CLI, and MCP may render or invoke that
contract but may not infer lifecycle, join operational tables, register
collectors, alter credentials/configuration, or initiate unbounded acquisition.

The final contract must preserve these existing authorities:

- code and reviewed manifests define adapters, schemas, subjects, schedules,
  safety policies, and supported recovery operations;
- PostgreSQL owns desired state, fencing, immutable evidence, and operation
  audit;
- the worker owns actual execution and graceful drain;
- provider-specific probes remain typed diagnostic extensions;
- canonical Fact and gap evidence remain the data truth;
- explicit authorization continues to bound any acquisition or backfill.
