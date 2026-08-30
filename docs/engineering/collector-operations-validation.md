---
title: Collector Operations Validation
status: historical
last_verified: 2026-08-10
---

# Collector Operations Validation

> Historical evidence record. "Live," "production," deployment, and rollback
> statements below describe the recorded 2026-08-10 cutover only; they do not
> certify the current fleet or current recovery state.

## Record boundary

This report records the cutover and operational validation for the canonical
collector control plane introduced after the
[collector operations discovery](collector-operations-discovery.md). It covers
the code-owned registry, scheduled and continuous runtimes, canonical API,
Frontend V2 console, `qt`, MCP, migration protection, and live failure/recovery
evidence.

It does not turn bounded provider reads, Level 2 captures, smoke probes,
storage-lifecycle maintenance, or run-scoped paper streams into fleet
collectors.

## Final collector inventory

| Family | Operational registration | Canonical output | History/recovery | Recorded live state |
| --- | --- | --- | --- | --- |
| Coinbase scheduled open interest | Code-recognized `scheduled_fact` definitions | `derivatives.open_interest.v1` | Latest-state polling; durable attempts and missed-schedule gaps; no arbitrary provider history | Three production definitions healthy |
| Coinbase scheduled funding | Code-recognized `scheduled_fact` definitions | `derivatives.funding_rate.v1` | Latest-state polling; durable attempts and missed-schedule gaps; no arbitrary provider history | Three production definitions healthy |
| Chainlink MVR reserve state | Reviewed structured scheduled adapter/manifest | `asset.reserve_state.v1` | Current/latest report with forward accumulation; no pre-collector history claim | One production definition installed, running, and healthy |
| Coinbase continuous trades | `coinbase.market_structure_trades.v1` | `market.trade.v1`, `market.trade_flow.v1`, `market.trade_flow_feature.v1` | Forward collection plus fenced, idempotent spool/archive recovery | Three production definitions configured and intentionally stopped |
| Coinbase Level 2 | Not admitted as an indefinite collector | Bounded L2 capture/reconstruction contracts | Explicit bounded capture/replay only | Disabled durable definitions are visible but expose no start action |

After Chainlink activation, the live projection contained 19 code-owned
collectors: seven `HEALTHY`, three `STOPPED`, and nine `DISABLED`. One worker
was current, desired-running count was seven, and no registered collector was
failed. Intentionally disabled Level 2 definitions retain registration
diagnostics without being counted as operator attention.

The database also contains 45 historical/test definitions that the deployed
code does not admit. Seventeen are referenced by frozen Dataset archive
evidence and other rows are retained by immutable evidence constraints. An
attempted transactional cleanup was rejected by those constraints and rolled
back. The final architecture therefore reports
`unregistered_definition_count=45` without projecting those rows as active or
failed collectors. This is honest retained evidence, not a compatibility read
path.

## Backup and migration boundary

The collector writers were stopped before the schema cutover. A complete
custom-format PostgreSQL 15.6 backup plus cluster globals was written to:

```text
output/db-backups/pre-collector-operations-20260810T0901Z/
```

Backup identity:

| Field | Value |
| --- | --- |
| Database | `quanttrad` |
| Logical size | 2,561,315,631 bytes |
| Captured | 2026-08-10 09:01:05 UTC |
| Repository boundary | `8253689` |
| Archive SHA-256 | `a40d6c66e14136ddade1e6d8a8ffe2e903f5ec992e2368e70887f5ecab73c9e0` |
| Globals SHA-256 | `1940a1b00b9729bb806f058282f98f6f9c5fc8797153b3f4fbb2cebb333ab929` |
| Migration | `scripts/db/manual_migration_collector_operations_v1.sql` |
| Migration SHA-256 | `819a08f0f310f9677cf402bbf8bbff49e43ada0c381ad3fdc635b0ded1f277cd` |

`SHA256SUMS` and `RESTORE.md` live beside the archive. `pg_restore --list`
read all 588 archive entries. The archive was then restored sequentially into
`quanttrad_restore_verify_20260810`; the restore completed in 570.5 seconds and
source/restored counts matched for canonical Facts, Fact schemas, Datasets,
collector definitions, stream definitions, gaps, Bot runs, and research items.
The scratch database was removed after verification.

Use the same PostgreSQL/TimescaleDB image and the sequential `pg_restore`
procedure in `RESTORE.md`. Parallel restore is not supported for this archive
because Fact row validation reads the schema catalog during COPY.

The manual migration added typed desired-state/control-generation fields and
the immutable operation ledger, mapped existing desired intent once, and
removed the superseded continuous runtime-mode control. Application startup
has no missing-column fallback and no dual-write path.

## Operational proof

### Restart, audit, and idempotency

Scheduled collector `mcd_564531b6236d2ee4f671525073fa28cd` was restarted
through the canonical command path with request ID
`campaign-scheduled-restart-20260810-1` and actor
`codex:collector-operations-goal`.

- control generation advanced exactly once from 0 to 1;
- the worker accepted the new generation and performed the next scheduled
  acquisition successfully;
- replaying the same request returned the existing operation result;
- one immutable operation row exists for the request;
- no duplicate active observation or sequence mismatch was introduced.

A restart without confirmation, request ID
`campaign-rejected-restart-20260810-1`, returned HTTP 409 and appended a failed
operation event. Prior and resulting generation remained 1. This proves failed
preconditions are auditable without mutating collector state.

### Chainlink structured acquisition

The checked-in nxtAssets reserve definition
`mcd_4a39a2f2dc042d7443c283510f77e04c` was installed enabled and restarted
through the canonical audited command path after the Chainlink RPC endpoint
was configured.

- the versioned manifest keeps code-owned instrument provisioning separate
  from the exact provider runtime binding;
- the worker acquired and decoded the confirmed Arbitrum MVR bundle;
- one `asset.reserve_state.v1` Fact was accepted with observation time
  `2026-08-10T19:00:00Z` and platform `known_at`
  `2026-08-10T19:32:35.856420Z`;
- the atomic payload retained report identity, BTC reserve quantity, unit,
  quality, response hash, confirmed block, manifest hash, and source path;
- the collector returned to `RUNNING` / `HEALTHY`, cleared its active error,
  reset consecutive failures to zero, and scheduled its next hourly poll;
- earlier endpoint/configuration failures remain as durable gap and attempt
  evidence rather than being rewritten as successful history.

### Worker/backend restart

The backend and collector containers were recreated while PostgreSQL remained
running. Both returned ready, the collector supervisor reconstructed desired
work, and the fleet returned to one current worker with no split ownership or
manual cleanup. Repeated restart behavior is covered by the scheduled and
continuous runtime suites.

### Bounded network/database interruption

The exact collector container was detached from the QT network for 45 seconds
and then reconnected. A scheduled acquisition failed loudly after the database
connection timeout. The next cadence recovered automatically and appended
`collection_schedule_missed` gap evidence for the unproven interval rather
than fabricating continuity.

Post-recovery validation across the six production OI/funding series recorded:

| Invariant | Result |
| --- | ---: |
| Active canonical observations | 56,956 |
| Distinct series/observation identities | 56,956 |
| Causal ordering violations | 0 |
| Duplicate active observations | 0 |
| `known_at != accepted_at` violations | 0 |

The retained gap is intentional: these latest-state definitions do not
register historical recovery. Diagnostics therefore report a
`gaps_recovery` warning and recommend inspecting gap evidence rather than
claiming that the minute was reconstructed.

### Failure boundaries exercised

Automated and live evidence covers:

- temporary provider/database failure and bounded retry/backoff;
- malformed provider observation and schema/canonicalization rejection;
- scheduled claim expiry and continuous ownership fencing;
- stale worker and stale feed projection;
- continuous restart, spool sealing, orphan recovery, and finalizer drain;
- gap creation and honest recovery capability reporting;
- disabled, stopped, healthy, retrying, failed, and recovery projections;
- safety qualification and fail-closed behavior;
- operation confirmation, capability, registration, and request-id guards.

The canonical diagnostic for a healthy scheduled collector passed
registration, worker, ownership, provider, canonicalization, schema,
persistence, freshness, and scheduler boundaries. It isolated the retained gap
evidence as the only warning.

## Verification results

| Verification | Result |
| --- | --- |
| Full backend/contract suite | 2,017 passed, 30 skipped |
| Focused collector/provider/recovery suite | 68 passed |
| Focused collector API/CLI/MCP/service suite | 41 passed |
| Frontend V2 tests | 235 passed |
| Frontend production build | Passed; 1,988 modules transformed |
| Collector-console scoped ESLint | Passed |
| MCP stdio smoke | Passed; protocol initialization returned server capabilities |
| Browser validation | Fleet, detail, Facts/actions, and canonical diagnostics rendered against live backend evidence |

The repository-wide frontend ESLint target is not yet a clean project gate. It
reports 67 pre-existing errors in unrelated chart, Bot, strategy, and generic
frontend modules. The collector-console files pass ESLint and the complete
frontend test/build gates pass. This campaign did not conceal or opportunistically
rewrite unrelated frontend debt.

## Remaining known gaps

- `active_gap_count` remains unavailable because QT has append-only gap
  evidence but no universal provider-neutral resolved-gap projection.
- Database write latency, storage growth, and some continuous-stream rates are
  `unavailable` when no canonical metric source exists; the frontend does not
  synthesize them.
- Current Coinbase OI/funding and Chainlink reserve adapters cannot reconstruct
  arbitrary pre-collector history.
- The public Arbitrum RPC bootstrap is suitable for local low-volume operation
  but has no availability, latency, or rate-limit guarantee; deployments should
  configure a reviewed endpoint appropriate to their operating requirements.
- Production continuous trade definitions remain intentionally stopped and are
  not production-admitted by this campaign.
- Level 2 remains bounded capture/replay until a code-reviewed indefinite
  adapter and safety contract are implemented.
- The 45 non-operational durable definitions should be cleaned only through a
  separate evidence-preserving migration, never by bypassing immutable Dataset
  references.

Browser screenshots are retained as local operational artifacts under
`output/collector-operations/`.

## Operator references

- [Collector operations guide](../guides/collector-operations.md)
- [Collector operations control plane](../architecture/data/COLLECTOR_OPERATIONS_CONTROL_PLANE.md)
- [Continuous collector runtime](../architecture/data/CONTINUOUS_COLLECTOR_RUNTIME.md)
- [ADR 0064](../architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md)
