# Bot Runtime Latency Drift (2026-02-28)

This dated investigation records a growth-related performance failure. Its
implementation paths and controls describe the incident revision; current
behavior is documented in the [BotLens boundary](../../architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
and [runtime boundary](../../architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md).

## What failed

Runtime and BotLens cycle latency rose as the run progressed. State/finalization
cost and pushed payload size grew with bar index; the former snapshot storage
also accumulated large historical payloads.

The cost came from repeated work over growing state: full chart snapshots,
embedded telemetry snapshots, redundant persistence reads, payload construction
without subscribers, sorting an already monotonic prefix, repeated fingerprints
and full serialization for size instrumentation. These mechanisms compounded;
the evidence did not support attributing the drift to one hot function.

## Correction and limits

The incident response avoided changing historical execution data. It skipped
unneeded stream assembly, removed redundant sorting/read-before-write work,
used mutation-driven revisions for delta invalidation, reduced full-snapshot
cadence and sampled instrumentation. Later schema cleanup removed legacy
snapshot/view-state tables from active storage.

Those were changes to projection and transport cost, not changes to fills,
strategy state or known-at timing. The incident's lack of hard historical-data
caps is not a current ban on bounded projections: later
[ADR 0055](../../architecture/decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md)
separates bounded hot views from complete durable inspection.

## Lasting lesson

Measure growth with elapsed history, payload size and subscriber state. A cheap
operation repeated over an ever-growing prefix can dominate a long run.
Instrumentation must have its own bounded cost. Projection caches and transport
cadence must remain downstream of canonical execution truth.
