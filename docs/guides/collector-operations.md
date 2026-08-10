# Collector Operations

QT operates every durable registered market-data collector through one
provider-neutral control plane. Use Frontend V2 for visual inspection and safe
actions, `qt` for exact operator workflows, and MCP for guarded agent-host
workflows. All three consume the same backend state, diagnostics, and immutable
operation ledger.

Collector definitions remain code-owned. These surfaces cannot create or
delete collectors, edit provider/runtime configuration, change credentials,
register schemas, run SQL, or authorize arbitrary acquisition.

## Fleet and market-data plane

Open **Operations -> Market** in Frontend V2 for the fleet topology:

```text
Provider -> registered collector -> canonical Fact schemas -> Fact store
```

The topology carries real throughput, freshness, reject, and gap evidence. The
precise inventory beneath it exposes the backend lifecycle state and reason for
every admitted collector. A separate warning reports durable definitions that
the deployed code does not admit to the operational registry.

Use the CLI for the same contracts:

```bash
qt data collectors fleet
qt data collectors plane
qt data collectors detail scheduled_fact <collector_id>
qt data collectors events scheduled_fact <collector_id> --limit 50
qt data collectors gaps scheduled_fact <collector_id> --limit 50
```

Collector kinds are `scheduled_fact` and `continuous_stream`. Copy the exact
kind and ID from `fleet`; do not infer identity from provider or subject labels.

## Lifecycle

The backend owns lifecycle classification:

| State | Operator meaning |
| --- | --- |
| `DISABLED` | The reviewed configuration gate is closed; no runtime action is authorized. |
| `STOPPED` | Configured, explicitly desired stopped, and no live owner remains. |
| `PAUSED` | Configured, explicitly desired paused, and no live owner remains. |
| `STARTING` | Desired running while ownership/readiness is being established. |
| `HEALTHY` | Worker, acquisition, persistence, validation, and freshness evidence agree. |
| `DEGRADED` | Collection continues but quality/freshness evidence needs attention. |
| `RETRYING` | A bounded retry or supervised restart delay is active. |
| `RECOVERING` | Durable retained work is being reconciled before normal acquisition. |
| `FAILED` | Desired work cannot proceed or a terminal invariant failed. |
| `STOPPING` | Desired work was withdrawn and an owner is draining. |

Frontend and clients must display `state_reason` rather than reclassifying a
collector from heartbeat, freshness, or a single metric.

## Diagnose before changing state

Run the read-only health probe to refresh bounded evidence, or request a full
diagnosis:

```bash
qt data collectors probe scheduled_fact <collector_id>
qt data collectors diagnose scheduled_fact <collector_id>
```

Diagnosis checks registration, worker, scheduler, ownership/fencing, provider
acquisition, canonicalization, schema validation, persistence, freshness, and
gap/recovery evidence. It returns the likely failing boundary, structured
evidence, and a safe recommendation. Recommendations never remediate
autonomously.

The Frontend V2 collector detail page provides the same diagnostic result under
the **Diagnostics** tab. Use **Activity**, **Facts**, and **Data quality** to
inspect authoritative event, canonical Fact, retry, reject, and gap evidence.

## Safe actions

Only actions advertised in a collector's `capabilities.actions` are valid:

```bash
qt data collectors start scheduled_fact <collector_id> \
  --request-id <unique-request-id> \
  --actor-id <operator> \
  --reason "approved operational reason"

qt data collectors restart scheduled_fact <collector_id> \
  --request-id <unique-request-id> \
  --actor-id <operator> \
  --reason "approved operational reason" \
  --confirm
```

The available lifecycle verbs are `start`, `stop`, `restart`, `pause`, and
`resume`. `probe` and `diagnose` are read-only. Bounded recovery appears only
when the deployed collector registers a recovery capability and must retain
explicit limits and confirmation.

Disruptive actions require confirmation in the UI and CLI. Reuse the same
request ID only to retry the same intended operation: the backend returns the
original immutable result and does not advance control generation twice.
Failures such as missing confirmation, invalid registration, or unsupported
action are also recorded with unchanged prior/resulting state.

## Restart and recovery semantics

For a scheduled collector, restart advances control generation. The durable
scheduler preserves its next-run boundary and attempts; it does not duplicate
canonical observations. For a continuous collector, the owner observes the
generation change, closes the provider connection, seals and drains spool
segments, releases the fenced lease, recovers retained work idempotently, and
then establishes a new provider coverage interval.

QT never bridges downtime by inventing completeness. Scheduled misses and
stream discontinuities remain gap evidence. If no registered historical
acquisition exists, diagnosis recommends inspection and the pre-collector or
outage interval remains explicitly unavailable.

Normal container/backend restarts require no direct database cleanup. If a
collector remains in `STARTING`, `FAILED`, or `STOPPING`, inspect diagnostics,
operation history, worker logs, ownership evidence, and safety state before
issuing another action.

## Provider and acquisition limitations

- Coinbase scheduled OI/funding collectors are latest-state polls. Their
  durable history begins at enablement; arbitrary provider history is not
  available through restart.
- Coinbase continuous trades preserve forward history plus idempotent spool
  recovery. Bounded capture/replay remains a separate explicitly authorized
  acquisition workflow.
- Coinbase Level 2 has bounded capture/replay support but no registered
  indefinite supervisor adapter, so it is not a live collector capability.
- Chainlink structured acquisition uses the scheduled collector contract when
  a reviewed definition is installed. No Chainlink collector appears in a
  fleet until the code-owned manifest/configuration is registered and enabled.
- Paper/observe-only Bot streams are run-scoped execution components, not
  market-data fleet collectors.

## MCP

`qt mcp serve` publishes collector resources under
`quanttrad://market-data/...` and the tools `list_collectors`, `get_collector`,
`diagnose_collector`, `probe_collector`, and `operate_collector`.

`operate_collector` is planned by default. Applying a mutation requires
`apply=true`, `confirm=true`, `request_id`, `actor_id`, and `reason`; it then
delegates to the same `qt` and backend command path described above.

## Incident evidence checklist

Capture these before changing state:

1. fleet/detail snapshot and `state_reason`;
2. diagnostic result and likely failing boundary;
3. recent event, gap, retry, reject, and Fact evidence;
4. worker identity/heartbeat and fencing/lease evidence;
5. provider success and last accepted Fact times;
6. requested action, request ID, actor, reason, and operation result.

Do not delete evidence, edit desired-state columns, clear leases, or repair
schemas through direct SQL during normal operations. Use repository migration
and forensic procedures only when an implementation defect requires an
explicit code-reviewed change.
