# Collector Operations

QT operates every durable registered market-data collector through one
provider-neutral control plane. Use Frontend V2 for visual inspection and safe
actions, `qt` for exact operator workflows, and MCP for guarded agent-host
workflows. All three consume the same backend state, diagnostics, and immutable
operation ledger.

Collector implementations and executable adapter packs remain code-owned.
Lifecycle surfaces cannot create or delete collectors, edit provider/runtime
configuration, change credentials, register schemas, run SQL, or authorize
arbitrary acquisition. Product definition enrollment is a separate confirmed
admin command and can use only collector types already registered by the
deployed adapter pack.

## Enroll another supported product

Adding a symbol is configuration when its provider and collector types are
already deployed. For a Coinbase future, run a bounded public stream smoke
check and then enroll its registered pack:

```bash
qt providers stream-smoke \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --symbol <coinbase-product-id> \
  --product-id <coinbase-product-id> \
  --channel market_trades \
  --channel level2 \
  --auth-mode public \
  --duration 12

qt data collector-definitions enroll-product \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --product-id <coinbase-product-id> \
  --actor-id <operator-id> \
  --reason "Approved market-data coverage" \
  --confirm
```

Omitting `--collector` installs all supported Coinbase futures definitions:
open interest, funding rate, trades, and Level 2. Repeat `--collector` to select
a subset. New definitions start running and are discovered without an
application deployment. Reapplying is idempotent and does not replace a later
audited lifecycle choice.

The command validates provider metadata and contract units before it writes a
canonical instrument or definition. It cannot introduce a new provider,
channel, projection, Fact schema, or recovery policy. Those changes remain a
normal code/CI/deployment workflow. On a single-node server, use the equivalent
`bash scripts/automation/server_deploy.sh qt ...` wrapper documented in the
[operator handbook](../operators/README.md).

Chainlink definitions run through the same scheduled lifecycle. Their feed
bindings also contain a network, contract address, dimensions, unit, endpoint
reference, and provenance contract, so V1 admits those bindings through
reviewed manifests rather than the Coinbase product command. That is a binding
admission difference, not a separate or opinionated collector runtime.

## Fleet and market-data plane

Open **Operations -> Market** in Frontend V2 for the provider-first fleet:

```text
Provider summary
  -> one expanded, bounded collector page
      -> exact collector detail and evidence
```

Provider rows show explicit running, stopped, paused, and disabled counts;
health is a separate value. Expand one provider to load its collectors, or use
**All collectors** for bounded search and pagination. Only the provider summary
uses a live stream, and it emits on material changes. Collector rows refresh
only for the visible page. This keeps the default workload bounded as the fleet
grows instead of opening one stream per collector.

Throughput and freshness describe only collectors expected to run. An
intentionally stopped collector shows its stable last-data date or no data,
never a seconds counter that keeps worsening. **Attention only** means a real
operator exception such as failed/delayed desired work or invalid
registration; normal polling does not enter the attention feed.

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

## Lifecycle and health

The backend owns two separate operator dimensions.

| Operational state | Operator meaning |
| --- | --- |
| `DISABLED` | The reviewed configuration gate is closed. |
| `STOPPED` | Configured and intentionally stopped. |
| `PAUSED` | Configured and intentionally paused. |
| `RUNNING` | Expected to acquire canonical Facts. |
| `STOPPING` | Desired work was withdrawn while ownership drains. |

| Health while running | Operator meaning |
| --- | --- |
| `HEALTHY` | Worker, acquisition, persistence, validation, and freshness agree. |
| `DELAYED` | Collection is retrying, recovering, stale, or otherwise degraded. |
| `FAILED` | Desired work has a terminal or active failure. |
| `UNKNOWN` | There is not yet enough evidence to claim health. |
| `NOT_APPLICABLE` | The collector is not expected to run. |

The detailed runtime `actual_state` and `state_reason` remain available for
diagnosis. Frontend and clients must display the backend-owned operational
state and health rather than reclassifying a collector from heartbeat,
freshness, or a single metric.

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
  recovery. A provider `UNKNOWN_ORDER_SIDE` trade remains in exact raw archive,
  is omitted from the canonical BUY/SELL tape with
  `provider_trade_side_unknown` evidence, and invalidates affected live-flow
  coverage without stopping sibling trade ingestion or the collector. Bounded
  capture/replay applies the same policy.
- Coinbase Level 2 uses the same registered continuous supervisor and lifecycle
  as trades. Its projection additionally requires a verified checkpoint plus
  durable deltas, or a fresh provider snapshot, before book validity can resume
  after a discontinuity.
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
