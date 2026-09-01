# Continuous Collector Reconnect Storm (2026-09-01)

## Incident

- Scope: continuous Coinbase trade and level-2 collectors
- Symptom: every stream reported healthy and fresh at inspection time, while
  the collector process was repeatedly disconnecting and reconnecting internally
- User-facing effect: no sustained coverage loss was observed during the
  inspection, but the churn increased the risk of gaps and made a point-in-time
  health summary misleading
- Engineering effect: the long-running collector accumulated avoidable CPU and
  filesystem work while continuously creating new sessions, epochs, and spool
  directories

This was a collector receive-path performance and lifecycle incident. It was
not a Docker restart loop: the repeated `restart_count` values belonged to the
supervised stream tasks inside the long-running container.

The underlying behavior was deliberately left running while
`qt-collector-reconnect-storm` was enabled, so the actual failure could exercise
the alert and email path before corrective code changed the signal.

## What We Observed

The current health plane could show every stream as healthy, fresh, and
accepting valid data while the lifecycle history showed repeated disconnect
epochs. Most recent disconnects reported `1011 keepalive ping timeout`; a
smaller secondary population reported `continuous_collector_epoch_mismatch`.

The spool trees contained enough historical paths that direct invocation of the
current backlog calculation was materially expensive even when the current
unacknowledged backlog was small. The exact measurements belong to the private
installation record; they are samples rather than latency bounds.

## Root Cause

The receive loop performs synchronous filesystem work before accepting every
provider frame. `require_spool_capacity()` calls `spool_backlog_bytes()`, which
recursively walks the stream's entire accumulated spool tree and calls `stat()`
for matching files.

That work grows with historical file and directory count rather than current
unacknowledged backlog. As the tree grew, the synchronous traversal repeatedly
occupied the event-loop thread long enough to delay WebSocket receive work.
The client then failed to process keepalive traffic in time, closed with a
`1011 keepalive ping timeout`, and reconnected. Each reconnect created more
session and epoch paths, making later traversals more expensive. The result was
a self-reinforcing loop:

```text
larger spool tree
      -> slower synchronous per-frame traversal
      -> delayed WebSocket receive / keepalive handling
      -> disconnect and reconnect
      -> more session and epoch paths
      -> larger spool tree
```

The attribution is based on the timeout distribution, process load, filesystem
timing samples, and the confirmed synchronous call path. An instrumented
event-loop-lag trace was not captured, so the exact scheduling delay for each
individual disconnect is not claimed as proven.

## Secondary Failure

The collector increments its expected `connection_epoch` before
`stream.connect()` succeeds, while the transport advances its own epoch only
after a successful connection. A failed connection can therefore leave the
collector one epoch ahead. Subsequent otherwise valid messages are rejected as
`continuous_collector_epoch_mismatch` until the supervised task restarts.

This mismatch amplifies the incident but does not explain the dominant
keepalive-timeout population.

## What Was Not The Root Cause

- The container itself was not repeatedly restarting.
- The database was available and accepted fresh data during the inspection.
- The durable spool's `fsync` requirement was not the identified defect and
  should not be removed.
- The current backlog byte limit was not exhausted.
- Larger WebSocket queues or keepalive timeouts would mask the receive-path
  blockage without removing its growth characteristic.

## Detection And Containment

The previous health view emphasized current liveness, freshness, and throughput.
It could therefore report healthy immediately after every successful reconnect.
The added Grafana rule treats repeated lifecycle transitions as the actionable
state: at least six distinct disconnect epochs for one stream inside a rolling
15-minute window, sustained for five minutes. Isolated reconnects remain events,
not alerts.

No collector restart or corrective code was applied during alert validation.
This preserved the evidence and avoided manufacturing a synthetic failure by
stopping a healthy dependency.

## Corrective Action Plan

The code correction is intentionally deferred until the alert path is proven.
The bounded follow-up is:

1. Maintain unacknowledged spool bytes incrementally on append, acknowledge,
   recovery, and discard instead of recursively walking the tree per frame.
2. Reconcile the counter asynchronously at startup and at a low operational
   frequency so drift is observable without entering the receive hot path.
3. Advance the collector's expected epoch only when connection establishment
   succeeds, keeping it aligned with the transport epoch.
4. Preserve append-before-parse, `fsync`, database acknowledgement, and
   acknowledged-only deletion semantics.

The correction is done only when a sustained production observation shows fresh
valid data, no epoch mismatches, collector CPU returning to its normal range,
and every stream remaining below the reconnect-storm threshold. A short green
snapshot is insufficient evidence.

## Permanent Lessons

- Collector health needs both current freshness and historical churn.
- Capacity checks on a per-frame path must have work bounded independently of
  historical archive size.
- Reconnect accounting must describe supervised tasks separately from container
  lifecycle.
- Alert on a sustained actionable state, not one email per lifecycle event.
- Preserve a real failure long enough to prove new detection when doing so does
  not create unacceptable data or safety risk.
