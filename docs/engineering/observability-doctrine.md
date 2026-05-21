# Observability Doctrine: Quant-Trad

**Status**: Active Engineering Doctrine  
**Audience**: All engineers, infrastructure, and operations maintaining observability systems  
**Change Policy**: Cross-team consensus required for principle changes

---

## 1. Purpose

This doctrine defines how Quant-Trad will observe itself—what gets instrumented, how, and why. Observability in this codebase is intentional, contract-based, and economical. Every log and metric serves a purpose. Nothing is emitted because it might be useful "someday."

The purpose of observability is to answer operational and debugging questions **during execution** without modifying code:

- Is the system running correctly?
- Where is time being spent?
- Why did a trade not execute?
- Is this viewer receiving stale data?
- Which symbol is degraded?
- Are queues backing up?

Observability must be durable across years of evolution without constant refactoring of instrumentation points.

---

## 2. Scope

This doctrine applies to:

- **All logging** in src/ and portal/backend
- **All metrics and timing instrumentation** across runtime, indicators, signals, projection, and streaming
- **All transport layers** (queues, websockets, database, external APIs)
- **All lifecycle phases** (bootstrap, execution, degradation, recovery, shutdown)

This doctrine does **not** apply to:

- Test-only logging (tests may emit anything for debugging)
- Third-party library instrumentation (accept what we get)
- Configuration/environment inspection (startup logs only)
- Development/repl environments (debug freely)

---

## 3. Core Principles

### 3.1 Observability Is Contract-Based, Not Ad Hoc

Every log and metric must belong to an explicit, documented observability contract. No instrumentation is added because someone "might want to debug this someday."

**What this means**:
- Before adding a log or metric, identify which contract or principle it serves
- Contracts are documented in the architecture docs or this doctrine
- When a contract is missing, that's a gap to plan in a refactor
- When a log doesn't belong to a contract, it's noise

**Consequence**: Part of code review is asking "which contract does this observe?" If there's no answer, the instrumentation is removed or belongs in a new formal contract.

### 3.2 Metrics Describe Flow, Logs Describe Meaning

**Metrics** capture continuous system behavior, measurable quantities:
- How deep is the queue?
- How long did this phase take?
- How many events/bytes flowed through?
- When did it happen (timestamp)?
- How many failed or dropped?

**Logs** capture transitions, decisions, anomalies:
- Why did we degrade?
- Which rule matched and why?
- What error occurred and what we're doing about it?
- What state changed?
- What decision did we make?

A log without meaning is noise. A metric without context is useless. Use both purposefully.

**Hard rule**: Never emit the same information as both a log and a metric. If it's a count, emit the count as a metric; derive the rate in dashboards. If it's a decision point, log it once with context.

### 3.3 Every Observable Event Must Be Attributable

Every observable event—log or metric—must carry **canonical context** that makes it traceable. You must be able to answer: "Which bot? Which run? Which trade? Which viewer?"

Context fields must be **consistent across all events**. If a log includes run_id, every related metric must also carry run_id so they can be joined in queries.

Context is added at the point where it becomes known, never retroactively inferred.

### 3.4 Counts Are Emitted, Rates Are Derived

Do not emit rates (events/sec, bytes/sec, etc.) as first-class instrumentation events.

**Why**: Rates are derived from counts and time windows. Emitting rates leads to:
- Loss of precision (rate samples are lossy)
- Query complexity (must align rate samples to meaningful intervals)
- Temporal confusion (unclear if rate is per second, per window, etc.)

**Pattern**:
- Emit: `trades_executed` count and `bytes_sent` count
- Dashboard: Calculate `trades_executed / time_interval` for rate

**Exception**: If a rate is already computed as part of the runtime logic (e.g., "this was the 43rd trade in this run"), emit it as context, not as a first-class metric.

### 3.5 Every Queue Must Expose Depth, Counts, and Pressure

Every asynchronous queue (asyncio.Queue, queue.Queue, actor mailbox, channel, broker) is a potential bottleneck. We must observe them consistently.

Every queue must expose:
- **Current depth** (items pending)
- **Max capacity** (configured limit)
- **Utilization percentage** (depth / capacity)
- **Cumulative enqueue count**
- **Cumulative dequeue count**
- **Cumulative timeout count**
- **Cumulative overflow/drop count**

These are metrics, emitted on a cadence (~1/sec for queues in critical paths, less frequently for others).

**Why**: Backpressure is invisible without queue metrics. A mysterious slowdown often traces back to a queue that's full and causing upstream stalls.

### 3.6 Every Pipeline Stage Must Expose Latency

The runtime-to-viewer pipeline has distinct stages. Each stage must expose timing so bottlenecks are visible. Stages include:

- Runtime execution (decision to emit)
- Emit (prepare delta)
- Transport send (queue to wire)
- Intake (receive at projection)
- Projection (fact ingestion to projection completion)
- Aggregation (if applicable)
- Fanout (distribute to subscribers)
- WebSocket delivery (queue to client send)
- Snapshot generation (if applicable)

Each stage must expose elapsed time for that phase. Stages may also emit intermediate state (queue depth, count of items).

Latency is hierarchical:
- Total end-to-end latency = sum of stage latencies + gaps
- Each stage latency = distribution of measurements (p50, p95, p99, max)

**Why**: Without per-stage latency, you can't answer "which stage is slow?" Only end-to-end latency hides the problem.

### 3.7 Payload Boundaries Must Expose Size

Whenever a payload crosses an operational boundary, emit its size:
- Runtime → emit channel (runtime payload bytes)
- Emit → transport (delta bytes)
- Transport → intake (wire bytes)
- Projection → fanout (projected state bytes)
- Fanout → websocket (message bytes to client)

Size is sampled (not every message, but statistically representative).

**Why**: Payload sizes directly impact latency, memory usage, and network saturation. No size observability = no ability to optimize payload handling.

### 3.8 No High-Frequency Logging in Hot Paths

Hot paths (code called thousands of times per second) must not emit logs per invocation.

**Rule**: If a code path runs >100 times/sec, it must not emit structured logs at every invocation. Use:
- Metrics (cheap) instead of logs for every event
- Sampling for diagnostic logs (e.g., 1% sample rate)
- Anomaly-only logging (log only when something is wrong)

**Exception**: A single terminal event per lifecycle phase (e.g., "bar processing complete") is acceptable.

**Why**: Logging is I/O-bound and network-dependent. High-frequency logging distorts system behavior, causes latency bloat, and defeats the purpose of observability.

### 3.9 All Production Logs Must Be Structured and Event-Named

No freeform logs in production code paths.

Every production log must have:
- **Stable event name** (e.g., `runtime_step_started`, not "starting step")
- **Structured fields** (key=value pairs, not freeform text)
- **Explicit severity** (DEBUG, INFO, WARN, ERROR)
- **Canonical context** (run_id, bot_id, etc.)

**Format**: Logs are emitted via canonical logging utilities (log_context.py, perf_log.py) that automatically add context and structure.

Consequence: Logs are machine-parseable and query-friendly. "Find all events where the rule made a bad decision" becomes feasible.

### 3.10 Bootstrap, Degradation, Recovery, and Lifecycle Are First-Class Observability Domains

Operational visibility hinges on understanding these critical phases. Each must have explicit lifecycle contracts:

- **Timing**: When did it start? When did it complete? How long did it take?
- **Counts**: How many items were bootstrapped? How many recovery attempts? How many overlays built?
- **Status**: Succeeded or failed? Which step failed?
- **Context**: Which symbol? Which run? Which operation?

These are not afterthoughts—they are designed into the observability system from the start.

### 3.11 Observability Must Explain User-Visible Smoothness

The viewer sees a stream of updates. Observability must be able to explain:

- **Stale views**: When did facts stop flowing? Why?
- **Jitter**: Why are updates suddenly slow, then fast?
- **Lag**: How fresh is the current snapshot? (freshness age)
- **Burstiness**: Why did a batch of old updates arrive together?
- **Dropped updates**: Did we drop anything? How do we know?
- **Delayed snapshots**: What delayed snapshot generation?

If observability can't answer these, a gap exists.

### 3.12 Observability Must Be Cheap in Steady State

Observability overhead must not materially distort system behavior. In steady state:

- Debug logging is **off by default**
- Sampled diagnostics use low rates (1-5%)
- Hot-path observability is **metrics only** (no logs)
- Recovery paths use **anomaly-only logging** (not per-attempt)
- Context is added once at the top of a flow, not at every step

**Measurement**: If enabling observability increases latency by >5%, the overhead is too high.

---

## 4. Canonical Context Fields

Every observable event must include appropriate canonical context. Context fields establish the "who, what, where, when" of the event.

### 4.1 Always-Present Core Context

These fields must be present in every log and every metric:

| Field | Type | Meaning | Source |
|-------|------|---------|--------|
| `bot_id` | string | Unique bot identifier | From bot configuration |
| `run_id` | string | Unique run identifier (bot instance) | Generated at bot startup |
| `instrument_id` | string | Canonical instrument identifier | From series or trade context |
| `component` | string | Component emitting the event | Class or module name |
| `timestamp` | UTC ISO8601 | Event generation time | System clock |

### 4.2 Contextual (When Applicable)

These fields must be added whenever the event occurs within their scope:

| Field | Type | Scope / Meaning | Example |
|-------|------|-----------------|---------|
| `phase` | string | Which major lifecycle phase | "bootstrap", "execution", "degradation" |
| `strategy_id` | string | Strategy identifier | For decision-level events |
| `indicator_id` | string | Algorithm/indicator identifier | For computation events |
| `trade_id` | string | Specific trade identifier | For execution events |
| `viewer_session_id` | string | WebSocket viewer session | For streaming events |
| `queue_name` | string | Name of queue being observed | "runtime_delta_queue" |
| `message_kind` | string | Type of message/event | "delta", "fact", "liquidation" |
| `worker_id` | string | Specific worker/thread identifier | For multi-worker contexts |
| `bridge_session_id` | string | Data provider session | For provider integration events |

### 4.3 Field Naming Conventions

- Fields use **snake_case** consistently
- Timestamp fields use UTC ISO8601 or milliseconds since epoch
- No abbreviations in field names (bot_identifier not bot_id... actually, bot_id is canonical)
- Avoid domain concept collisions (see Naming Conventions section)

---

## 5. Metrics vs Logs Policy

### 5.1 Metrics

Metrics capture **continuous behavior** and **quantifiable state**. Metrics are:

- **Additive**: Numeric values that can be summed, averaged, percentiled
- **Time-series**: Emitted on a cadence so trends are visible
- **Sampled**: Statistical representation, not every event
- **Aggregatable**: Can be grouped by dimension (bot_id, instrument_id, etc.)

**Metric types**:
- **Counter**: Monotonically increasing count (trades executed, bytes sent, overflows)
- **Gauge**: Current state (queue depth, connected viewers, degraded symbols)
- **Histogram/Distribution**: Sampled latencies (duration_ms, send_latency_ms)

**Examples of metrics**:
- `runtime_execution_latency_ms` (histogram)
- `queue_depth` (gauge, per queue_name)
- `trades_executed` (counter, per strategy_id)
- `snapshot_bytes` (histogram, sampled)

### 5.2 Logs

Logs capture **meaningful events** and **decisions**. Logs are:

- **Semantic**: Describe transitions, decisions, anomalies
- **On-demand**: Emitted when something interesting happens, not on a cadence
- **Contextual**: Include enough detail to understand the event without external lookup
- **Rare in hot paths**: Not emitted per-invocation in high-frequency code

**Appropriate log events**:
- Lifecycle transition (bootstrap started, degradation entered, recovery attempted)
- Decision point (rule matched, signal emitted, fallback applied)
- Anomaly detected (queue timeout, entity evicted, recovery failed)
- Configuration change (new overlay loaded, strategy updated)

**Inappropriate log events**:
- Every loop iteration in a hot path (use metric instead)
- Redundant repeats of the same event (throttle or sample)
- Implementation details with no operational meaning

### 5.3 Decision Matrix

| Event | Metric | Log | Both | Reasoning |
|-------|--------|-----|------|-----------|
| Trade executed | ✓ | - | - | Count trend is useful; no decision to log |
| Rule matched | - | ✓ | ✓ (C) | Decision to note; also count frequently |
| Queue full | - | ✓ | ✓ (C) | Anomaly with context; also count drops |
| Stage latency >threshold | - | ✓ | ✓ (C) | Anomaly; also track distribution |
| Bootstrapping overlay N | - | ✓ | - | Lifecycle; no need for counter |
| Degradation entered | - | ✓ | ✓ (C) | Lifecycle + state (gauge: degraded_count) |
| Facts projected | ✓ | - | - | Flow metric; no semantic decision |
| Recovery successful | - | ✓ | - | Semantic decision, not a count |

**C** = Counter metric + structured log

### 5.4 Metric Cardinality and Dimension Safety

Metrics must be queryable and aggregatable without explosive cardinality.

**Safe dimensions** (okay to add as labels):
- `bot_id` (typically 1-10 unique values per environment)
- `instrument_id` (hundreds, managed)
- `strategy_id` (tens to hundreds)
- `phase` (4-8 lifecycle phases)
- `queue_name` (dozens of queues)
- `component` (dozens of components)

**Dangerous dimensions** (must be sampled, not per-event):
- `viewer_session_id` (could be thousands)
- `trade_id` (unbounded)
- `payload_hash` (unbounded)

When a metric has dangerous dimensions, emit it as a histogram (sampled distribution) not per-event aggregation.

---

## 6. Queue Observability Policy

Every queue is a potential failure mode. Queue observability is standardized and mandatory.

### 6.1 Queue Metrics

Every queue (async or sync) must emit these metrics on a periodic basis (~1/sec for critical paths):

| Metric | Type | Dimension | Notes |
|--------|------|-----------|-------|
| `queue_depth` | Gauge | queue_name | Current items pending |
| `queue_max_capacity` | Gauge | queue_name | Configured max |
| `queue_utilization_pct` | Gauge | queue_name | depth / capacity * 100 |
| `queue_enqueued_total` | Counter | queue_name | Cumulative items added |
| `queue_dequeued_total` | Counter | queue_name | Cumulative items removed |
| `queue_timeout_total` | Counter | queue_name | Cumulative timeout events |
| `queue_overflow_total` | Counter | queue_name | Cumulative drops/overflows |

### 6.2 Queue Logs

Queues must emit logs for anomalies:

| Event | Severity | When | Context |
|-------|----------|------|---------|
| `queue_timeout` | WARN | Item timeout waiting for dequeue | queue_name, item_kind, wait_ms |
| `queue_overflow` | WARN | Item dropped or overflow | queue_name, depth_at_time, item_count |
| `queue_depth_threshold_exceeded` | WARN | Depth >80% capacity sustained | queue_name, utilization_pct, duration |

### 6.3 Queue Identification

Every queue must have a stable, descriptive name:
- `runtime_delta_queue`
- `symbol_projector_fact_channel`
- `viewer_update_queue`
- `data_provider_fetch_queue`
- `trade_lock_wait_queue`

Names must be consistent across code and dashboards.

---

## 7. Latency and Freshness Policy

Pipeline latency is a primary observability concern. Each major stage must expose timing independently and cumulatively.

### 7.1 Pipeline Stages and Responsibilities

| Stage | Measurement | Metric Name | Why |
|-------|-------------|-------------|-----|
| **Runtime execution** | Time from bar arrival to decision | `runtime_execution_latency_ms` | Execution speed |
| **Emit** | Time to build and serialize delta | `delta_emit_latency_ms` | Serialization speed |
| **Transport send** | Time from queue to wire | `transport_send_latency_ms` | Network and queue delay |
| **Intake** | Time from intake receipt to processing | `intake_latency_ms` | Intake backlog |
| **Projection** | Time from fact ingestion to projection complete | `projection_latency_ms` | Projection speed |
| **Fanout** | Time from projection to fanout distribution | `fanout_latency_ms` | Distribution delay |
| **WebSocket delivery** | Time from queue to client receipt | `websocket_send_latency_ms` | Network and client speed |
| **Snapshot generation** | Time to build snapshot | `snapshot_generation_latency_ms` | View building speed |

### 7.2 Latency Measurement Format

Each stage must expose latency as a histogram distribution:
- **Mean, Median (p50)**
- **p95, p99, p99.9** (tail latencies)
- **Max** (worst case)

Metrics are sampled where appropriate (e.g., snapshot generation every 10th snapshot).

### 7.3 Freshness Age

User-facing snapshots must carry a **freshness age** metric:
- `snapshot_age_ms`: Time since the most recent fact was incorporated into the snapshot

Freshness age must be queryable per viewer to detect stale connections.

### 7.4 Latency Context

Latency metrics must include context about what was being measured:
- Size of the entity (bytes, count)
- Phase context (bootstrap vs execution)
- Recovery attempt count (if applicable)

---

## 8. Payload Size Policy

Size matters: for performance, memory, and network. Size observability is mandatory at operation boundaries.

### 8.1 Payload Size Measurements

At each boundary, measure and emit representative payload sizes:

| Boundary | Metric | Sampled | Context |
|----------|--------|---------|---------|
| Runtime → emit | `runtime_delta_bytes` | Every delta | symbol_count, trade_count |
| Emit → wire | `transport_payload_bytes` | Per message | message_kind, compression |
| Projection → fanout | `projected_state_bytes` | Per projection | entity_count, delta_count |
| Fanout → WebSocket | `websocket_message_bytes` | Every 10th msg | viewer_update_kind |

### 8.2 Size Metrics

Each boundary emits:
- **Size histogram**: Distribution of payloads (mean, p95, p99, max)
- **Byte count**: Cumulative bytes sent (for throughput calculation)

**Example metrics**:
- `runtime_delta_bytes` (histogram, p50/p95/p99)
- `runtime_delta_bytes_total` (counter, bytes sent cumulatively)

### 8.3 Size Thresholds

If a payload exceeds a known threshold (e.g., 1MB), emit a WARN log with context:
- Size encountered
- Entity count (symbols, trades)
- Expected vs actual

---

## 9. Structured Logging Policy

### 9.1 Required Format

All production logs must be structured:

```
<timestamp> <level> <component> | <event_name> | <field_1=value_1> <field_2=value_2> ...
```

Fields are key=value pairs, space-separated, in a canonical order.

### 9.2 Log Levels

| Level | Use Case | Emission Rate |
|-------|----------|---------------|
| **DEBUG** | Implementation details, diagnostics, rare conditional behavior | Rare, off by default, sampled |
| **INFO** | Lifecycle milestones, operational decisions, bootstrap complete | Infrequent, always on |
| **WARN** | Anomalies, degradation, recovery attempts, threshold exceeded | Infrequent, always on |
| **ERROR** | Failures, exceptions, unrecoverable errors | Rare, critical |

- DEBUG logs are disabled by default and only enabled for targeted troubleshooting.
- INFO logs are always on and should be sparse (<<1/sec per bot).
- WARN logs indicate something needs attention (recoverable, but unexpected).
- ERROR logs indicate a failure that operator intervention may be needed.

### 9.3 Event Naming Convention

Events are named with the pattern: `<domain>_<entity>_<event>`

Examples:
- `runtime_step_started` (domain: runtime, entity: step, event: started)
- `botlens_viewer_connected` (domain: botlens/viewer, entity: viewer, event: connected)
- `symbol_degradation_entered` (domain: symbol lifecycle, entity: degradation, event: entered)
- `queue_overflow` (domain: queue, entity: queue, event: overflow)
- `indicator_computation_failed` (domain: indicator, entity: computation, event: failed)

### 9.4 Canonical Field Order

All structured logs use the same field order for consistency:

```
timestamp, bot_id, run_id, instrument_id, component,
phase, strategy_id, indicator_id, trade_id,
viewer_session_id, queue_name, message_kind, worker_id, bridge_session_id,
[event-specific fields]
```

Only include fields that are relevant to the event.

### 9.5 Error Logging

When logging an error or exception:

```
<event_name> | status=failed | error_type=<class> | error_message=<msg> | context_field=<value> ...
```

Always include enough context to correlate to the system's state (run_id, bot_id, etc.).

---

## 10. Hot-Path Policy

Hot paths are code executed thousands of times per second. Unbounded observability in hot paths degrades the thing we're trying to observe.

### 10.1 Hot-Path Definition

A code path is "hot" if it executes:
- **>100 times/sec in normal operation**
- **Latency is a primary concern** for the operation
- **I/O happens inside the path** (logging, disk, network)

Examples in Quant-Trad:
- Bar processing loop
- Candle update loop
- Series computation
- Trade lock acquisition/release

### 10.2 Observability in Hot Paths

**Allowed**:
- Metrics (counters, gauges, histograms)—these are in-memory and cheap
- Single terminal log per lifecycle phase (e.g., "bar processed")
- Sampled diagnostics (1-5% sample rate)

**Not allowed**:
- Per-invocation structured logs
- High-volume DEBUG logging
- Unconditional conditional logging (if debug: log)
- I/O calls in hot loops

### 10.3 Diagnostics in Hot Paths

When diagnostics are needed in a hot path:
- Use **anomaly-only logging** (log only when something violates a threshold)
- Use **sampling** (emit diagnostic log with configurable rate, typically 1%)
- Use **deferred logging** (queue for later processing, don't block loop)

### 10.4 Example: Correct Hot-Path Observability

```
Correct:
  - Emit counter: trades_executed += 1
  - Emit histogram: execution_latency_ms = now - start
  - On anomaly: logger.warn("execution_slow | elapsed_ms=500")
  
Wrong:
  - logger.debug("executing trade") per iteration
  - logger.info("phase: execution") per iteration
  - Structured log every 100ms inside the loop
```

---

## 11. Lifecycle and Recovery Policy

Bootstrap, degradation, and recovery are critical operational modes. Each must have explicit observability contracts.

### 11.1 Bootstrap Lifecycle

Bootstrap is the period where the system prepares to execute (overlays built, state initialized).

**Observable phases**:
1. **Bootstrap start** → log event, start timer
2. **Lock acquire** → measure lock wait time
3. **Overlay build** (per overlay) → measure per-overlay duration
4. **Series projection** → measure projection setup
5. **Bootstrap complete** → log event, emit total duration + component breakdown

**Required metrics**:
- `bootstrap_duration_ms` (histogram)
- `bootstrap_overlay_build_ms` (per indicator_id)
- `bootstrap_lock_wait_ms` (histogram)
- `bootstrap_overlay_count` (gauge)

**Required logs**:
- `runtime_bootstrap_started` (INFO): When bootstrap begins
- `runtime_bootstrap_complete` (INFO): When bootstrap finishes successfully
- `runtime_bootstrap_failed` (ERROR): If bootstrap fails
- `bootstrap_overlay_failed` (WARN): If specific overlay build fails

### 11.2 Degradation Lifecycle

Degradation is when the system falls back to reduced functionality (e.g., sparse data, fallback pricing).

**Observable phases**:
1. **Degradation trigger** → Condition detected that requires fallback
2. **Degradation entered** → log event, timestamp when degradation began
3. **Fallback applied** → log which fallback, when applied
4. **Recovery attempted** → log retry/recovery attempt, timestamp
5. **Recovery success/failure** → log outcome

**Required metrics**:
- `degraded_symbols_count` (gauge)
- `degradation_duration_ms` (histogram, per symbol_id)
- `recovery_attempts_total` (counter, per symbol_id)
- `fallback_applied_total` (counter, per fallback_type)

**Required logs**:
- `symbol_degradation_entered` (WARN): Degradation began
- `degradation_fallback_applied` (WARN): Fallback in use
- `recovery_attempt` (INFO): Retry started
- `recovery_successful` (INFO) or `recovery_failed` (WARN)

### 11.3 Recovery Lifecycle

Recovery is the process of returning from degradation to full functionality.

**Observable phases**:
1. **Recovery initiation** → Condition detected that allows recovery
2. **Recovery attempt** → log attempt count, strategy being tried
3. **Verification** → Check if recovery worked
4. **Recovery complete** → Restored to full state, or gave up

**Required metrics**:
- `recovery_attempts_total` (counter per symbol)
- `recovery_duration_ms` (histogram)
- `recovery_success_rate` (derived from attempts/successes)

**Required logs**:
- `recovery_started` (INFO): Attempting recovery
- `recovery_verification_passed` (INFO): System verified as recovered
- `recovery_max_attempts_exceeded` (ERROR): Given up on recovery

### 11.4 State Transitions

Bootstrap → Degradation and Recovery must be logged as state transitions:

```
Event: runtime_bootstrap_complete -> status: success
Event: symbol_degradation_entered -> reason: <error>
Event: recovery_attempted -> attempt: 2, strategy: fallback_cache
Event: recovery_successful -> duration_ms: 1200
```

---

## 12. Naming Conventions

Naming consistency is essential for querying and correlating events. Naming drift leads to operational friction.

### 12.1 Domain Prefixes

Events are grouped by domain using prefixes:

| Domain | Prefix | Examples |
|--------|--------|----------|
| Runtime execution | `runtime_` | `runtime_step_started`, `runtime_execution_complete` |
| Indicator engine | `indicator_` | `indicator_computation_started`, `indicator_cache_miss` |
| Signals | `signal_` | `signal_rule_matched`, `signal_emission_count` |
| Projection | `projection_` | `projection_fact_ingest`, `projection_latency_ms` |
| WebSocket streaming | `websocket_` or `viewer_` | `websocket_message_send`, `viewer_connected` |
| Queue/transport | `queue_` | `queue_overflow`, `queue_timeout` |
| Data providers | `provider_` | `provider_fetch_failed`, `provider_cache_hit` |
| Degradation/recovery | `symbol_` or none | `symbol_degradation_entered`, `recovery_attempted` |

### 12.2 Event Naming Pattern

Events follow: `<domain>_<entity>_<event>` with lowercase snake_case:

```
runtime_step_started       (domain: runtime, entity: step, event: started)
indicator_overlay_built    (domain: indicator, entity: overlay, event: built)
queue_overflow             (domain: queue, entity: queue, event: overflow)
viewer_disconnected        (domain: viewer, entity: viewer, event: disconnected)
```

### 12.3 Metric Naming Pattern

Metrics follow: `<domain>_<entity>_<measurement>_<unit>`:

```
runtime_execution_latency_ms
queue_depth
projection_latency_ms
websocket_message_bytes
trades_executed_total
```

Or for gauges (no unit):

```
queue_depth
connected_viewers
degraded_symbols_count
```

### 12.4 Avoiding Domain Concept Collisions

**Named entity to avoid**: `MetricOutput` (existing domain concept in Quant-Trad)

Do not use "metric" or "MetricOutput" to refer to observability metrics. Use:
- **For observability measurements**: "instrumentation", "telemetry", "observation", "measurement"
- **For indicator outputs**: "MetricOutput" (domain concept, unchanged)
- **For counters/gauges/histograms**: "counter", "gauge", "histogram", "timing", "sample"

**Examples of correct usage**:
- ❌ "emit a metric" (ambiguous)
- ✅ "emit an instrumentation counter"
- ✅ "exposing queue depth via observability"
- ✅ "tracking projection latency in telemetry"

### 12.5 Consistency Checks

Field and event names are owned by the platform team. Changes to names:
- Must be documented
- Must be version-tagged (especially in logs / dashboards)
- Should not occur more than quarterly (stability is important)

---

## 13. Anti-Patterns

These are patterns that **must not** appear in the codebase. If you see one, it's a refactoring opportunity.

### 13.1 Per-Invocation Logging in Hot Paths

**Anti-pattern**:
```python
for bar in bars:
    logger.debug(f"Processing bar {bar.time}")  # WRONG: per iteration
    process(bar)
```

**Why**: Logs per invocation in hot paths emit thousands of events/sec.

**Correct**:
```python
for bar in bars:
    process(bar)
    bar_counter.incr(1)  # Metric is cheap
logger.debug(f"Processed {len(bars)} bars")  # Single log, not per-bar
```

### 13.2 Context-Free Logs

**Anti-pattern**:
```python
logger.warn("Queue full")
```

**Why**: No context. Can't correlate to bot, run, or queue.

**Correct**:
```python
logger.warn("queue_overflow | queue_name=runtime_delta_queue | depth=1000 | run_id=...", context)
```

### 13.3 Observable Events Not Belonging to Contracts

**Anti-pattern**:
```python
logger.debug("User clicked button at offset 42")  # Observability with no contract
```

**Why**: Not part of any documented observability contract. Noise.

**Decision**: Either (a) remove it, or (b) add a formal contract for this observability.

### 13.4 Redundant Logs and Metrics

**Anti-pattern**:
```python
counter.incr("trades_executed")
logger.info(f"Trade executed | trade_id={trade_id}")  # Redundant
```

**Why**: If the event is important enough to log, make it count. One or the other, not both.

**Decision**: Use the counter, and log only when something is anomalous (trade didn't execute as expected).

### 13.5 Freeform Log Messages

**Anti-pattern**:
```python
logger.info(f"Processed {len(items)} items in {elapsed}ms, {success_count} succeeded")
```

**Why**: Freeform text doesn't parse. Must be structured.

**Correct**:
```python
logger.info("batch_processed | item_count=100 | elapsed_ms=50 | success_count=99")
```

### 13.6 Derived Rates as First-Class Events

**Anti-pattern**:
```python
events_per_sec = event_count / elapsed_seconds
logger.info(f"Rate: {events_per_sec} events/sec")  # Emitting rate as log
```

**Why**: Rates are lossy and hard to align. Emit counts, derive rates in dashboards.

**Correct**:
```python
event_counter.incr(event_count)  # Emit count
# Dashboard calculates: sum(event_count) / time_window
```

### 13.7 State Mutation Without Logging

**Anti-pattern**:
```python
symbol.degradation_entered = True  # State change, no log
```

**Why**: State changes that matter operationally must be logged.

**Correct**:
```python
symbol.degradation_entered = True
logger.warn("symbol_degradation_entered | symbol=...", context)
```

---

## 14. Examples

### Example 1: Instrumented Queue

A correctly instrumented queue provides both metrics and anomaly logs.

**Metrics** (emitted every 1 sec):
- `queue_depth = 45`
- `queue_max_capacity = 1000`
- `queue_utilization_pct = 4.5`
- `queue_enqueued_total = 500000` (cumulative)
- `queue_dequeued_total = 499955` (cumulative)
- `queue_timeout_total = 0`
- `queue_overflow_total = 0`

**Logs** (emitted on anomalies):
- When depth exceeds 80%: `queue_depth_threshold_exceeded | queue_name=runtime_delta | depth=800 | utilization_pct=80`
- When item times out: `queue_timeout | queue_name=... | wait_ms=5000 | item_kind=delta`
- When overflow occurs: `queue_overflow | queue_name=... | depth=1000 | dropped_count=5`

### Example 2: Instrumented Bootstrap

Bootstrap observability shows timing breakdown and identifies slow overlays.

**Log on bootstrap start**:
```
INFO runtime_bootstrap_started | run_id=run_abc | bot_id=bot_1 | phase=bootstrap | 
      series_count=50 | overlay_count_planned=200 | timestamp=2026-04-11T14:23:01Z
```

**Metrics during bootstrap** (per overlay):
- `bootstrap_overlay_build_ms = 45` (for VWAP overlay on SPY)
- `bootstrap_overlay_build_ms = 1200` (for MarketProfile overlay on ES—slow!)

**Log on bootstrap complete**:
```
INFO runtime_bootstrap_complete | run_id=run_abc | bot_id=bot_1 |
     elapsed_ms=5400 | series_bootstrapped=50 | overlay_count_built=200 |
     status=success | slowest_overlay=MarketProfile | slowest_overlay_ms=1200
```

This tells ops: "Bootstrap took 5.4s, MarketProfile overlay is the bottleneck at 1.2s."

### Example 3: Instrumented Signal Rule

A rule evaluation that matches should be both logged (for debugging) and counted (for trending).

**Log on rule match**:
```
DEBUG signal_rule_matched | strategy_id=strat_xyz | rule_id=rule_buy_volatility_surge |
      rule_intent=buy | matched=true | trigger_result=12.5 | guards_passed=3 |
      signal_emitted=true | signal_kind=buy_signal
```

**Metric**:
- `signal_rules_matched_total` counter += 1 (by strategy_id, rule_id)

**No redundant log** saying "signal emitted" — the counter tracks it.

### Example 4: Instrumented Projection

End-to-end projection latency allows diagnosis of freshness issues.

**Log on fact ingest**:
```
DEBUG projection_fact_ingest | run_id=run_abc | instrument_id=ES | 
      fact_kind=trade | fact_seq=1023 | ingest_timestamp=2026-04-11T14:23:01.123Z |
      known_at_timestamp=2026-04-11T14:23:01.100Z
```

**Metric during projection**:
- `projection_latency_ms` = 45 (histogram)

**Log on projection complete**:
```
DEBUG projection_fact_projected | run_id=run_abc | instrument_id=ES |
      fact_seq=1023 | elapsed_ms=45 | state_changed=true | entries_updated=3
```

**Metric for freshness**:
- `snapshot_age_ms` = 50 (time since this fact incorporated into viewer snapshot)

Ops can now query: "Which viewers are seeing stale data?" by looking at snapshot_age_ms.

### Example 5: Instrumented Degradation and Recovery

Degradation is a state change that requires timing and recovery tracking.

**Log on degradation**:
```
WARN symbol_degradation_entered | run_id=run_abc | instrument_id=ES |
     error_cause=no_recent_trades | timestamp=2026-04-11T14:23:01Z |
     remediation_planned=fallback_cache
```

**Metric**:
- `degraded_symbols_count` gauge = 1

**Log on recovery attempt**:
```
INFO recovery_attempt | run_id=run_abc | instrument_id=ES |
     attempt=1 | strategy=fetch_historical | timestamp=2026-04-11T14:23:15Z
```

**Metric**:
- `recovery_attempts_total` counter += 1 (per instrument_id)

**Log on recovery success**:
```
INFO recovery_successful | run_id=run_abc | instrument_id=ES |
     degradation_duration_ms=14000 | recovery_duration_ms=500 | 
     fallback_applied_count=1 | timestamp=2026-04-11T14:23:16Z
```

**Metric**:
- `degraded_symbols_count` gauge = 0 (recovered)
- `degradation_duration_ms` histogram = 14000

---

## 15. Enforcement Expectations

### 15.1 Code Review Gate

Pull requests **must be rejected** if:
- A log or metric is added without corresponding observability contract documentation (in architecture docs or this doctrine)
- High-frequency paths emit per-invocation logs
- Freeform (unstructured) logs appear in production paths
- Context fields are missing where applicable
- Event names don't follow the naming convention

**Code reviewer question**: "Which contract does this observe?" If there's no good answer, the instrumentation gets removed or formalized.

### 15.2 Documentation Requirements

When adding a new observability domain or contract:
- Document the contract in this doctrine or in architecture/observability docs
- Document event names and required context fields
- Document metric types and dimensions
- Document sampling rate or emission frequency
- Add operator-facing guidance (what does this observable tell you?)

### 15.3 Operational Dashboards

Operator dashboards **must only use** events and metrics defined in this doctrine. No custom ad-hoc metrics.

Consequence: Dashboards are stable, queries are reproducible, and events are self-documenting.

### 15.4 Observability Refactoring

When filling a gap (adding queue metrics, adding bootstrap timing, etc.):
- Create a pull request that adds the contract documentation **first**
- Implement instrumentation in a follow-up PR (clearly marked as related)
- Add tests or integration verification that the metrics are emitted
- Update operational runbooks

### 15.5 Deprecation of Observability Events

When an event is no longer needed:
- Leave the instrumentation code in place for 1 release cycle
- Emit DEBUG logs instead of INFO during deprecation window
- Remove after grace period
- Document the retirement in the doctrine

### 15.6 Observability RFC Process

Changes to this doctrine or addition of major new observability domains require **consensus** among:
- Engineering
- Operations
- Infrastructure

A lightweight RFC or doc comment in this file suffices.

---

## Appendix A: Observability Contracts Quick Reference

| Contract | Domain | Primary Metric | Primary Log | Gap Status |
|----------|--------|----------------|-------------|------------|
| Runtime Execution | runtime | execution_latency_ms | runtime_step_started | ✅ Filled |
| Bootstrap Lifecycle | runtime | bootstrap_duration_ms | bootstrap_started/complete | ⚠️ Partial |
| Queue Depth & Pressure | transport | queue_depth, queue_overflow_total | queue_overflow | ❌ Missing |
| Projection Latency | projection | projection_latency_ms | projection_fact_ingest | ❌ Missing |
| Indicator Computation | indicator | indicator_latency_ms | indicator_computation_started | ⚠️ Partial |
| Signal Evaluation | signal | rule_matched_total | signal_rule_matched | ❌ Missing |
| WebSocket Delivery | streaming | websocket_send_latency_ms | viewer_connected/disconnected | ❌ Missing |
| Degradation/Recovery | lifecycle | degraded_symbols_count, recovery_attempts_total | degradation_entered, recovery_attempted | ⚠️ Partial |
| Data Provider Performance | provider | provider_latency_ms | provider_error (if fails) | ⚠️ Minimal |

**Status Key**: ✅ = Implemented, ⚠️ = Partial, ❌ = Not yet implemented

---

## Appendix B: This Doctrine and the Audit

This doctrine formalizes principles that the Q2 2026 observability audit identified as necessary. The audit found:

- **36% observability coverage** of necessary contracts
- **Strong infrastructure in place** (logging utilities, correlation context)
- **Critical gaps** in queue metrics, projection latency, WebSocket delivery, signal evaluation
- **Noise in some areas** (low-signal debug logs, repetitive warnings)

This doctrine is the policy layer that:
1. Eliminates future ad-hoc observability decisions
2. Provides a framework for filling the identified gaps systematically
3. Establishes quality gates so observability remains high-signal and low-cost

---

**Document Revision**: 1.0 (April 2026)  
**Next Review**: Q3 2026 (after Phase 1 implementation)  
**Owner**: Engineering / Infrastructure  
**All questions**: Check the architecture docs in `/docs/architecture/` and this codebase's AGENTS.md for system context.
