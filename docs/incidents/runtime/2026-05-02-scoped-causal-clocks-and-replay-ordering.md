# Scoped Causal Clocks and Replay Ordering (2026-05-02)

## Incident

- Scope: runtime replay, indicator typed outputs, overlay deltas, position lifecycle, wallet ledger, BotLens projection, reporting compare
- Symptom: deterministic replay depended on a mix of `run_seq`, bar time, event time, insertion order, map iteration, and transport sequence fields
- User-facing effect: identical runs could be hard to compare because the system lacked a durable causal order for several state-machine domains
- Engineering effect: unrelated clocks were being asked to prove ordering they did not own

This was an architecture correctness incident.

The system had enough identifiers to correlate facts, but not enough scoped
sequence contracts to replay every state-machine transition without guessing.

## What We Observed

### 1. `run_seq` was overloaded

`run_seq` is the durable runtime event-ledger spine. It orders committed runtime
events inside a run.

It is not sufficient by itself to explain internal causality for every domain:

- wallet state changes can happen as a sequence inside one decision or close settlement,
- position lifecycle can open, update, and close on the same bar,
- indicator state advances once per indicator `apply_bar -> snapshot`,
- overlay transport can emit a viewport delta for changed geometry without being durable execution truth.

Using only `run_seq` forced consumers to infer domain order from payload shape
or from event time. That is too weak for replay.

### 2. Bar time and event time were being treated like sequence fields

`bar_time`, `known_at`, and `event_ts` are required timing fields, but they are
not causal sequence counters.

Multiple facts can share the same bar time:

- several symbols can process the same simulated hour,
- one trade can close while another opens on the same bar,
- wallet release, fees, realized PnL, and equity updates can occur in a close group,
- multiple indicator outputs are published from one indicator snapshot.

Sorting by time plus a fallback tie-breaker works for display. It is not a
durable replay contract.

### 3. Transport sequence fields leaked into causal thinking

Selected-symbol stream `base_seq` and `stream_seq` are websocket replay cursors.
They answer: "which live messages has this viewer seen?"

They do not answer:

- which indicator state transition produced this output,
- which position transition closed the trade,
- which wallet transition changed available margin,
- whether overlay geometry changed because the indicator state changed or because the viewport was rebuilt.

Overlay deltas previously used generic `seq` / `base_seq`, which made this
confusion easier. The overlay delta now uses `overlay_commit_seq` /
`base_overlay_commit_seq` to make the clock owner explicit.

## Root Cause

The system treated "sequence" as a single black-or-white concept instead of a
scoped causal contract.

That created two failure modes:

- too few clocks where replay needed domain order,
- too many generic sequence names where the scope was unclear.

The correct model is not one global counter for everything. The correct model
is just enough durable causal sequence per state-machine boundary.

## Clock Ownership Model

| Clock | Owner | Scope | Still needed? | Why |
| --- | --- | --- | --- | --- |
| `run_seq` | runtime event ledger | committed runtime events in one run | yes | cross-domain ledger spine, reporting order, projection replay |
| `wallet_commit_seq` | wallet gateway / wallet ledger | wallet state transitions | yes | deterministic margin, release, fee, PnL, and equity replay |
| `position_commit_seq` | position/trade lifecycle | one position/trade lifecycle | yes | same-bar open/update/close ordering and stale projection rejection |
| `indicator_commit_seq` | indicator execution engine | one indicator instance | yes | typed output replay for `apply_bar -> snapshot` transitions |
| `overlay_commit_seq` | overlay delta builder | live overlay viewport deltas | yes, but transport/projection only | changed overlay geometry application order |
| `stream_seq` / selected-symbol `base_seq` | websocket stream | viewer delivery continuity | yes, but transport only | live replay window and stale message rejection |

## What Became Unneeded

The new scoped clocks make several weaker ordering mechanisms obsolete for
causal replay:

- generic overlay delta `seq` / `base_seq`,
- using selected-symbol stream cursors as overlay causality,
- using map iteration order for typed output replay,
- using `bar_time` as the only ordering key for same-bar domain transitions,
- using insertion order or row id as the first-class explanation for domain order.

These mechanisms may still appear as storage or transport implementation
details, but they should not be used as the causal proof.

## What Did Not Become Unneeded

Scoped clocks do not remove `run_seq`.

`run_seq` remains the event-ledger spine and the way reporting and projection
compare committed facts across domains. Domain clocks answer narrower questions
inside that spine:

- wallet: what state did the next wallet operation derive from?
- position: what position state did this lifecycle fact supersede?
- indicator: which indicator state transition produced this typed output?
- overlay: which overlay delta should be applied next to the viewport?

Scoped clocks also do not remove timestamps. Time fields still prove known-at
semantics and market simulation timing. They just stop being overloaded as
tie-breaker sequence counters.

## Efficiency Benefits

The immediate benefit is correctness: replay can order facts without guessing.

The performance benefit comes from narrower reads and smaller comparisons:

- indicator replay can sort by `(indicator_id, indicator_commit_seq, output_name)` instead of reconstructing from event time and payload shape,
- overlay projection can ignore indicator provenance changes when geometry is unchanged, avoiding unnecessary live deltas,
- position replay can compare `position_commit_seq` before inspecting full trade payloads,
- wallet replay can validate a compact wallet commit chain before comparing large snapshots,
- reporting diagnostics can classify ordering failures by clock owner instead of scanning unrelated event families.

This should let future validation move from broad "sort everything and inspect
payloads" checks toward scoped checks:

- ledger density via `run_seq`,
- wallet density/drift via `wallet_commit_seq`,
- position lifecycle monotonicity via `position_commit_seq`,
- typed output replay via `indicator_commit_seq`,
- live overlay continuity via `overlay_commit_seq`,
- websocket delivery continuity via `stream_seq`.

That is more efficient because each check uses the smallest clock that proves
the claim.

## Architectural Decision

Use scoped causal clocks for durable replay boundaries and explicit transport
clocks for live viewport delivery.

Rules:

- Do not use a transport cursor as domain causality.
- Do not use wall-clock or bar time as the only tie-breaker for same-bar state transitions.
- Do not add a commit sequence to every field; add one when the domain has independent mutable state that must be replayed durably.
- Keep clock names explicit: `wallet_commit_seq`, `position_commit_seq`, `indicator_commit_seq`, `overlay_commit_seq`.
- Keep `run_seq` as the cross-domain ledger spine.

## Follow-Up Cleanup

The new clocks create a path to remove or demote older ordering workarounds:

- preserve `overlay_commit_seq`, `base_overlay_commit_seq`, and
  `overlay_commit_seq_status` through live overlay transport,
- reject overlay deltas that do not carry an overlay-scoped clock,
- replay wallet ledger facts by `wallet_commit_seq` before runtime ledger order,
- choose trade close context by highest `position_commit_seq` instead of event list position,
- add compact gap/duplicate/status checks per `position_commit_seq` owner,
- keep selected-symbol `base_seq` documented as websocket continuity only.

## Permanent Lessons

- A single global sequence is not enough for a distributed runtime with multiple state machines.
- A sequence field without an owner and scope becomes ambiguous.
- Timestamps prove known-at timing; clocks prove durable causal order.
- Use the smallest clock that proves the replay claim.
- Add clocks where state mutates independently, not everywhere data changes.
