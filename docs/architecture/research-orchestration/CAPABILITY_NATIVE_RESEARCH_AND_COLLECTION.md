---
component: capability-native-research-and-collection
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - research
  - market-data
  - nomenclature
  - collectors
  - safety
code_paths:
  - src/research_science/temporal.py
  - src/research_science/hashing.py
  - src/research_science/replay_availability.py
  - src/market_data/stream_enrollment.py
  - portal/backend/service/market/collector_safety.py
  - config/market_data/coinbase_perpetual_trade_fleet.v1.json
  - config/market_data/coinbase_perpetual_l2_fleet.v1.json
---
# Capability-Native Research and Collection

## Current execution and historical custody

[ADR 0082](../decisions/0082-separate-historical-custody-from-active-research.md)
defines the current Indicator/Check/Study boundary and current-only execution
catalog. Earlier version-specific descriptions below are historical method
meaning, not promises that retired factories remain executable. Original
records remain integrity-checked and readable through data-only custody.


## Canonical nouns

| Capability | Canonical contract | Owns |
|---|---|---|
| Research question/findings | Research Memory Study | Question, interpretation revisions and evidence relationships |
| Reusable causal recognition | Indicator | Typed causal runtime, stages, references, duration and context |
| Analytical procedure/execution | CheckDefinition / saved Check | Admitted Python method, pinned settings and immutable execution evidence |
| Scientific authority | `ScientificProtocol` | Frozen role assignments, budgets, leakage controls, metrics, holdout rules |
| Search lineage | `ResearchFamily`, `Trial` | Attempt accounting, failures, validation feedback, lineage |
| Selection evidence | `Candidate`, `Certificate` | Frozen candidate and independent scientific assessment |
| Provider product meaning | `ProductContract` | Quantity/contract translation and exact canonical product version |
| Collection request | `StreamEnrollment` | Provider, venue, instrument, channels, adapter contract, spool bounds, fleet |
| Runtime safety | `CollectorSafetyPolicy`, `QualificationEvidence`, `SafetyHalt` | Thresholds, current proof, persistent kill switches |

Human instructions may group work however is convenient. Those groupings do
not become schema versions, foreign keys, API nouns, or runtime modes.

## Research composition

A Study links current Check executions and findings. Check requirements and
preparation resolve explicit typed inputs and frozen bindings. Reusable causal
recognition comes from Indicators/shared kernels; the admitted Check owns
selection, future-path labels and summaries. Causal Indicator outputs/candidate signals may be developed before Checks;
Checks test usefulness rather than authorize Indicator creation. Signal is
already a typed Indicator output, and Strategy owns actions/risk.
No executable StudyDefinition or
implementation-bundle registry is part of this route.

OI, funding, trades, volume, price and book facts remain peers. Each admitted
procedure declares its actual requirements and causal clocks. The pure temporal
join and availability helpers remain available; they do not imply a runnable
generic study framework.

## Collection composition

```text
StreamEnrollment manifest
  -> ProductContract registered immutably
  -> canonical source + fact series
  -> mutable stream definition references immutable identities
  -> system-derived QualificationEvidence
  -> generic supervisor resolves exactly one adapter
  -> provider frames -> durable spool -> archive -> canonical facts
```

The Coinbase perpetual trade and L2 fleets currently enroll BIP, ETP, and SLP.
Each has no `stop_at`; the worker runs it until an operator stop, a persistent
safety latch, or a fail-closed local spool condition. A compatible product is
added by manifest data and catalog registration. Authentication is enrollment
data rather than collector ownership: the single-node public market channels
use `auth_mode: public`, while another reviewed manifest may explicitly select
authenticated transport without changing the supervisor or projection path.

## Safety semantics

- Healthy: start/continue collection.
- Warning: append evidence, alert Grafana, continue collection.
- Critical: append evidence, latch the applicable scope, remove the stream
  from desired work, and drain finalizers before releasing the lease.
- Operator halt: same persistent latch without requiring a threshold breach.
- Acknowledgement: a distinct append-only event clears one exact scope.

Global scope uses `global:*`; fleet and stream scopes use their canonical IDs.
Qualification checks all applicable scopes on every supervisor pass. A process
restart therefore cannot silently resume a halted stream.
