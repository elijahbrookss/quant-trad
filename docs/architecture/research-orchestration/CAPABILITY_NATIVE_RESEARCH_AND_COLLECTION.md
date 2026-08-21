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
  - src/research_science/study.py
  - src/research_science/temporal.py
  - src/research_science/replay_availability.py
  - src/market_data/stream_enrollment.py
  - portal/backend/service/market/collector_safety.py
  - config/market_data/coinbase_perpetual_trade_fleet.v1.json
  - config/market_data/coinbase_perpetual_l2_fleet.v1.json
---
# Capability-Native Research and Collection

## Canonical nouns

| Capability | Canonical contract | Owns |
|---|---|---|
| Requested research | `ResearchBrief` | Objective, economic claim, immutable claim intent, requester |
| Executable study | `StudyDefinition` | Instruments, declared facts/transforms/joins, exact implementation bundles, benchmarks |
| Scientific authority | `ScientificProtocol` | Frozen role assignments, budgets, leakage controls, metrics, holdout rules |
| One execution | `ResearchRun` | Exact study, protocol, code, datasets, availability evidence, and bundle versions |
| Search lineage | `ResearchFamily`, `Trial` | Attempt accounting, failures, validation feedback, lineage |
| Selection evidence | `Candidate`, `Certificate` | Frozen candidate and independent scientific assessment |
| Provider product meaning | `ProductContract` | Quantity/contract translation and exact canonical product version |
| Collection request | `StreamEnrollment` | Provider, venue, instrument, channels, adapter contract, spool bounds, fleet |
| Runtime safety | `CollectorSafetyPolicy`, `QualificationEvidence`, `SafetyHalt` | Thresholds, current proof, persistent kill switches |

Human instructions may group work however is convenient. Those groupings do
not become schema versions, foreign keys, API nouns, or runtime modes.

## Research composition

```text
ResearchBrief + ScientificProtocol
  -> StudyDefinition
       facts: FactRequirement[]
       clocks: AvailabilityTransform[]
       joins: TemporalJoinSpec[]
       code: exact registered bundles
  -> provider-free preflight against frozen train/validation/holdout roles
  -> immutable ResearchRun
  -> budgeted family/trials/candidate/certificate
```

OI, funding, raw trades, trade flow, volume, price, and book state are peers.
No generic orchestrator presumes which facts must be joined. The study declares
them and the registry resolves exact implementations. Temporal joins require
event, sample, and known-at clocks no later than the decision time. Missing
facts follow the declared reject, exclude, or null policy. Prefix-invariance
tests prove later facts cannot change an earlier frame.

The trade-flow replay transform is one registered availability implementation.
It preserves canonical `known_at` while deriving a separate receipt-backed
research clock from exact frozen raw, aggregate, and coverage evidence.

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
