---
component: system-architecture-model
subsystem: platform
layer: architecture
doc_type: architecture
status: active
tags:
  - system
  - runtime
  - contracts
  - projections
  - reporting
  - boundaries
code_paths:
  - src/engines/indicator_engine
  - src/strategies
  - src/engines/bot_runtime
  - src/market_data/frozen.py
  - portal/backend/service/research
  - src/data_providers
  - portal/backend
  - portal/frontend
  - docs/architecture/system/diagrams/system-runtime-truth-flow.mmd
---
# System Architecture Model

Quant-Trad separates market evidence, analytical evidence and simulated execution.
Its central rule is causal: a consumer may use only facts available at its
evaluation boundary. A reproducible result must identify its inputs and the
versioned procedure that interpreted them. Reproducibility alone does not
establish economic realism, scientific validity or permission to trade.

The [platform contracts](../../contracts/README.md) define product meaning.
This model explains their composition; component documents explain individual
owners. [Current system](../../current-system.md) is the product introduction.

## Two paths share frozen evidence

[system-runtime-truth-flow.mmd](diagrams/system-runtime-truth-flow.mmd) shows the
shared data boundary and the separate research and execution paths.

Explicit acquisition normalizes provider observations into schema-registered,
append-only Facts. Source identity, effective time, publication/receipt time,
platform acceptance, known-at, revision and quality have distinct meanings.
Canonical readers select revisions under the admitted source and causal scope;
a cache miss never authorizes acquisition.

Dataset preparation freezes exact series, selected ranges, per-series committed
watermarks, provenance and gap evidence. Freeze preserves what QT knows,
including gaps; each consumer still decides whether that evidence is usable.
A frozen manifest is an input identity, not a universal readiness certificate.

A Check consumes an explicit frozen read binding through its registered,
versioned method. Indicator-based Checks use the same indicator engine as other
consumers. Checks produce analytical evidence; they do not create fills, mutate
wallets or make a Strategy executable. Research Observations and Studies retain
findings, relationships and interpretation revisions separately from Check
execution. Scientific protocol, holdout and governance operations have their
own admission boundaries. A completed Check or saved interpretation does not
grant scientific certification, promotion or deployment authority.

A backtest admits a frozen Dataset and pinned Strategy, Indicator and execution
contracts. It advances the indicator timeline, evaluates typed outputs and
applies simulated order, fee, margin, wallet and settlement semantics. Runtime
events and trade/accounting facts explain what it did. Reports and BotLens
project those facts; neither can rewrite execution.

Paper intake is a separate provider-stream composition. Closed candles become
runtime inputs only through its admitted persistence path; provisional candles
remain display-only. Existing live-candle aggregation is not a preparation
route for new research Datasets. New research candles must be provider-native
at the requested timeframe. No current execution mode authorizes external
order submission; a lifecycle label of live means actively producing facts.

## Ownership at the boundaries

| Owner | What it establishes | What it cannot establish |
| --- | --- | --- |
| Data | Registered Fact meaning, acquisition, source/provenance, revisions, causal selection, gaps and frozen input identity | Consumer readiness, trading decisions or fills |
| Indicator engine | Dependency order, state transitions, validated typed outputs and indicator-scoped clocks | Strategy policy or execution |
| Decision layer | Compiled rules, typed-output history, decision artifacts and rejection reasons | Wallet, fills or settlement |
| Execution runtime | Admitted simulated orders, ordering, fills, fees, margin, wallet, lifecycle and events | Venue-realized fills or live-trading authorization |
| Research execution and memory | Check evidence, recorded findings, Studies and versioned interpretations | Alternate Indicator/runtime truth or self-certification |
| Scientific authority | Explicit protocol, attempt, holdout and governance operations under their contracts | Implicit authority from a chart, narrative or successful experiment |
| Persistence | Durable evidence, transactional ownership and verified storage reads | Alternate semantic reconstruction |
| BotLens and reporting | Bounded inspection, durable replay/read models, metrics, comparison and export | New execution facts |
| Observability | Health, failures, resource usage and diagnostic attribution | Market facts, PnL correctness or scientific validity |
| API, CLI, MCP and frontend | Typed requests, admitted operations and human inspection | Independent product semantics or client-owned truth |

The API/backend contracts own operations. CLI and MCP adapt them. Browser
primary rooms and inventories are read-only; the separately routed collector
lens calls the canonical collector-action contract, and Settings > Storage calls
the storage-management contract. These scoped controls do not make inventory
views mutating or browser state authoritative. See the
[operator-surface contract](../../contracts/platform/00_system_contract.md#operator-surface-authority).

## One event can have several clocks

The ordinary bar path is:

    available candle -> indicator snapshot -> decision -> execution effects -> runtime events

Candle open time identifies the interval; availability determines when it may be
used. The indicator engine advances through initialize, apply_bar and snapshot.
No output can appear before its known-at time. Strategy rules read typed
signal, context and metric outputs rather than overlays or mutable internals.

Durable run_seq is persistence order. Indicator, position, wallet and overlay
commit clocks preserve their respective causal transitions; one global arrival
order cannot replace them. Stable IDs connect these transitions to reports,
forensics and logs. See [identity](../identity/IDENTITY_AND_CORRELATION_BOUNDARY.md)
and [persistence](../persistence/PERSISTENCE_BOUNDARY.md).

## Durability and projection are different obligations

Canonical persistence is required: queue overflow, failed writes or an undrained
terminal buffer prevent a successful complete run. Live projection may arrive
before an asynchronous durable batch commits, so visible data is not proof of
commit. Terminal finalization must establish the required durability boundary.

Visual fanout, overlays, debug payloads and metrics use bounded queues, windows
and caches. Their failure must produce explicit unavailable, invalid, incomplete
or degraded state. A missing projection is not an empty valid execution result.
Cold inspection can page durable history; it cannot feed later facts back into
the original decision.

Frozen data reads verify selected material and required archive checksums.
Header-only planning can establish selection metadata without proving payload
integrity. Transactional snapshots, reference/expiry fences and filesystem
identity protect different boundaries; none alone proves a complete recovery.
Local recovery generations share a failure domain with the archive. Encrypted
database-plus-archive recovery is a separate configured adapter and release
requirement, not a claim that every local copy is encrypted or restore-qualified.

## Read at the depth of the change

- [Data boundary](../data/DATA_BOUNDARY.md): acquisition, Facts and Dataset admission.
- [Generalized Fact data plane](../data/GENERALIZED_FACT_DATA_PLANE.md): canonical storage and frozen reads.
- [Indicator runtime](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md) and
  [decision layer](../decision-layer/DECISION_LAYER_BOUNDARY.md): analytical and Strategy inputs.
- [Check evidence](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md) and
  [research memory](../research-memory/RESEARCH_MEMORY_BOUNDARY.md): research evidence and interpretation.
- [Execution runtime](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md) and
  [wallet](../execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md): simulated economic effects.
- [BotLens](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md) and
  [reporting](../reporting/REPORTING_BOUNDARY.md): inspection and comparison.
- [Storage management](../persistence/STORAGE_MANAGEMENT.md): physical placement,
  recovery and explicit operator cutovers.
- [Observability](../observability/OBSERVABILITY_BOUNDARY.md) and
  [security](../security/SECURITY_LAYER.md): operational evidence and trust assumptions.

Historical method records and architectural decisions explain earlier choices.
They do not expand the current executable catalog, establish current deployment
status or override platform contracts. Public documentation describes reusable
infrastructure and process; actual research records remain in QT's private stores.
