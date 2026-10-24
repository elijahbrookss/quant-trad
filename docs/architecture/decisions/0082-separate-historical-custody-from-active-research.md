---
component: adr-active-research-and-historical-custody
subsystem: research-orchestration
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - research
  - indicators
  - evidence
code_paths:
  - src/core/execution_control.py
  - src/engines/indicator_engine/runtime_engine.py
  - portal/backend/service/research/execution.py
  - portal/backend/service/research/execution_limits.py
  - src/indicators/reversal/definition.py
  - src/indicators/reversal/runtime.py
  - src/engines/indicator_engine/signal_output.py
  - src/research_science/hashing.py
  - src/research_science/temporal.py
  - portal/backend/service/research/recognition_preparation.py
  - portal/backend/service/research/registry.py
  - portal/backend/service/research/catalog.py
  - portal/backend/service/research/service.py
---
# ADR 0082: Separate Historical Custody From Active Research

Draft, October 18, 2026. This records the approved separation of reusable runtime
capabilities from historical evidence reading. Exact integrated qualification
and release remain separate. This document does not attest custody of any
specific private record or authorize a production data migration.

## Reusable execution and historical reading have different lifetimes

Keeping fixed experiment wrappers executable solely to inspect old evidence
creates a growing execution catalog. Conversely, removing an executor must not
silently reinterpret its old evidence as a different current procedure.

Indicator owns reusable causal recognition and state through
`initialize -> apply_bar -> snapshot`. CheckDefinition pins an admitted
analytical procedure; its evaluator owns selection, comparisons and outcomes.
Saved Checks retain immutable execution evidence. Studies own questions,
interpretation revisions and evidence relationships. These are responsibilities,
not a compulsory linear development sequence.

Signals remain typed Indicator outputs with causal identity and known-at time.
A candidate signal can exist before validation; its existence establishes
neither usefulness nor profitability. Strategy owns decisions and Bot owns
execution.

<a id="decision"></a>
## Retirement must preserve interpretation without claiming replay

Remove unused executable composition registries and unsupported catalog entries
through explicit changes. Keep versioned Indicator contracts separate and admit
reusable recognizers through the existing runtime, rather than a parallel Signal
framework. Discovery must reflect supported preview/evidence capabilities.

Saved Check payloads, Observations, Study interpretations and their existing
relationships remain readable without rewriting original records. A retired
method is inspectable as `legacy_frozen_unverifiable`, with current verification,
execution, replay and new evidence-based admission unavailable. This is an
availability limit, not a claim that the original result was invalid. Reading
must not substitute a current calculation for the original procedure.

The earlier record-specific runtime custody allowlist is removed. Source code
must not carry private research-record identities or hashes to grant selected
legacy records verified-history status. Retirement installs no replacement
registry and requires no data migration.

New Check-to-Observation admission, publication references, image attachments
and verified exports require supported Check validation. Existing Study
interpretation history and saved image bindings remain readable without
re-admitting retired research. Saved-selection measurements accept supported
Check17 selections; retired selection profiles cannot feed a new measurement.
The versioned Check18 rule metadata stays fixed for existing payload identities;
current admission does not restore retired input procedures. ScientificAttempt
references that require deterministic replay still need actual supported replay.

Actual research records belong in private QT custody. The repository documents
schemas and reusable behavior; this decision neither publishes record contents
nor claims that a custody transfer or verification has occurred. Discovery follows
the current code-owned catalog, including each method's frozen-only or preview
capabilities, rather than maintaining another version inventory here.

## Shared execution observability

Common timing, structural sizes and outcomes belong to the shared execution
harness. Reuse ExecutionControl and worker snapshots rather than adding timers to
every evaluator. Stage durations are inclusive wall measurements and may overlap;
they are not additive CPU accounting. Queue residence belongs to the async-job
repository.

Structural observations are latest known counts, not heap measurements.
Unavailable or lazy counts remain null; do not traverse or serialize inputs
solely to populate metrics. Operational diagnostics must not enter scientific
definition or result hashes. Cancellation and shutdown uncertainty take
precedence over waiting for a diagnostic snapshot.

## Consequences

No original evidence payload, hash, image, Study publication or scientific
result is rewritten. Historical dependencies do not cause automatic
re-registration, new evidence eligibility or data migration. Producing-source
archives may support separate forensic reproduction, but current code cannot
claim it executes a removed evaluator. New analytical semantics require an
admitted version and focused validation, not a generic DSL or an implied upgrade
of old evidence.
