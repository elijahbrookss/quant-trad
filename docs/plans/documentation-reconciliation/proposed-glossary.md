# Proposed QT Platform Glossary

> **Gate 2 proposal — non-normative.** This document is a review artifact for
> frozen baseline `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`. It adopts no
> terminology, activates no guarantee, resolves no conflict, and changes no
> product semantics. Existing platform contracts and accepted decisions retain
> their current authority.

## Purpose And Intended Placement

This proposal calibrates the smallest cross-boundary vocabulary needed by the
initial guarantee batch plus the ambiguity labels needed to read that batch
honestly. It intentionally does not expand the Phase 1 inventory into a
whole-system glossary.

If Gate 2 approves this model and calibration, the non-blocked entries may
proceed to a separate terminology-adoption review. Gate 2 itself adopts zero
entries. Any later adoption must create the normative vocabulary index at
`docs/contracts/platform/04_glossary.md` and add it to the platform-contract
read order through the required normative review. Until that distinct adoption
decision, this proposal must not enter the normative contract read order or
`AGENTS.md` required reading.

The eventual glossary is an index into existing owners, not a new behavioral
authority. Its summaries must remain narrower than their cited clauses. A
contract or accepted decision owns behavior; the glossary owns only reviewed
labels, distinctions, and usage boundaries.

## Entry Model

Each proposed entry records independent axes rather than collapsing them into a
single status:

| Field | Meaning |
| --- | --- |
| Stable term ID | Existing `QT-TERM-NNN` identifier from Phase 1; IDs are never renumbered or reused |
| Entry kind | `domain_term`, `contrast_set`, or `qualification_rule` |
| Proposal status | `proposed`, `blocked`, or `deferred`; this is not semantic lifecycle |
| Term lifecycle | Whether the underlying repository concept is current, historical, or unclear |
| Source lifecycle | Lifecycle of each cited authority, such as active contract or accepted ADR |
| Owner | One existing subsystem responsible for the term; consulted boundaries remain separate |
| Summary strategy | A short link-backed description, never a copied or newly invented contract |
| Usage boundary | The collision the term prevents and concepts it must not silently absorb |
| Alias handling | Explicit code spelling, compatibility, historical, discouraged, or rejected status |
| Conflict handling | Exact Phase 1 conflict IDs and whether they block adoption |
| Calibration uses | Initial `QT-GC-*` candidates that need the term |

All conflict IDs below refer to
[Phase 1 Terminology Inventory — Semantic Conflicts And Collisions](terminology-inventory.md#semantic-conflicts-and-collisions).
All guarantee IDs refer to the still-unactivated
[Phase 1 Guarantee-Candidate Inventory](guarantee-candidates.md).

## Calibration-Scoped Entries

There are exactly 21 entries in this calibration scope: 19 proposals and two
blocked entries. A proposed status means “eligible for Gate 2 review,” not
“adopted.”

### `QT-TERM-001` — Canonical Fact

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR
- Owner: `data`
- Consulted boundaries: `persistence`
- Calibration uses: `QT-GC-003`, `QT-GC-004`, `QT-GC-005`
- Authority clauses:
  - [ADR 0063 — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
  - [ADR 0063 — Atomicity And Structured State](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#atomicity-and-structured-state)
- Summary strategy: Describe one immutable canonical envelope with one
  schema-registered typed payload for one dataset-eligible market observation;
  defer exact shape, identity, hashing, and validation rules to ADR 0063 and the
  schema registry.
- Usage boundary: Capitalized `Canonical Fact` names the market-data concept.
  Bare `Fact` is unsafe across boundaries and does not include runtime or
  lifecycle facts.
- Alias handling: no alias is adopted; contextual `Fact` is discouraged outside
  an already explicit canonical-market-data scope.
- Conflict handling: `QT-CONFLICT-001` through `QT-CONFLICT-005` remain open
  documentation-remediation findings. This entry does not declare the
  conflicting active documents repaired or superseded.

### `QT-TERM-002` — Fact revision

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADRs
- Owner: `data`
- Consulted boundaries: `persistence`
- Calibration uses: `QT-GC-003`, `QT-GC-005`
- Authority clauses:
  - [ADR 0050 — Invariants](../../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md#invariants)
  - [ADR 0063 — Versioning](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#versioning)
  - [ADR 0063 — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
- Summary strategy: Contrast one immutable stored revision with the logical
  observation it revises; link correction, invalidation, and causal-selection
  behavior to the cited decisions rather than reproducing it.
- Usage boundary: A Fact revision is not a Fact type, payload schema version,
  observation key, or database-global commit sequence.
- Alias handling: none adopted.
- Conflict handling: `QT-CONFLICT-001` through `QT-CONFLICT-005` remain related
  but non-blocking for this distinction; no retired table name becomes an alias.

### `QT-TERM-003` — Observation key

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR
- Owner: `data`
- Consulted boundaries: `persistence`, `research-memory`
- Calibration uses: `QT-GC-003`, `QT-GC-005`
- Authority clauses:
  - [ADR 0063 — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
  - [ADR 0063 — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)
- Summary strategy: Identify the stable logical market-observation identity
  shared by its immutable revisions without copying its schema algorithm.
- Usage boundary: An Observation key is not a durable Research Observation or
  research-memory item.
- Alias handling: `observation_key` is an allowed code spelling, not a second
  concept.
- Conflict handling: retain `QT-CONFLICT-010`; the qualification prevents the
  collision but does not resolve Research Observation eligibility.

### `QT-TERM-004` — Fact type, payload schema version, and revision

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR
- Owner: `data`
- Consulted boundaries: `persistence`
- Calibration uses: `QT-GC-003`, `QT-GC-005`
- Authority clauses:
  - [ADR 0063 — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
  - [ADR 0063 — Versioning](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#versioning)
- Summary strategy: State only that canonical meaning, exact historical payload
  interpretation, and correction sequence are separate axes. Their detailed
  rules remain in ADR 0063 and schema-owned contracts.
- Usage boundary: A generic `version` must not silently stand for any of the
  three axes.
- Alias handling: none adopted; bare `version` is discouraged.
- Conflict handling: `QT-CONFLICT-001` through `QT-CONFLICT-005` remain open and
  do not change the three-axis distinction.

### `QT-TERM-005` — Runtime fact, qualified

- Entry kind: `qualification_rule`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: active contract and accepted ADR
- Owner: `execution-runtime`
- Consulted boundaries: `reporting`, `botlens-projections`
- Calibration uses: `QT-GC-011`, `QT-GC-012`, `QT-GC-013`
- Authority clauses:
  - [Runtime Contract — Artifact Contract](../../contracts/platform/01_runtime_contract.md#artifact-contract)
  - [Runtime Contract — Shared-Wallet Entry Ordering](../../contracts/platform/01_runtime_contract.md#shared-wallet-entry-ordering)
  - [ADR 0042 — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
- Summary strategy: Require a specifically named runtime, wallet, lifecycle, or
  projection fact and link its semantics to the owning contract.
- Usage boundary: Runtime facts do not inhabit the Canonical Fact repository and
  are not made equivalent merely by sharing `fact` vocabulary.
- Alias handling: bare `Fact` is discouraged; no universal runtime-fact alias is
  adopted.
- Conflict handling: retain `QT-CONFLICT-009`; explicit qualification avoids
  claiming a universal Fact store.

### `QT-TERM-006` — Research Observation

- Entry kind: `domain_term`
- Proposal status: `blocked`
- Term lifecycle: `current`
- Source lifecycle: conflicting accepted ADR and active architecture sources
- Owner: `research-memory`
- Consulted boundaries: `research-orchestration`, `data`
- Calibration uses: `QT-GC-008`, conflict calibration `QT-GC-009`
- Competing source clauses:
  - [ADR 0034 — Decision](../../architecture/decisions/0034-use-research-checks-as-analytical-memory-evidence.md#decision)
  - [ADR 0062 — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
  - [Check Evidence Boundary — Assurance Levels](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#assurance-levels)
  - [Research Memory Boundary — Research Check Semantics](../../architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md#research-check-semantics)
  - [Research Memory Boundary — Memory Graph](../../architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md#memory-graph)
- Summary strategy: Supply no normative definition before review. The proposal
  may describe the collision and link both sides, but it may not select an
  Observation-creation or evidence-eligibility rule.
- Usage boundary: The capitalized research-memory concept is distinct from a
  market observation and its Observation key.
- Alias handling: none adopted. Lowercase `observation` remains ambiguous and
  cannot be normalized automatically.
- Conflict handling: `QT-CONFLICT-007` and `QT-CONFLICT-010` block adoption.
  Explicit supersession or normative reconciliation is required first.

### `QT-TERM-007` — Frozen Dataset

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADRs
- Owner: `data`
- Consulted boundaries: `execution-runtime`, `research-orchestration`, `reporting`
- Calibration uses: `QT-GC-005`, `QT-GC-008`
- Authority clauses:
  - [ADR 0051 — Decision](../../architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md#decision)
  - [ADR 0063 — Dataset And Research Semantics](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#dataset-and-research-semantics)
- Summary strategy: Describe the immutable `market_dataset.v1` source manifest
  and point to its owner for exact identity, ranges, revisions, hashes, sources,
  gaps, quality, and watermark.
- Usage boundary: A Frozen Dataset is neither a consumer binding nor a
  RunResearchDataset, report artifact, readiness result, or scientific
  certificate.
- Alias handling: `market_dataset.v1` is a contract identifier, not an alias.
  Bare `Dataset` is discouraged across boundaries.
- Conflict handling: retain `QT-CONFLICT-008`; qualification prevents reporting
  readiness from being mistaken for source-data admissibility.

### `QT-TERM-008` — Frozen Market Data Read Binding

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR and active architecture boundary
- Owner: `data`
- Consulted boundaries: `research-orchestration`, `execution-runtime`
- Calibration uses: `QT-GC-005`, `QT-GC-008`
- Authority clauses:
  - [ADR 0062 — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
  - [Check Evidence Boundary — FrozenMarketDataReadBinding](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#frozenmarketdatareadbinding)
- Summary strategy: Point to the exact provider-free Dataset, subject, series,
  source, range, revision, hash, gap, watermark, and causal-read binding without
  reproducing its field contract.
- Usage boundary: The binding is not the Frozen Dataset itself, consumer
  admission policy, a Strategy snapshot, or a readiness certificate.
- Alias handling: `FrozenMarketDataReadBinding` is the allowed code spelling.
- Conflict handling: none blocking; this entry does not decide consumer-owned
  gap admission.

### `QT-TERM-009` — RunResearchDataset

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR and active architecture boundary
- Owner: `reporting`
- Consulted boundaries: `execution-runtime`, `research-memory`
- Calibration uses: `QT-GC-011`
- Authority clauses:
  - [ADR 0010 — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
  - [Reporting Boundary — Boundary Contract](../../architecture/reporting/REPORTING_BOUNDARY.md#boundary-contract)
- Summary strategy: Identify the canonical reporting data product derived from
  durable run truth and leave its schema and reconstruction behavior with the
  reporting owner.
- Usage boundary: A RunResearchDataset is not a Frozen Dataset, source-data
  binding, runtime ledger, or report materialization status.
- Alias handling: `RunResearchDataset v1` is an allowed versioned spelling.
  Bare `Dataset` is not an alias.
- Conflict handling: retain `QT-CONFLICT-008`; the qualified label does not
  certify source Dataset completeness.

### `QT-TERM-010` — Reporting dataset readiness

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR and active architecture boundary
- Owner: `reporting`
- Consulted boundaries: none
- Calibration uses: `QT-GC-011`
- Authority clauses:
  - [ADR 0010 — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
  - [Reporting Boundary — Readiness Vocabulary](../../architecture/reporting/REPORTING_BOUNDARY.md#readiness-vocabulary)
- Summary strategy: Point to reporting's sectioned status vocabulary rather than
  defining a universal ready boolean.
- Usage boundary: Reporting readiness does not certify Frozen Dataset
  admissibility, runtime health, comparison safety, golden eligibility, or
  scientific validity.
- Alias handling: `dataset_ready` is a derived convenience field, not a synonym
  for the status contract.
- Conflict handling: retain `QT-CONFLICT-008`; qualification is required.

### `QT-TERM-011` — Check

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR and active architecture boundary
- Owner: `research-orchestration`
- Consulted boundaries: `data`, `indicator-runtime`, `research-memory`
- Calibration uses: `QT-GC-008`, `QT-GC-009`
- Authority clauses:
  - [Check Evidence Boundary — Purpose](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#purpose)
  - [ADR 0062 — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
- Summary strategy: Describe a bounded analytical operation over declared
  inputs, while linking evaluation, persistence, and authority limits to the
  Check owner.
- Usage boundary: A Check is not acquisition, an Indicator event, a Strategy
  decision, execution, promotion authority, or scientific certification.
- Alias handling: `Research Check` may remain contextual prose but is not a
  separate canonical concept or adopted alias in this batch.
- Conflict handling: `QT-CONFLICT-007` does not block the core analytical
  definition, but it blocks any glossary statement about automatic Observation
  creation or linking.

### `QT-TERM-012` — Check preview

- Entry kind: `domain_term`
- Proposal status: `blocked`
- Term lifecycle: `current`
- Source lifecycle: conflicting accepted ADR and active architecture sources
- Owner: `research-orchestration`
- Consulted boundaries: `research-memory`
- Calibration uses: `QT-GC-008`
- Competing source clauses:
  - [ADR 0062 — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
  - [Check Evidence Boundary — Assurance Levels](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#assurance-levels)
  - [ADR 0034 — Decision](../../architecture/decisions/0034-use-research-checks-as-analytical-memory-evidence.md#decision)
  - [Research Memory Boundary — Research Check Semantics](../../architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md#research-check-semantics)
- Summary strategy: Retain the label as a review pointer only. Do not place its
  persistence or Observation-eligibility consequence into normative vocabulary
  until the older auto-create/link language is explicitly reconciled.
- Usage boundary: A Check preview is distinct from Frozen Check evidence, but
  the glossary may not yet use that distinction to settle Observation creation.
- Alias handling: bare `preview` is discouraged and no alias is adopted.
- Conflict handling: `QT-CONFLICT-007` blocks adoption of the term's behavioral
  definition. Label existence is not conflict resolution.

### `QT-TERM-013` — Frozen Check evidence

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR and active architecture boundary
- Owner: `research-orchestration`
- Consulted boundaries: `data`, `research-memory`
- Calibration uses: `QT-GC-008`, `QT-GC-009`
- Authority clauses:
  - [ADR 0062 — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
  - [Check Evidence Boundary — Assurance Levels](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#assurance-levels)
  - [Check Evidence Boundary — Result, Verdict, And Replay](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#result-verdict-and-replay)
- Summary strategy: Summarize definition-pinned, input-pinned, provider-free,
  hash-verifiable, replayable Check evidence and defer exact persisted fields to
  the cited owner.
- Usage boundary: Frozen Check evidence is not a verdict, scientific protocol,
  Strategy result, execution proof, or by itself an adopted Research
  Observation eligibility rule.
- Alias handling: `legacy_unpinned` and `legacy_frozen_unverifiable` are
  historical compatibility classifications, not aliases.
- Conflict handling: `QT-CONFLICT-007` remains open only for the Observation
  relationship; it does not redefine the frozen evidence artifact.

### `QT-TERM-014` — Known-at

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: active platform contract and accepted ADRs
- Owner: `platform`
- Consulted boundaries: `data`, `execution-runtime`
- Calibration uses: `QT-GC-001`, `QT-GC-005`, `QT-GC-008`
- Authority clauses:
  - [System Contract — Core Invariants](../../contracts/platform/00_system_contract.md#core-invariants)
  - [ADR 0044 — Decision](../../architecture/decisions/0044-enforce-known-at-prefix-invariance.md#decision)
  - [ADR 0063 — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
- Summary strategy: Identify the causal availability clock used at an evaluation
  boundary and link prefix behavior and schema-owned derivation to their owners.
- Usage boundary: Effective, funding, valuation, publication, receipt,
  acceptance, attestation, and wall-clock times do not become known-at unless an
  owning schema supplies the cited causal rule.
- Alias handling: `known_at` is the allowed code spelling.
- Conflict handling: none blocking; the glossary must not define a new clock
  derivation.

### `QT-TERM-015` — Gap Evidence

- Entry kind: `qualification_rule`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: active platform contract, accepted ADR, and active architecture boundary
- Owner: `data`
- Consulted boundaries: `execution-runtime`, `indicator-runtime`, `reporting`
- Calibration uses: `QT-GC-005`, `QT-GC-008`
- Authority clauses:
  - [ADR 0063 — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
  - [Check Evidence Boundary — Gap Ownership](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#gap-ownership)
  - [Runtime Contract — BotLens Candle Continuity Audit Surface](../../contracts/platform/01_runtime_contract.md#botlens-candle-continuity-audit-surface)
- Summary strategy: Name source-data gap evidence and require runtime, candle,
  book, runner, overlay, projection, and reporting gaps to keep their owning
  qualifier.
- Usage boundary: Gap absence in one namespace cannot certify coverage,
  continuity, book validity, Dataset admission, or reporting readiness in
  another.
- Alias handling: bare `Gap` is discouraged; no cross-namespace alias is
  adopted.
- Conflict handling: retain `QT-CONFLICT-014`; qualification records the
  collision without resolving subsystem policy.

### `QT-TERM-022` — Bot and Bot Run

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADRs
- Owner: `execution-runtime`
- Consulted boundaries: `persistence`, `botlens-projections`, `reporting`
- Calibration uses: `QT-GC-012`
- Authority clauses:
  - [ADR 0030 — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
  - [ADR 0042 — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
- Summary strategy: Contrast the durable Bot definition with the Bot Run that
  owns runtime identity, lifecycle, lease, events, and report linkage.
- Usage boundary: Bot definition state cannot be used as an alternate Bot Run
  lifecycle or execution ledger.
- Alias handling: removed Bot-row lifecycle fields are historical ownership,
  not aliases. `Bot` and `Bot Run` remain distinct labels.
- Conflict handling: none blocking.

### `QT-TERM-023` — Run, qualified

- Entry kind: `qualification_rule`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADRs and active architecture boundaries
- Owner: `platform`
- Consulted boundaries: `execution-runtime`, `research-orchestration`, `data`, `reporting`
- Calibration uses: `QT-GC-012`
- Authority clauses:
  - [ADR 0030 — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
  - [ADR 0062 — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
  - [Reporting Boundary — Boundary Contract](../../architecture/reporting/REPORTING_BOUNDARY.md#boundary-contract)
- Summary strategy: Define no universal Run object. Require the owner-specific
  noun, such as Bot Run, ingestion run, Check execution, or report build, and
  link to that owner.
- Usage boundary: Shared use of a run identifier or time range does not merge
  lifecycle, evidence, or authority between run types.
- Alias handling: bare `Run` is contextual shorthand only, not an adopted alias.
- Conflict handling: none blocking; new run kinds remain owner-defined.

### `QT-TERM-024` — Report, qualified

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted ADR and active architecture boundary
- Owner: `reporting`
- Consulted boundaries: `execution-runtime`, `research-memory`
- Calibration uses: `QT-GC-011`, `QT-GC-012`
- Authority clauses:
  - [ADR 0010 — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
  - [Reporting Boundary — Boundary Contract](../../architecture/reporting/REPORTING_BOUNDARY.md#boundary-contract)
  - [Reporting Boundary — What Reporting Publishes](../../architecture/reporting/REPORTING_BOUNDARY.md#what-reporting-publishes)
- Summary strategy: Distinguish RunResearchDataset, `RunReportDTO`, materialized
  artifact, compact summary, comparison result, and export by pointing to the
  reporting owner rather than defining one universal Report object.
- Usage boundary: Report or materialization status cannot alter Bot Run status,
  runtime truth, or source Dataset identity.
- Alias handling: bare `Report` is contextual shorthand only; none of the
  concrete products is an alias for another.
- Conflict handling: none blocking; `QT-CONFLICT-008` remains relevant wherever
  Dataset or readiness is shortened.

### `QT-TERM-026` — Evidence, qualified

- Entry kind: `qualification_rule`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: active platform contract, accepted ADRs, and active architecture boundary
- Owner: `platform`
- Consulted boundaries: `data`, `execution-runtime`, `research-orchestration`, `reporting`
- Calibration uses: `QT-GC-008`, `QT-GC-009`, `QT-GC-011`, `QT-GC-013`
- Authority clauses:
  - [ADR 0050 — Decision](../../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md#decision)
  - [ADR 0042 — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
  - [Check Evidence Boundary — Assurance Levels](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#assurance-levels)
  - [Check Evidence Boundary — Result, Verdict, And Replay](../../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md#result-verdict-and-replay)
  - [Reporting Boundary — Boundary Contract](../../architecture/reporting/REPORTING_BOUNDARY.md#boundary-contract)
- Summary strategy: Define no universal Evidence artifact. Require source,
  receipt, gap/quality, runtime-ledger, Check, report, proof, or scientific
  qualification and defer semantics to that owner.
- Usage boundary: Evidence in one boundary grants no authority, readiness, or
  certification owned by another boundary.
- Alias handling: bare `Evidence` is discouraged and is not a canonical artifact
  type.
- Conflict handling: none blocking for the qualification rule. Specific evidence
  eligibility conflicts remain attached to their entries and candidates.

### `QT-TERM-047` — BotLens projection

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: active platform contract and accepted ADR
- Owner: `botlens-projections`
- Consulted boundaries: `execution-runtime`, `reporting`, `frontend`
- Calibration uses: `QT-GC-013`
- Authority clauses:
  - [Runtime Contract — BotLens Selected-Symbol Reads](../../contracts/platform/01_runtime_contract.md#botlens-selected-symbol-reads)
  - [Runtime Contract — BotLens Projection Failure Semantics](../../contracts/platform/01_runtime_contract.md#botlens-projection-failure-semantics)
  - [ADR 0055 — Decision](../../architecture/decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md#decision)
  - [ADR 0055 — Invariants](../../architecture/decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md#invariants)
- Summary strategy: Identify bounded current debugger/read-model state derived
  from runtime facts and link bootstrap, cursor, pressure, completeness, and
  durable-history behavior to the cited owners.
- Usage boundary: A BotLens projection is not execution truth, a durable run
  database, complete history, or an implicit readiness certificate.
- Alias handling: none adopted; `snapshot`, `bootstrap`, and `delta` remain
  distinct projection artifacts.
- Conflict handling: retain `QT-CONFLICT-022`; qualification does not reconcile
  every current BotLens vocabulary use.

### `QT-TERM-049` — Readiness, qualified

- Entry kind: `qualification_rule`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: active platform contract, accepted ADR, and active architecture boundary
- Owner: `platform`
- Consulted boundaries: `data`, `indicator-runtime`, `botlens-projections`, `reporting`, `deployment`
- Calibration uses: `QT-GC-011`, `QT-GC-013`
- Authority clauses:
  - [Runtime Contract — BotLens Readiness Semantics](../../contracts/platform/01_runtime_contract.md#botlens-readiness-semantics)
  - [ADR 0010 — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
  - [Reporting Boundary — Readiness Vocabulary](../../architecture/reporting/REPORTING_BOUNDARY.md#readiness-vocabulary)
- Summary strategy: Define no universal ready state. Require the exact owner and
  status vocabulary for runtime, output, projection, reporting, comparison,
  golden, Dataset, or deployment readiness.
- Usage boundary: Ready, healthy, complete, comparable, golden, deployable, and
  available are not interchangeable and do not confer authority outside their
  owner.
- Alias handling: bare `ready` and `complete` are discouraged; derived booleans
  do not replace their status contracts.
- Conflict handling: retain related `QT-CONFLICT-008`, `QT-CONFLICT-014`,
  `QT-CONFLICT-022`, and `QT-CONFLICT-026`. The qualification rule records the
  collision without resolving subsystem semantics.

## Calibration Mapping

The Phase 1-recommended ten-candidate batch uses the glossary as follows:

| Candidate | Term references | Note |
| --- | --- | --- |
| `QT-GC-001` | `QT-TERM-014` | Known-at causality and prefix invariance |
| `QT-GC-003` | `QT-TERM-001`–`004` | Canonical Fact identity, revision, and version axes |
| `QT-GC-004` | `QT-TERM-001` | Provider identity vocabulary is intentionally unnecessary to state the no-read-fallback boundary |
| `QT-GC-005` | `QT-TERM-001`–`004`, `007`, `008`, `014`, `015` | Frozen source identity and causal/gap scope |
| `QT-GC-008` | `QT-TERM-006`–`008`, `011`–`015`, `026` | Includes blocked Observation/preview terminology; candidate stays unactivated |
| `QT-GC-009` | `QT-TERM-006`, `011`–`013`, `026` | Deliberate conflict calibration; `006` and `012` block activation |
| `QT-GC-011` | `QT-TERM-005`, `009`, `010`, `024`, `026`, `049` | Reporting product, projection, evidence, and readiness boundaries |
| `QT-GC-012` | `QT-TERM-005`, `022`–`024` | Bot Run lifecycle versus report projection |
| `QT-GC-013` | `QT-TERM-005`, `026`, `047`, `049` | Strict runtime persistence versus degradable projection |
| `QT-GC-014` | none | Interface-authority wording is sufficiently owned by its direct contract references |

Term references do not activate a candidate. In particular, the blocked term
references make `QT-GC-008` and `QT-GC-009` useful calibration cases without
settling their unresolved terminology.

## Deferred And Excluded Phase 1 Vocabulary

The following Phase 1 term candidates stay outside this proposed glossary:

- `QT-TERM-016` through `QT-TERM-021`;
- `QT-TERM-025`;
- `QT-TERM-027` through `QT-TERM-046`;
- `QT-TERM-048`;
- `QT-TERM-050` through `QT-TERM-055`.

They are deferred, not rejected. They remain unadopted until whole-system
classification and the relevant subsystem or conflict review. This preserves
the open identity-chain, collector-state, coverage, archive/book, overlay,
Strategy-stage, execution/order, wallet, cursor, lease, fingerprint,
deployment, and validation-selector questions associated with
`QT-CONFLICT-011` through `QT-CONFLICT-026`.

All `QT-ALIAS-001` through `QT-ALIAS-020` also remain Phase 1 findings. Phase 2A
does not formally deprecate, replace, remove, or normalize any of them. Exact
code spellings recorded on a proposed entry are not deprecation decisions.

## Validation Rules

The eventual validator must enforce all of the following before this proposal
can become normative:

1. Term headings match `QT-TERM-[0-9]{3}`, are unique, preserve Phase 1 IDs, and
   never reuse a removed ID.
2. Canonical labels are unique after case folding and punctuation/whitespace
   normalization.
3. Every entry has entry kind, proposal status, term lifecycle, source
   lifecycle, one owner, calibration use or critical-ambiguity role, authority
   clauses, summary strategy, usage boundary, alias handling, and conflict
   handling.
4. Owners come from the existing architecture subsystem vocabulary. Additional
   affected boundaries are recorded separately and do not create shared or
   inferred ownership.
5. Every `proposed` entry cites at least one active platform contract or accepted
   ADR. Explanatory architecture may add context but cannot independently
   authorize platform vocabulary.
6. Every referenced repository path and Markdown heading exists. A line number
   may be a review hint but cannot be the durable reference identifier.
7. A `blocked` entry names at least one existing open conflict and cannot carry
   adopted aliases, deprecations, replacements, or a normative definition.
8. Each conflict reference is classified as blocking or related/non-blocking.
   A related reference never implies that the conflict is resolved.
9. Alias labels are globally unique and explicitly typed as `code_spelling`,
   `compatibility`, `historical`, `discouraged`, or `rejected`.
10. Historical, discouraged, or rejected aliases never become automatic
    replacements. A future deprecation requires a reviewed replacement term,
    valid target ID, and an acyclic replacement chain.
11. Each calibration-scoped proposed term is referenced by at least one
    calibration candidate or explicitly marked as a critical ambiguity label;
    this prevents unreviewed glossary expansion.
12. Guarantee-registry term references resolve to a glossary ID. A candidate
    may reference a blocked term only as constrained context: it must remain
    unactivated, explicitly name each blocking conflict in its finding or
    wording-constraint fields, and may not treat the blocked definition as
    adopted claim authority. An active guarantee may reference only separately
    adopted terms from the normative glossary.
13. Glossary summaries may not introduce uncited `MUST`, `SHALL`, thresholds,
    algorithms, behavioral exceptions, or authority. Such content belongs in
    the owning contract or reviewed decision.
14. The proposal cannot be marked active, enter the normative read order, or be
    cited as adopted vocabulary while Gate 2 remains pending.
15. Link checking and any generated index/view are deterministic; regeneration
    must leave a clean tree.

## Gate 2 Model And Entry Review Decisions

Gate 2 review should decide only:

- whether the entry model and intended normative location fit the existing
  hierarchy;
- whether each of the 19 proposed entries is sufficiently narrow and correctly
  owned to remain a candidate for separate future adoption;
- whether `QT-TERM-006` and `QT-TERM-012` remain blocked pending explicit
  Check/Observation reconciliation;
- whether the deferred denominator is appropriate for the calibration boundary;
- whether the validation and guarantee-reference rules prevent accidental
  activation or vocabulary drift.

Approval adopts zero entries: not the 19 proposed terms, not the two blocked
terms, and not any deferred term or alias. It also must not be interpreted as
resolving any named Phase 1 conflict, activating any guarantee, or authorizing
a product-semantic change.
