# Proposed QT Platform Glossary

> **Phase 2B whole-system proposal — non-normative.** This document is a review artifact for
> frozen baseline `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`. It adopts no
> terminology, activates no guarantee, resolves no conflict, and changes no
> product semantics. Existing platform contracts and accepted decisions retain
> their current authority.

## Purpose And Intended Placement

Phase 2A calibrated the entry model on 21 terms. Phase 2B now accounts for all
55 Phase 1 term records: 34 further entries are added without adopting any
meaning, alias, deprecation, or replacement. Entries remain proposed, blocked,
or deferred according to their existing authority and unresolved conflicts.

Gate 2 approved the model for whole-system classification but adopted zero
entries. Non-blocked entries can proceed only to a separate
terminology-adoption review. Any later adoption must create the normative vocabulary index at
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

The preserved Phase 2A calibration scope contains exactly 21 entries: 19
proposals and two blocked entries. In that phase, proposed meant eligible for
Gate 2 model review, not adopted.

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

## Phase 2B Whole-System Vocabulary Expansion

The following 34 Phase 1 terms were deferred during calibration. Phase 2B
records each one explicitly for whole-system guarantee classification.
`proposed` means eligible for a later terminology-adoption review; `deferred`
means the source/ownership model needs review first. Neither status is
adoption, and the registry remains non-normative.

### `QT-TERM-016` — Candle continuity gap

- Entry kind: `qualification_rule`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `data / runtime`
- Whole-system classification uses: QT-GC-017
- Frozen Phase 1 source evidence: runtime contract lines 239-248;
  `src/core/candle_continuity.py:16-177`
- Summary strategy: Index the Phase 1 distinction without adding behavior: One exact
  candle-continuity classification.
- Usage boundary: Use expected-session, provider-missing, ingestion-failure,
  runtime-missing, projection-missing, or unknown
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-014`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-017` — Order-book sequence gap

- Entry kind: `qualification_rule`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `market-structure`
- Whole-system classification uses: QT-GC-024
- Frozen Phase 1 source evidence: market-structure data-plane lines 421-490
- Summary strategy: Index the Phase 1 distinction without adding behavior: Missing
  sequence evidence that invalidates book reconstruction.
- Usage boundary: Not candle coverage evidence
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-014`, `QT-CONFLICT-015`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-018` — Runner clock gap / overlay clock gap

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `execution-runtime / indicator-runtime`
- Whole-system classification uses: QT-GC-054, QT-GC-067
- Frozen Phase 1 source evidence: ADR 0021 line 42; runtime contract lines 99-102
- Summary strategy: Index the Phase 1 distinction without adding behavior: Separate
  runner-liveness and projection-invalidation gaps.
- Usage boundary: Not source-data gaps
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-014`, `QT-CONFLICT-022`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-019` — Book Validity Interval

- Entry kind: `domain_term`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `market-structure`
- Whole-system classification uses: QT-GC-024
- Frozen Phase 1 source evidence: market-structure data-plane lines 410-490;
  `portal/backend/db/market_data_models.py:1484`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Interval over
  which reconstructed book state is valid.
- Usage boundary: Avoid generic “valid interval”
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-014`, `QT-CONFLICT-015`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-020` — Raw record

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `raw-archive`
- Whole-system classification uses: QT-GC-022
- Frozen Phase 1 source evidence: ADR 0053 lines 72-102; market-structure data-plane
  line 773 onward
- Summary strategy: Index the Phase 1 distinction without adding behavior: One provider
  frame record with deterministic `raw_record_id`.
- Usage boundary: Not a raw archive object or Canonical Fact revision
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-014`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-021` — Raw archive object / manifest

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `raw-archive`
- Whole-system classification uses: QT-GC-022, QT-GC-023
- Frozen Phase 1 source evidence: `portal/backend/db/market_data_models.py:1118,1186`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Durable
  placement/manifest for one or more preassigned raw records.
- Usage boundary: Preserve record-versus-object distinction
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-014`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-025` — Live runtime/composition

- Entry kind: `qualification_rule`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `execution-runtime`
- Whole-system classification uses: QT-GC-015
- Frozen Phase 1 source evidence: ADR 0049 lines 33-62
- Summary strategy: Index the Phase 1 distinction without adding behavior: A live
  composition/runtime label, without venue-trading authority.
- Usage boundary: “Live” does not mean external order submission
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-025`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-027` — Provider ID / Venue ID / exchange slug

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `data-provider`
- Whole-system classification uses: QT-GC-016, QT-GC-019, QT-GC-020
- Frozen Phase 1 source evidence: `src/data_providers/registry.py:40-58,124-174`; data
  boundary lines 96-105; runtime contract lines 50-58
- Summary strategy: Index the Phase 1 distinction without adding behavior: Provider ID
  selects an implementation and capability/auth contract; Venue ID selects one concrete
  market route; an exchange slug is an adapter translation.
- Usage boundary: Provider, venue, datasource, and exchange are not interchangeable
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-011`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-028` — Canonical Instrument / provider product identity

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `identity / data`
- Whole-system classification uses: QT-GC-016, QT-GC-020, QT-GC-037
- Frozen Phase 1 source evidence: `portal/backend/db/models.py:986-1016`;
  market-structure Phase 4 lines 129-132; BotLens boundary lines 222-228
- Summary strategy: Index the Phase 1 distinction without adding behavior: Canonical
  Instrument is the platform identity keyed by `instrument_id`; symbol and product ID
  are provider/venue-facing identifiers resolved against it.
- Usage boundary: Never use a display symbol as a globally stable instrument key
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-012`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-029` — Source Identity / Series Identity

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `data`
- Whole-system classification uses: QT-GC-016, QT-GC-020
- Frozen Phase 1 source evidence: `src/market_data/contracts.py:170-205`;
  `portal/backend/db/market_data_models.py:29-78`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Source
  Identity identifies acquisition provenance; Series Identity identifies a typed fact
  stream for an instrument, timeframe, and dimensions.
- Usage boundary: A provider, source, series, and instrument are four separate
  identities
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-011`, `QT-CONFLICT-012`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-030` — Collector Definition / collector adapter / collector worker

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `data collection`
- Whole-system classification uses: QT-GC-021, QT-GC-025
- Frozen Phase 1 source evidence: collector operations boundary lines 41-78; continuous
  collector boundary lines 112-141;
  `portal/backend/service/market/collector_supervisor.py:63-176`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Definition is
  durable reviewed configuration; adapter is code that implements one stream contract;
  worker is the running owner of a definition.
- Usage boundary: Bare Collector is unsafe when configuration, code, and process
  ownership differ
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-013`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-031` — Enrollment / collector operation

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `data operations`
- Whole-system classification uses: QT-GC-025
- Frozen Phase 1 source evidence: collector operations boundary lines 46-77 and 215-229;
  continuous collector boundary lines 263-277
- Summary strategy: Index the Phase 1 distinction without adding behavior: Enrollment
  admits reviewed product/stream definitions; an operation requests start, stop,
  restart, pause, or resume against enrolled configuration.
- Usage boundary: Enrollment neither starts a collector nor proves production readiness
  by itself
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-013`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-032` — Qualified Coverage

- Entry kind: `qualification_rule`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `data / reporting`
- Whole-system classification uses: QT-GC-017, QT-GC-021, QT-GC-022, QT-GC-023,
  QT-GC-026
- Frozen Phase 1 source evidence: numeric-facts boundary lines 239-285; market-structure
  data plane lines 329-330 and 579-581; data boundary lines 273-280
- Summary strategy: Index the Phase 1 distinction without adding behavior: Acquisition
  coverage, stream coverage interval, archive coverage, Dataset coverage, and reporting
  coverage are separately owned evidence.
- Usage boundary: Complete in one coverage namespace does not certify another
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-014`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-033` — Archive mapping / archive coverage / retention pin

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `raw-archive`
- Whole-system classification uses: QT-GC-022, QT-GC-023, QT-GC-026
- Frozen Phase 1 source evidence: market-structure data plane lines 315-330, 874-889,
  and 1017-1023; `portal/backend/db/market_data_models.py:1118-1307`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Mapping links
  raw records to immutable archive objects; archive coverage proves durable placement
  over a range; a retention pin prevents eligible object expiry.
- Usage boundary: None is a Canonical Fact, Dataset freeze, or book-validity certificate
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-014`, `QT-CONFLICT-015`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-034` — Reconstructed Book State / book checkpoint / execution-book tape

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `market-structure / execution-runtime`
- Whole-system classification uses: QT-GC-024, QT-GC-049
- Frozen Phase 1 source evidence: market-structure data plane lines 380-385 and 481-487;
  Phase 3A book execution lines 58-90
- Summary strategy: Index the Phase 1 distinction without adding behavior: Book state is
  disposable current reconstruction; checkpoint accelerates deterministic replay; a
  certified execution tape is a frozen runtime input over causal snapshots.
- Usage boundary: A checkpoint or tape is not provider truth, and hot book state is
  never Dataset truth
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-015`, `QT-CONFLICT-020`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-035` — Indicator definition / Indicator config / runtime Indicator instance

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `indicator-runtime`
- Whole-system classification uses: QT-GC-027 through QT-GC-031
- Frozen Phase 1 source evidence: indicator boundary lines 28-44 and 87-109;
  `src/indicators/registry.py:18-64`; `src/indicators/manifest.py:161-181`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Definition
  owns type contract and manifest; config is persisted parameterization; runtime
  instance owns private walk-forward state.
- Usage boundary: Qualify Indicator when type, saved config, and stateful runtime object
  could differ
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-017`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-036` — Typed Indicator Output / output catalog / output readiness

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `indicator-runtime`
- Whole-system classification uses: QT-GC-002, QT-GC-027 through QT-GC-031
- Frozen Phase 1 source evidence: indicator boundary lines 44-57, 129-136, and 166-193;
  `src/indicators/manifest.py:65-104`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Catalog
  declares every public output; each bar returns every declared typed output;
  `ready=false` means present but not yet usable.
- Usage boundary: Output preference or visibility must not rewrite Indicator truth
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-017`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-037` — Indicator lifecycle output

- Entry kind: `domain_term`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `indicator-runtime / research`
- Whole-system classification uses: QT-GC-031
- Frozen Phase 1 source evidence: indicator boundary lines 97-105 and 187-191; runtime
  contract lines 71-79
- Summary strategy: Index the Phase 1 distinction without adding behavior: Public typed
  research evidence about candidate/setup progression owned by an Indicator.
- Usage boundary: Not a Bot Run, order, trade, collector, or deployment lifecycle event
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-017`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-038` — Overlay contract / overlay snapshot / overlay delta

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `indicator-runtime / BotLens`
- Whole-system classification uses: QT-GC-002, QT-GC-030, QT-GC-054
- Frozen Phase 1 source evidence: runtime contract lines 81-111 and 139-170;
  `src/overlays/registry.py:15-135`; BotLens boundary lines 522-559
- Summary strategy: Index the Phase 1 distinction without adding behavior: Contract
  names renderable payload semantics; snapshot is full current visual state; delta is
  bounded projection transport with its own clock.
- Usage boundary: Overlays are projections, never Strategy inputs or canonical execution
  state
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-016`, `QT-CONFLICT-017`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-039` — Signal ID / Decision ID / order and fill IDs

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `decision / execution`
- Whole-system classification uses: QT-GC-032, QT-GC-040 through QT-GC-045
- Frozen Phase 1 source evidence: decision boundary lines 75-105; Phase 2B order
  lifecycle lines 88-91
- Summary strategy: Index the Phase 1 distinction without adding behavior: Distinct
  causal identities link an Indicator signal to a Strategy decision and later execution
  artifacts without aliasing them.
- Usage boundary: Sharing provenance does not make signal, decision, order, attempt,
  event, and fill the same object
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-018`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-040` — Strategy definition / Compiled Strategy / effective strategy / run strategy snapshot

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `decision-layer`
- Whole-system classification uses: QT-GC-032 through QT-GC-035
- Frozen Phase 1 source evidence: decision boundary lines 87-118 and 119-141; ADR 0018
  lines 82-90 and 158-168
- Summary strategy: Index the Phase 1 distinction without adding behavior: Authored
  rules compile to executable semantics; variant resolution produces the effective
  strategy; run start freezes the exact snapshot.
- Usage boundary: “Strategy” must not conceal which stage or frozen identity is meant
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-019`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-041` — Strategy Variant / Output Filter

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `decision-layer`
- Whole-system classification uses: QT-GC-033, QT-GC-034
- Frozen Phase 1 source evidence: ADR 0018 lines 66-93, 154-189; decision boundary lines
  101-116
- Summary strategy: Index the Phase 1 distinction without adding behavior: A Variant is
  a named diff whose output filters add deterministic conditions over public outputs
  already attached to the base Strategy.
- Usage boundary: A Variant does not own ATM selection, Indicator config, or a second
  evaluator
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-019`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-042` — Instrument Execution Profile

- Entry kind: `domain_term`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `execution-runtime`
- Whole-system classification uses: QT-GC-037
- Frozen Phase 1 source evidence: Phase 1 economic execution contract lines 117-139;
  reporting boundary lines 215-225
- Summary strategy: Index the Phase 1 distinction without adding behavior: Persisted
  runtime-readiness and economic semantics for one instrument, including capability,
  fee, margin, and model references.
- Usage boundary: Not provider metadata, a Strategy setting, or proof of external-order
  authority
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-020`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-043` — Resolved Execution Context / context bundle

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `execution-runtime`
- Whole-system classification uses: QT-GC-038
- Frozen Phase 1 source evidence: Phase 2A execution context lines 55-80 and 160-168;
  `src/engines/bot_runtime/core/execution_assumptions.py:1`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Immutable
  per-series binding of exact execution contracts; a bundle pins one context per runtime
  series for a run.
- Usage boundary: Context binds execution assumptions; it does not own Strategy meaning,
  accounting, or authorization
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-020`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-044` — Runtime Execution Plan / Canonical Order Request

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `execution-runtime`
- Whole-system classification uses: QT-GC-039 through QT-GC-044
- Frozen Phase 1 source evidence: Phase 2A lines 114-121 and 164-168; Phase 2B lines
  57-91
- Summary strategy: Index the Phase 1 distinction without adding behavior: Plan is the
  run-time intended action; Canonical Order Request begins durable order quantity and
  policy custody.
- Usage boundary: Neither is a fill, and the plan is not the durable order lifecycle
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-020`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-045` — Canonical Order Lifecycle / `FillOrder` / fill

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `execution-runtime`
- Whole-system classification uses: QT-GC-040 through QT-GC-045
- Frozen Phase 1 source evidence: Phase 2B lines 57-91, 133-165, and 180-196
- Summary strategy: Index the Phase 1 distinction without adding behavior: Lifecycle is
  append-only durable order truth; `FillOrder` is a compatibility request to an
  execution adapter; fill is one executed quantity event.
- Usage boundary: Never describe `FillOrder` as the durable order or a fill as the whole
  order
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-018`, `QT-CONFLICT-020`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-046` — Wallet state / Wallet Ledger fact / wallet commit clock

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `execution-runtime`
- Whole-system classification uses: QT-GC-045 through QT-GC-048
- Frozen Phase 1 source evidence: wallet boundary lines 72-120 and 123-146; runtime
  contract lines 114-130
- Summary strategy: Index the Phase 1 distinction without adding behavior: Wallet state
  is current capital truth; ledger facts are replayable transitions; `wallet_commit_seq`
  orders shared-wallet mutation.
- Usage boundary: Report/BotLens wallet views and runtime-event append order are not
  alternate wallet truth
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-021`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-048` — Qualified Cursor

- Entry kind: `qualification_rule`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `identity / BotLens`
- Whole-system classification uses: QT-GC-051, QT-GC-054
- Frozen Phase 1 source evidence: runtime contract lines 121-146; BotLens boundary lines
  222-228, 333-356, 438-440, and 550-559
- Summary strategy: Index the Phase 1 distinction without adding behavior: `run_seq`,
  stream `base_seq`, `after_seq`/`after_row_id`, overlay commit sequence, and trade
  revision are distinct ordered positions.
- Usage boundary: Bare Cursor or Sequence invites cross-clock comparison
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-022`, `QT-CONFLICT-024`. Qualification records
  the collision but does not resolve subsystem semantics.

### `QT-TERM-050` — Lease / claim / ownership fence

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `persistence / orchestration`
- Whole-system classification uses: QT-GC-060, QT-GC-061
- Frozen Phase 1 source evidence: ADR 0047 lines 31-44 and 68-88; research async-job
  boundary lines 54-79; ADR 0025
- Summary strategy: Index the Phase 1 distinction without adding behavior: Lease or
  claim grants time-bounded work ownership; the fence is the token-and-generation check
  that prevents a stale owner from committing.
- Usage boundary: A row lock, heartbeat, lease timestamp, token, and generation are
  related but not synonyms
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-023`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-051` — Async Job / job-owned effect

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `research-orchestration`
- Whole-system classification uses: QT-GC-061
- Frozen Phase 1 source evidence: research async-job boundary lines 28-79;
  `portal/backend/service/async_jobs/repository.py`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Durable queue
  item whose current fenced claim may append its result and associated domain effects
  atomically.
- Usage boundary: A job is neither the Check it dispatches nor the worker process that
  claims it
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-023`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-052` — Semantic fingerprint / operational fingerprint

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `reporting`
- Whole-system classification uses: QT-GC-071
- Frozen Phase 1 source evidence: ADR 0015 lines 29-60; reporting boundary lines 255-268
  and 303-333
- Summary strategy: Index the Phase 1 distinction without adding behavior: Semantic
  fingerprint covers stable trading behavior and material identity; operational
  fingerprint covers diagnostics, ordering, availability, and runtime drift.
- Usage boundary: Operational drift can coexist with semantic equivalence
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-024`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-053` — Report input fingerprint / data snapshot hash

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `reporting`
- Whole-system classification uses: QT-GC-011, QT-GC-071
- Frozen Phase 1 source evidence: ADR 0031 lines 39-54; reporting boundary lines 60-79
  and 142-146
- Summary strategy: Index the Phase 1 distinction without adding behavior: Input
  fingerprint validates one report materialization against durable run inputs; data
  snapshot hash identifies exact runtime-consumed data material.
- Usage boundary: Neither is the semantic or operational fingerprint
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-024`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-054` — Deployment contract / strategy deployment authority

- Entry kind: `contrast_set`
- Proposal status: `proposed`
- Term lifecycle: `current`
- Source lifecycle: accepted decision and/or active platform contract as indexed in
  Phase 1
- Owner: `platform / security`
- Whole-system classification uses: QT-GC-015, QT-GC-059, QT-GC-069
- Frozen Phase 1 source evidence: `.github/workflows/test.yaml:53-125`; ADR 0049 lines
  33-62; autonomous research roadmap lines 159-164
- Summary strategy: Index the Phase 1 distinction without adding behavior: Deployment
  contract validates build, Compose, source attestation, and bootstrap; strategy
  deployment authority would permit an artifact to control external execution.
- Usage boundary: Successful deployment CI does not open external-order authority
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-025`. Qualification records the collision but
  does not resolve subsystem semantics.

### `QT-TERM-055` — Make target / CI job / test suite / pytest profile

- Entry kind: `contrast_set`
- Proposal status: `deferred`
- Term lifecycle: `current`
- Source lifecycle: mixed explanatory or source-owned evidence; authority review pending
- Owner: `platform / CI`
- Whole-system classification uses: QT-GC-072, QT-GC-074, QT-GC-075
- Frozen Phase 1 source evidence: `Makefile:454-493`;
  `scripts/ci/run_test_suite.sh:4-118`; `.github/workflows/test.yaml:9-125`
- Summary strategy: Index the Phase 1 distinction without adding behavior: Separate
  validation selectors: developer aggregation, hosted workflow lane, shell-runner suite,
  and conftest file-selection profile.
- Usage boundary: Passing one selector must not be reported as passing all validation
  layers
- Alias handling: No alias, deprecation, or replacement is adopted; labels
  separated by slashes remain contrasted spellings or concepts pending review.
- Conflict handling: Retain `QT-CONFLICT-026`. Qualification records the collision but
  does not resolve subsystem semantics.

## Calibration Mapping Preserved

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

## Phase 2B Vocabulary Accounting

All 55 Phase 1 term IDs now have an explicit proposal-model entry: 34 are
`proposed`, two (`QT-TERM-006` and `QT-TERM-012`) remain `blocked`, and
19 remain `deferred` pending source/ownership review. Zero entries are
adopted. No new term ID or terminology conflict was introduced by this
mechanical expansion.

All `QT-ALIAS-001` through `QT-ALIAS-020` remain Phase 1 findings. Phase 2B
does not deprecate, replace, remove, or normalize any alias.

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
11. Each proposed or deferred term is referenced by at least one whole-system
    candidate or explicitly marked as a critical ambiguity label; this prevents
    unreviewed glossary expansion.
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
    cited as adopted vocabulary without a separate terminology-adoption review.
15. Link checking and any generated index/view are deterministic; regeneration
    must leave a clean tree.

## Phase 2B Terminology Review Boundary

Phase 2B classification does not adopt any entry. Later terminology review
must decide whether each proposed entry has sufficient authority and ownership,
whether each deferred entry can advance, and whether the two blocked entries
have received the required conflict resolution. No alias or replacement may be
adopted by implication from the registry.
