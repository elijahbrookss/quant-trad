# Phase 2A Guarantee Calibration

This document records the Phase 2A calibration performed against frozen
baseline `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`. It is a non-normative audit
artifact. It does not change the existing authority hierarchy, settle a
terminology or normative conflict, certify a proof run, or activate a
guarantee.

All proof references below describe evidence that exists at the frozen
baseline. Phase 2A did not execute those proofs as guarantee attestations. Every
record therefore has verification status `NOT_RUN` and activation state
`unactivated`.

## Calibration Purpose And Scope

The batch contains twelve of the 75 Phase 1 candidates:

- the ten recommended core records `QT-GC-001`, `QT-GC-003`, `QT-GC-004`,
  `QT-GC-005`, `QT-GC-008`, `QT-GC-009`, `QT-GC-011`, `QT-GC-012`,
  `QT-GC-013`, and `QT-GC-014`;
- `QT-GC-070`, which exercises operational and manual proof for destructive
  recovery; and
- `QT-GC-073`, which exercises a validator capability ceiling and a partially
  enforced documentation policy.

This is a representative calibration set, not an exhaustive classification of
the system. It deliberately covers different authority, enforcement, proof,
environment, and conflict shapes:

| Proof shape | Calibration records |
| --- | --- |
| Causality and suffix invariance | `QT-GC-001` |
| Database constraints and append-only persistence | `QT-GC-003`, `QT-GC-005`, `QT-GC-012` |
| Provider-free read boundary and deterministic replay | `QT-GC-004`, `QT-GC-005`, `QT-GC-008` |
| Research evidence and Observation admission | `QT-GC-008`, `QT-GC-009` |
| Durable reporting projection | `QT-GC-011` |
| Runtime failure and projection degradation | `QT-GC-013` |
| CLI/API/MCP interface parity | `QT-GC-014` |
| Manual recovery and destructive-operation safety | `QT-GC-070` |
| Documentation validation and an explicit validator ceiling | `QT-GC-073` |
| Unresolved normative conflict | `QT-GC-009` / `QT-CONFLICT-007` |

The remaining 63 candidates retain their Phase 1 status. No conclusion in this
sample may be projected onto an unclassified candidate or used to claim
whole-system coverage.

## Calibration Rules

The frozen authority hierarchy remains:

1. platform contracts own normative platform behavior;
2. accepted ADRs record durable decisions within that contract hierarchy;
3. active boundary and architecture documents explain current ownership and
   flows; and
4. code, schema constraints, tests, and operational procedures provide
   conformance and enforcement evidence, but do not silently replace higher
   authority.

The detailed records use these static enforcement-assessment labels. They
describe frozen implementation evidence, not registry disposition or
activation:

- `enforced`: the frozen implementation contains a direct enforcement
  mechanism aligned with the candidate, with named proof evidence;
- `partial`: representative paths or part of the claim are enforced, but the
  broad claim, environment, or proof surface is incomplete;
- `contradicted`: applicable authorities or authority and implementation make
  incompatible claims.

The machine registry uses the approved classification taxonomy instead:

- `enforced`: an authoritative, current claim is statically aligned and has
  adequate enforcement and proof;
- `partially_enforced`: the intended claim is clear and has named backing, but
  conformance, enforcement, or proof remains below the `enforced` threshold;
- `candidate`: desirable or intended behavior is represented but is not an
  adopted normative promise;
- `implementation_property`: observable implementation behavior without an
  adopted normative promise;
- `superseded`: a prior statement explicitly replaced by later authority;
- `contradicted`: applicable authorities or authority and implementation make
  incompatible claims; and
- `unclear`: the evidence does not support a defensible classification.

Neither an `enforced` registry disposition nor an `enforced` static assessment
activates a guarantee. Gate 2 approval authorizes classification only.
Activation requires a distinct future guarantee-activation review, an admitted
registry record, successful commit-specific proof, and an attestation bound to
its proof environment and source revision. Static inspection is not a proof
`PASS`.

The proposed stable IDs below are semantic and do not encode a sequence number,
document path, or current owner. They are proposals for registry calibration,
not adopted terminology.

Accordingly, no Phase 2A row is `enforced`: the directly guarded rows with
partial proof remain `partially_enforced`; the two non-normative
decision-owned claims remain `candidate`; the conflict example is
`contradicted`; and the documentation capability ceiling is an
`implementation_property`. The independent `enforcement_maturity` axis retains
the observed mechanism strength. Every row remains
`activation_status: unactivated`.

The eight `partially_enforced` rows and the one `contradicted` row use
`remediation_status: pending`; Phase 2A does not fabricate a plan from a finding
or its `P1-Cxx` crosswalk alias. The two `candidate` rows and the one
`implementation_property` row use `not_required` under the current
classification. Concrete `QT-REM-*` records require Phase 2B owner/reviewer
work and are required before a whole-system partial or contradicted
classification can be complete.

## Batch Summary

| Candidate | Proposed stable ID | Primary owner | Registry disposition | Static conformance | Verification | Activation |
| --- | --- | --- | --- | --- | --- | --- |
| `QT-GC-001` | `QT-GUAR-KNOWN-AT-PREFIX-INVARIANCE` | market data / execution runtime | `partially_enforced` | representative paths aligned; universal path coverage absent | `NOT_RUN` | `unactivated` |
| `QT-GC-003` | `QT-GUAR-CANONICAL-FACT-APPEND-ONLY` | data persistence | `partially_enforced` | aligned | `NOT_RUN` | `unactivated` |
| `QT-GC-004` | `QT-GUAR-PROVIDER-FREE-CANONICAL-READS` | market data | `partially_enforced` | aligned; read-entrypoint inventory incomplete | `NOT_RUN` | `unactivated` |
| `QT-GC-005` | `QT-GUAR-FROZEN-DATASET-REPLAY` | market data / research data | `candidate` | aligned with deterministic pinning implementation | `NOT_RUN` | `unactivated` |
| `QT-GC-008` | `QT-GUAR-CHECK-PREVIEW-EVIDENCE-SEPARATION` | research orchestration | `candidate` | aligned | `NOT_RUN` | `unactivated` |
| `QT-GC-009` | `QT-GUAR-CHECK-OBSERVATION-ADMISSION` | research memory / research orchestration | `contradicted` | newer path aligned; legacy seam and older accepted ADR remain | `NOT_RUN` | `unactivated` |
| `QT-GC-011` | `QT-GUAR-REPORTS-DURABLE-TRUTH-PROJECTION` | reporting | `partially_enforced` | canonical reporting path aligned; repository-wide claim unproved | `NOT_RUN` | `unactivated` |
| `QT-GC-012` | `QT-GUAR-RUN-LIFECYCLE-LEDGER-AUTHORITY` | execution persistence | `partially_enforced` | aligned | `NOT_RUN` | `unactivated` |
| `QT-GC-013` | `QT-GUAR-RUNTIME-PERSISTENCE-FAILURE-BOUNDARY` | execution runtime / BotLens projection | `partially_enforced` | aligned; proof cases incomplete | `NOT_RUN` | `unactivated` |
| `QT-GC-014` | `QT-GUAR-SHARED-APPLICATION-CONTRACT` | platform interfaces | `partially_enforced` | strong for Research Check operations only | `NOT_RUN` | `unactivated` |
| `QT-GC-070` | `QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION` | recovery / data retention | `partially_enforced` | archive deletion aligned; real restore proof absent | `NOT_RUN` | `unactivated` |
| `QT-GC-073` | `QT-GUAR-ARCHITECTURE-DOC-INDEX-INTEGRITY` | architecture documentation | `implementation_property` | metadata/index checks exist; validator ceiling confirmed | `NOT_RUN` | `unactivated` |

## Calibration Records

### `QT-GC-001` — Known-at prefix invariance

- **Proposed stable ID:** `QT-GUAR-KNOWN-AT-PREFIX-INVARIANCE`.
- **Owner:** market data and execution runtime.
- **Authority and lifecycle:** active normative system contract at
  `docs/contracts/platform/00_system_contract.md:9-13`; accepted ADR 0044 at
  `docs/architecture/decisions/0044-enforce-known-at-prefix-invariance.md:39-48,61-70,88-108`.
- **Static enforcement assessment:** `partial`.
- **Static finding:** known-at selection is guarded in
  `src/market_data/requirements.py:403-475`; runtime interval visibility and
  frozen revisions are bounded in
  `portal/backend/service/market/runtime_market_data.py:213-250,344-362`;
  Indicator runtime timing is checked in
  `portal/backend/service/indicators/indicator_service/runtime_validation.py:750-792`;
  and reserve-state evaluation explicitly rejects future-known facts in
  `src/indicators/reserve_state/runtime.py:44-60`. These are substantial but do
  not prove that every causal source and runtime output uses the same boundary.
- **Named proof evidence:** Check and report suffix tests at
  `tests/test_portal/test_research_checks.py:101-134` and
  `tests/test_portal/test_report_data.py:186-244`; runtime known-at evidence at
  `tests/test_portal/test_runtime_events_repo.py:1506-1524`; adapter and
  persisted-runtime invariance at
  `tests/integration/runtime/test_reference_execution_scenarios.py:869-913`
  and `tests/integration/runtime/test_persisted_runtime_correctness.py:25-180`.
- **Required proof and environment:** credential-free deterministic fixtures;
  both backtest and paper adapters; repeated prefix/suffix comparisons; a
  generated inventory or static rule covering every causal output path; clean
  tree and exact-source attestation.
- **Open findings:** `CI-TRACE-001` and `ARCH-COVERAGE-001` prevent an
  exhaustive claim.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-003` — Append-only Canonical Fact revisions

- **Proposed stable ID:** `QT-GUAR-CANONICAL-FACT-APPEND-ONLY`.
- **Owner:** data persistence.
- **Authority and lifecycle:** accepted ADR 0063 at
  `docs/architecture/decisions/0063-use-schema-registered-canonical-facts.md:76-91,141-151,170-172`;
  the direct correction rule also appears in accepted ADR 0050 at
  `docs/architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md:90-95`.
  The Phase 1 candidate's ADR 0063 citation alone is not sufficient authority
  trace and should retain both references.
- **Static enforcement assessment:** `enforced`.
- **Static finding:** immutable revision identity and constraints are declared
  in `portal/backend/db/market_data_models.py:180-201`; database triggers reject
  updates and deletes in `portal/backend/db/session.py:840-903`; and locked,
  monotonic revision allocation followed by INSERT-only persistence is in
  `portal/backend/service/storage/repos/market_data.py:1948-2070`.
- **Named proof evidence:** correction and frozen-replay behavior at
  `tests/test_market_data/test_repository_db.py:191-222`; trigger installation
  and rejected update at
  `tests/test_market_data/test_canonical_fact_store_migration_db.py:318-327,404-411`.
- **Required proof and environment:** isolated disposable PostgreSQL/Timescale;
  clean schema bootstrap; concurrent correction allocation; successful append;
  rejected update and rejected delete; schema/trigger fingerprint captured in
  the attestation.
- **Open findings:** add direct delete-rejection proof. `SCHEMA-AUTH-001`
  remains relevant because schema authority is distributed.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-004` — Provider-free canonical reads

- **Proposed stable ID:** `QT-GUAR-PROVIDER-FREE-CANONICAL-READS`.
- **Owner:** market data.
- **Authority and lifecycle:** accepted ADR 0050 at
  `docs/architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md:41-46,55-57,72-75,83-90,120`.
- **Static enforcement assessment:** `enforced`.
- **Static finding:** provider use remains in explicit acquisition code at
  `portal/backend/service/market/feed_service.py:117-188`; the canonical feed
  reads stored facts and records no provider call at
  `portal/backend/service/market/feed_service.py:371-525`; repository reads are
  provider-neutral at
  `portal/backend/service/storage/repos/market_data.py:2219-2241,2828`.
- **Named proof evidence:** provider-trap read at
  `tests/test_market_data/test_feed_service.py:122-150`; database-backed frozen
  structured-Fact provider trap at
  `tests/test_market_data/test_structured_fact_research_path_db.py:243-264`.
- **Required proof and environment:** credential-free provider trap for every
  registered canonical read entrypoint; non-database unit profile plus an
  isolated database structured-Fact read; exact-source attestation.
- **Open findings:** the existing test proves a representative feed path, not a
  closed inventory of every canonical read. Cross-reference
  `ARCH-COVERAGE-001`.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-005` — Frozen Dataset replay

- **Proposed stable ID:** `QT-GUAR-FROZEN-DATASET-REPLAY`.
- **Owner:** market data and research data.
- **Authority and lifecycle:** accepted ADR 0050 at
  `docs/architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md:72-81,95-100`;
  accepted ADR 0051 at
  `docs/architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md:43-77,100-120`;
  accepted ADR 0063 generalization at
  `docs/architecture/decisions/0063-use-schema-registered-canonical-facts.md:176-186`.
- **Static enforcement assessment:** `enforced`, provided the human statement reflects
  the actual deterministic pinning model.
- **Static finding:** Dataset identity, hashes, watermarks, per-series ranges,
  sources, gaps and schema evidence are represented in
  `portal/backend/db/market_data_models.py:698-760`; Dataset tables are included
  in immutability protection at `portal/backend/db/session.py:865-866`;
  repeatable-read freezing and as-of selection occur in
  `portal/backend/service/storage/repos/market_data.py:2975-3073`; series and
  evidence hashes are built at `3279-3337`; identity and idempotent insertion at
  `3437-3465`; replay range expansion and post-watermark facts are rejected at
  `3564-3636`.
- **Wording constraint:** the implementation pins revisions through the exact
  series/range/source/as-of-watermark binding and content hashes. It does not
  require a manifest that literally enumerates every Fact revision row ID. If
  literal enumeration remains part of the claim, the disposition is `partial`.
- **Named proof evidence:** stable repeat freeze at
  `tests/test_market_data/test_repository_db.py:125-149`; source-bound gap
  evidence at `152-188`; correction isolation, new Dataset identity and range
  rejection at `191-229`.
- **Required proof and environment:** isolated PostgreSQL/Timescale; freeze,
  later correction, old replay, new freeze, hash comparison, tamper rejection
  and range-expansion rejection; manifest and source revision attached to the
  attestation.
- **Open findings:** `SCHEMA-AUTH-001`; do not strengthen the claim from
  deterministic pinning to row-ID enumeration without owner review.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-008` — Preview/evidence separation

- **Proposed stable ID:** `QT-GUAR-CHECK-PREVIEW-EVIDENCE-SEPARATION`.
- **Owner:** research orchestration.
- **Authority and lifecycle:** accepted ADR 0062 at
  `docs/architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md:47-62,81-90`;
  active Check boundary at
  `docs/architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md:61-87,164-187`.
- **Static enforcement assessment:** `enforced`.
- **Static finding:** preparation plans and freezes without creating Check or
  Observation state in `portal/backend/service/research/service.py:906-1036`;
  preview is explicitly non-persistent at `1039-1060`; evidence runs require an
  explicit evidence mode at `1063-1092`; persistence admits validated hashes
  and durable families at `1218-1257`.
- **Named proof evidence:** persistence traps and unqualified-run rejection at
  `tests/test_portal/test_research_evidence_service.py:258-315`; missing binding
  and preview-only family rejection at `317-353`; replay, dirty-tree and tamper
  cases at `401-469`; frozen provider-access checks at
  `tests/test_market_data/test_frozen_binding.py:84-146`; database end-to-end
  replay at
  `tests/test_market_data/test_structured_fact_research_path_db.py:133-402`.
- **Required proof and environment:** credential-free unit proof with
  persistence traps; isolated database end-to-end replay; dirty tree, source
  mismatch and tamper cases; exact-source attestation.
- **Open findings:** `QT-CONFLICT-007` affects subsequent Observation admission,
  not the preview/evidence split itself.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-009` — Check-to-Observation admission

- **Proposed stable ID:** `QT-GUAR-CHECK-OBSERVATION-ADMISSION`.
- **Owner:** research memory and research orchestration.
- **Authority and lifecycle:** older accepted ADR 0034 says every Check links an
  Observation and auto-creates one when absent at
  `docs/architecture/decisions/0034-use-research-checks-as-analytical-memory-evidence.md:51-66,104-112`.
  Newer accepted ADR 0062 restricts support to eligible frozen evidence at
  `docs/architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md:52-68`.
  The active Check boundary follows the newer rule at
  `docs/architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md:65-68,219-220`.
  ADR 0062 does not explicitly supersede ADR 0034.
- **Static enforcement assessment:** `contradicted`.
- **Static finding:** V2 classification admits completed, durable and replayable
  evidence in `portal/backend/service/research/service.py:500-568,1218-1257`;
  Observation creation rejects ineligible or unreplayable evidence and creates
  the support link at `1323-1392`. The legacy persistence path around `1129`
  still auto-creates an Observation, preserving the older semantic seam.
- **Named proof evidence:** legacy and unverifiable evidence rejection at
  `tests/test_portal/test_research_evidence_service.py:356-398`; eligible replay
  fixtures at `401-431`. No positive proof was found that actually creates an
  Observation from completed V2 evidence and verifies its support link.
- **Required proof after normative review:** positive completed V2 evidence to
  Observation and support link; negative preview, incomplete, blocked, legacy,
  non-durable and hash-mismatch cases; preferably an isolated database
  transaction proof.
- **Open conflict:** preserve `QT-CONFLICT-007` exactly: older Check documents
  require creation/linking for every Check, while ADR 0062 and current service
  prohibit preview or unqualified promotion. Phase 2A does not select a winner,
  mark either accepted ADR superseded, rewrite the candidate, or repair the
  legacy path.
- **Activation:** expressly prohibited. This record cannot become active until
  the normative conflict receives the required review and Gate 2 approval.

### `QT-GC-011` — Reporting as durable-truth projection

- **Proposed stable ID:** `QT-GUAR-REPORTS-DURABLE-TRUTH-PROJECTION`.
- **Owner:** reporting.
- **Authority and lifecycle:** accepted ADR 0010 at
  `docs/architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md:31-57`;
  active reporting boundary at
  `docs/architecture/reporting/REPORTING_BOUNDARY.md:36-54,64-103,137-178,188-197`.
- **Static enforcement assessment:** `partial`.
- **Static finding:** the canonical RunResearchDataset builder loads durable
  runs, events and trades at
  `portal/backend/service/reports/run_research_dataset.py:6884-6903`, derives
  reporting metrics and carries durable diagnostics/context at `6928-6987`,
  reads candidate lifecycle artifacts at `4607-4638`, and converts absent
  inputs to explicit unavailable/caveat state at `5427-5460`.
- **Named proof evidence:** durable-data construction without an artifact
  directory at `tests/test_portal/test_run_research_dataset.py:675-710`;
  missing-diagnostic unavailability at `811-829`; lifecycle from persisted
  report evidence at `936-1000`; captured Indicator/market context around
  `1819`.
- **Required proof and environment:** credential-free reporting unit suite;
  missing-input and no-artifact-directory cases; database materialization
  integration; generated inventory or static dependency rule for every report
  entrypoint that forbids Indicator rerun and guessed evidence.
- **Open findings:** the canonical dataset path conforms, but the words “all
  reports” remain broader than current absence proof. Cross-reference
  `ARCH-COVERAGE-001` and `CI-TRACE-001`.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-012` — Runtime event ledger lifecycle authority

- **Proposed stable ID:** `QT-GUAR-RUN-LIFECYCLE-LEDGER-AUTHORITY`.
- **Owner:** execution persistence.
- **Authority and lifecycle:** accepted ADR 0042 at
  `docs/architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md:38-63,85-94`;
  active reporting boundary isolation at
  `docs/architecture/reporting/REPORTING_BOUNDARY.md:49-54`.
- **Static enforcement assessment:** `enforced`.
- **Static finding:** lifecycle transition validation is in
  `portal/backend/service/storage/repos/lifecycle.py:134-187`; projection at
  `257-310`; canonical append and same-session projection at `310-409`; ledger
  rebuild at `412-430`. Non-lifecycle run upserts reject lifecycle fields in
  `portal/backend/service/storage/repos/runs.py:23,62-75`; runtime-event
  projection participates in the event transaction at
  `portal/backend/service/storage/repos/runtime_events.py:1272-1276,1509-1510`.
- **Named proof evidence:** append/project and projection failure at
  `tests/test_portal/test_lifecycle_repo.py:25-87,161-203`; canonical reads,
  rebuild and illegal-transition cases at `229-433`; direct lifecycle-field
  rejection at `tests/test_portal/test_run_storage_json_safety.py:60-70`;
  report-status isolation at
  `tests/test_reports/test_report_materialization.py:45-73,106-125`.
- **Required proof and environment:** existing repository unit proof plus
  isolated PostgreSQL proof of append/projection rollback atomicity and
  concurrent run-sequence ordering; exact schema and source attestation.
- **Open findings:** strongest transaction claim currently lacks real-database
  rollback proof. Cross-reference `SCHEMA-AUTH-001`.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-013` — Durable failure versus projection degradation

- **Proposed stable ID:** `QT-GUAR-RUNTIME-PERSISTENCE-FAILURE-BOUNDARY`.
- **Owner:** execution runtime, with BotLens projection as the secondary owner.
- **Authority and lifecycle:** active normative runtime contract at
  `docs/contracts/platform/01_runtime_contract.md:188-211`.
- **Static enforcement assessment:** `enforced`.
- **Static finding:** the canonical bounded writer and durable failure path are
  in `src/engines/bot_runtime/runtime/components/canonical_facts.py:153-176,239-297,378-419`;
  projection dispatch and bounded degradation at `437-490,536-633`; durable
  error raising versus projection warning at `841-859`; trade persistence
  buffering at `31-33,131-231`. Runtime execution drains durable work before
  terminal persistence and records failure in
  `src/engines/bot_runtime/runtime/mixins/execution_loop.py:241-265,338-349`; persistence
  wrappers reraise durable failures in
  `src/engines/bot_runtime/runtime/mixins/runtime_persistence.py:99-156`.
- **Named proof evidence:** canonical ordering, projection dispatch, overflow
  degradation and writer failure at
  `tests/integration/runtime/test_canonical_fact_appender.py:95-313`; trade writer
  failure, overflow, timeout and terminal flush at
  `tests/integration/runtime/test_bot_runtime_persistence_buffer.py:122-229`.
- **Required proof and environment:** credential-free runtime unit/integration
  profile; explicit canonical-buffer overflow and drain-timeout cases;
  projection drain-timeout and asynchronous consumer-failure cases; end-to-end
  proof that durable failure prevents successful terminal state while
  projection failure only degrades visibility.
- **Open findings:** implementation is aligned, but those failure combinations
  are not all represented in the named proof set.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-014` — Shared application contract across interfaces

- **Proposed stable ID:** `QT-GUAR-SHARED-APPLICATION-CONTRACT`.
- **Owner:** platform interfaces.
- **Authority and lifecycle:** active repository governance at `AGENTS.md:15-21`;
  active normative engineering contract at
  `docs/contracts/platform/03_engineering_contract.md:18-30`; active Check
  boundary map at
  `docs/architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md:189-224`.
- **Static enforcement assessment:** `partial`.
- **Static finding:** `cli/research_operations.py:1-135` is a thin application
  adapter; Check CLI paths use it in `cli/main.py:1999,2031-2091`; MCP research
  tools reuse it with interface-specific confirmation guards in
  `cli/mcp_server.py:473-570`.
- **Named proof evidence:** exact MCP route/payload and confirmation behavior at
  `tests/test_cli/test_mcp_server.py:275-346`; API Check delegation at
  `tests/test_portal/test_research_checks.py:2064-2124,2127,2217`.
- **Required proof and environment:** an operation-family manifest enumerating
  CLI, API and MCP exposure; route, payload, authorization/confirmation, result
  and error parity; a static rule prohibiting interface-owned domain
  calculations. The Research Check family can be the first proof environment,
  but it cannot certify unrelated interface families.
- **Open findings:** strong representative conformance does not establish the
  repository-wide claim across the Phase 1 inventory of CLI/API/MCP surfaces.
  Cross-reference `ARCH-COVERAGE-001` and `CI-TRACE-001`.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

<a id="qt-gc-070-destructive-recovery-verification"></a>

### `QT-GC-070` — Destructive recovery verification

- **Proposed stable ID:** `QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION`.
- **Owner:** recovery and data retention.
- **Authority and lifecycle:** active operational policy in
  `docs/engineering/server-deployment.md:344-363`; accepted ADR 0053 production
  evidence requirements at
  `docs/architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md:268-285`.
- **Static enforcement assessment:** `partial`.
- **Static finding:** archive deletion verifies the expected checksum before
  removal in `src/market_data/archive.py:538-568`; lifecycle execution rechecks
  retention pins, verifies any replacement object and records the operation at
  `portal/backend/service/market/market_storage_lifecycle.py:453-531`.
- **Named proof evidence:** checksum mismatch preserves the object and matching
  evidence permits deletion at
  `tests/test_market_data/test_market_structure_archive.py:291-314`.
- **Required proof and environment:** automated archive safety proof in a
  disposable filesystem; pin-race and corrupt/missing-replacement cases; a
  separately authorized, isolated backup/restore rehearsal with before/after
  database identity, row-count, constraint and application-read verification;
  checksummed archive-move rehearsal. Operational proof must capture operator,
  environment, timestamps, source revision, backup identity and recovery
  result. A production database is not an acceptable calibration target.
- **Open findings:** archive deletion has direct enforcement, but the deployment
  instruction to schedule and restore-test database backups is manual and no
  real restore proof is named. The attestation must report `UNAVAILABLE` rather
  than `PASS` when the isolated recovery environment is absent.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

### `QT-GC-073` — Architecture document and index integrity

- **Proposed stable ID:** `QT-GUAR-ARCHITECTURE-DOC-INDEX-INTEGRITY`.
- **Owner:** architecture documentation.
- **Authority and lifecycle:** accepted ADR 0001 requires frontmatter-backed
  discovery and regenerated indexes at
  `docs/architecture/decisions/0001-use-boundary-first-architecture-docs.md:34-53`;
  active but Phase 1-stale explanatory architecture model at
  `docs/architecture/ARCHITECTURE_DOCS_MODEL.md:107-118,141-151`. Neither
  architecture document is promoted to normative or operational-canonical
  authority by this calibration.
- **Static enforcement assessment:** `partial`.
- **Static finding:** `scripts/docs/build_architecture_index.py:13-95` parses
  frontmatter and generates component/code-path index rows, but silently skips
  files with no frontmatter at `55-60` and does not validate field values, path
  existence or links. `tests/contract/test_architecture_docs_index.py:16-38`
  checks required key substrings and two expected index references, not exact
  regeneration or reference integrity. Phase 1 independently found that the
  frozen generated index exactly represented the 114 eligible component files;
  that discovery result is not a general validator capability.
- **Named proof evidence:** architecture frontmatter contract tests at
  `tests/contract/test_architecture_docs_index.py:16-38` and the
  `make validate-docs` wiring at `Makefile:468-470`; both remain `NOT_RUN` for this
  calibration.
- **Required proof and environment:** credential-free documentation validation;
  invalid/missing frontmatter cases; schema/value validation; duplicate
  component and mapping detection; existence checks for `code_paths`; internal
  link validation; regeneration followed by an exact clean-tree comparison.
  The proof result must publish the validator capability version so a narrow
  validator is not mistaken for wider integrity coverage.
- **Open findings:** `DOC-MODEL-001`, `DOC-VALIDATION-001`, `P1-C06`, and
  `P1-C07`. Phase 2A does not change the architecture validator or declare the
  missing checks satisfied.
- **Activation:** prohibited before Gate 2; verification remains `NOT_RUN`.

## Proof And Attestation Requirements Exposed By The Batch

The batch demonstrates that a single Boolean test result is not an adequate
proof model. A later attestation must bind at least:

- stable guarantee ID and exact guarantee revision;
- frozen source commit and clean/dirty source state;
- proof definition version and the exact commands or manual procedure used;
- evidence paths and hashes;
- execution profile and environment identity, including database engine and
  extension versions when applicable;
- start/end timestamps and runner or operator identity;
- outcome `PASS`, `FAIL`, `NOT_RUN`, `MANUAL`, `PARTIAL`, or `UNAVAILABLE`,
  without converting a missing environment into a pass;
- static, automated, database, integration, manual and operational proof kinds
  as separate evidence items;
- applicable scope, including enumerated entrypoints or adapters where the
  claim uses “every”, “all”, or “never”;
- unresolved finding and conflict references; and
- activation decision and approving gate, separately from proof outcome.

Representative tests establish examples. Universal negative claims such as “no
provider fallback”, “all reports”, or “no parallel interface truth” also need a
closed surface inventory, a static dependency rule, or another mechanism that
defines the proof denominator. Database guarantees need both application-level
tests and database-level constraint/transaction evidence. Manual recovery proof
must be reproducible and independently reviewable, but must never run against
production merely to satisfy calibration.

## Gate 2 Stop

Phase 2A stops with these provisional classifications and proof requirements.
Before any whole-system classification or activation, Gate 2 review must decide:

1. whether the glossary, registry schema, stable-ID rules, generated or
   validated human view, and attestation model are adequate;
2. whether the proposed dispositions and wording constraints in this batch are
   calibrated consistently;
3. whether broad claims have an acceptable proof denominator or must be narrowed;
4. how `QT-CONFLICT-007` will be reviewed, without resolving it implicitly;
5. which manual and database proof environments are admissible; and
6. whether any record may proceed to an activation review after successful,
   commit-specific proof.

Until that approval:

- all twelve records remain unactivated and `NOT_RUN`;
- no guarantee may be advertised, enforced as new policy, or used as a release
  claim from this document;
- no unresolved term is adopted as canonical;
- no normative conflict is resolved and no accepted ADR is marked superseded;
- no product semantic repair is authorized;
- no document, schema, test, code, or historical material is deleted or
  archived; and
- no classification may be extrapolated to the other 63 Phase 1 candidates.
