# Assurance Maintenance

This document explains how QT maintains the architecture knowledge preserved in
the guarantee inventory without making formal proof publication the default for
ordinary development. It implements
[ADR 0066](../architecture/decisions/0066-scale-assurance-to-consequence-and-trust-boundaries.md)
and is the complete ongoing-treatment crosswalk for the 75 existing identifiers.

## Current State

- The registry contains 75 records, and all 75 remain unactivated.
- The proof catalog contains 85 proof definitions. A definition describes
  desired evidence; it is not a proof result.
- The remediation directory contains 68 proposed records. They retain their
  existing lifecycle and review state.
- The 22 core-promise constituents, 51 owned engineering invariants, and two
  historical/deferred properties below are an ongoing-maintenance overlay. They
  do not change the registry's classifications or authority references.

The human summary of the 22 constituents is
[`docs/core-promises.md`](../core-promises.md). The generated, exact inventory
remains [`GUARANTEES.md`](../assurance/guarantees/GUARANTEES.md).

## What Remains Active

The active assurance model is deliberately small:

- platform contracts and accepted ADRs remain the authority for product
  meaning;
- subsystem owners maintain ordinary unit, integration, static, and frontend
  tests for relevant changes;
- database tests use disposable isolated databases;
- tests involving secrets use synthetic credentials and exclude live values;
- external order submission remains disabled while evidence is collected;
- destructive recovery uses a separately approved isolated rehearsal;
- the registry validator and generated-view check protect the preserved
  inventory from accidental corruption; and
- release or exact-build evidence is added only for an actual evidence audience
  and trust boundary.

Normal test output is internal engineering evidence. It does not need an
execution admission, immutable attestation, or staged publication.

## What Is Retained

The following remain in the repository for audit traceability and possible
future high-trust use:

- `registry.json`, `proof-catalog.json`, the generated human view, all 68
  remediation records, and their schemas;
- the exact-source runner, environment-admission, cleanup, attestation, and
  publication implementation;
- the isolated recovery procedure; and
- the frozen review packets under `docs/plans/documentation-reconciliation/`,
  including their original filenames and terminology.

Retained machinery is not a recurring obligation. It must not be deleted,
rewritten, or treated as current activation evidence merely because this
maintenance model is simpler.

## When Stronger Evidence Is Justified

Before requiring evidence stronger than normal tests, record:

1. the consequence of the property being wrong;
2. the person or system that must trust the result;
3. the trust boundary ordinary CI cannot satisfy;
4. the owner and failure response;
5. the trigger or cadence; and
6. the evidence-retention need.

Use an isolated database for real persistence semantics, a reviewed rehearsal
for destructive recovery, and release evidence for a supported release. Reserve
exact source, runner image, daemon, wheelhouse, admission, immutable attestation,
and publication records for an external, security, legal, customer, capital, or
supply-chain boundary that actually needs them.

## Ongoing-Treatment Crosswalk

Promise labels refer to the six headings in
[`docs/core-promises.md`](../core-promises.md). `CI` means the ordinary test or
static-validation path on relevant changes. `DB` means a disposable isolated
database. `Release` and `rehearsal` are event-driven, not per-change ceremony.
Proof and remediation links preserve the existing record; they do not report a
PASS or completed remediation.

| Existing property | Ongoing treatment | Default check | Preserved proof and remediation records |
| --- | --- | --- | --- |
| [`QT-GUAR-KNOWN-AT-PREFIX-INVARIANCE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-known-at-prefix-invariance) | Core constituent — Promise 1 | CI on causal-path changes | [`QT-PROOF-001`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-001); [`QT-REM-001`](../assurance/guarantees/remediations/QT-REM-001.md) |
| [`QT-GUAR-DERIVED-OUTPUT-TIMELINE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-derived-output-timeline) | Core constituent — Promise 3 | CI on runtime, Strategy, projection, or reporting changes | [`QT-PROOF-100`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-100); [`QT-REM-100`](../assurance/guarantees/remediations/QT-REM-100.md) |
| [`QT-GUAR-CANONICAL-FACT-APPEND-ONLY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-canonical-fact-append-only) | Core constituent — Promise 1 | DB on canonical-fact persistence changes | [`QT-PROOF-002`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-002); [`QT-REM-002`](../assurance/guarantees/remediations/QT-REM-002.md) |
| [`QT-GUAR-PROVIDER-FREE-CANONICAL-READS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-provider-free-canonical-reads) | Core constituent — Promise 1 | CI; DB when structured persistence paths change | [`QT-PROOF-003`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-003), [`QT-PROOF-004`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-004); [`QT-REM-003`](../assurance/guarantees/remediations/QT-REM-003.md) |
| [`QT-GUAR-FROZEN-DATASET-REPLAY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-frozen-dataset-replay) | Core constituent — Promise 2 | DB on freeze, correction, or replay changes | [`QT-PROOF-004`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-004), [`QT-PROOF-005`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-005); no remediation |
| [`QT-GUAR-BACKTEST-FROZEN-BINDING`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-backtest-frozen-binding) | Core constituent — Promise 2 | CI on backtest binding changes | [`QT-PROOF-101`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-101); no remediation |
| [`QT-GUAR-DATASET-REALITY-CONSUMER-ADMISSION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-dataset-reality-consumer-admission) | Owned engineering invariant | CI on dataset-consumer admission changes | [`QT-PROOF-102`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-102); no remediation |
| [`QT-GUAR-CHECK-PREVIEW-EVIDENCE-SEPARATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-check-preview-evidence-separation) | Core constituent — Promise 2 | CI; DB when persisted evidence binding changes | [`QT-PROOF-004`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-004), [`QT-PROOF-006`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-006), [`QT-PROOF-007`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-007); no remediation |
| [`QT-GUAR-CHECK-OBSERVATION-ADMISSION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-check-observation-admission) | Core constituent — Promise 2 | CI on Check or Observation admission changes | [`QT-PROOF-006`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-006), [`QT-PROOF-008`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-008); [`QT-REM-004`](../assurance/guarantees/remediations/QT-REM-004.md) |
| [`QT-GUAR-CHECK-AUTHORITY-CEILING`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-check-authority-ceiling) | Core constituent — Promise 2 | CI on Check verdict or promotion changes | [`QT-PROOF-103`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-103); no remediation |
| [`QT-GUAR-REPORTS-DURABLE-TRUTH-PROJECTION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-reports-durable-truth-projection) | Owned engineering invariant | CI on report builders and projections | [`QT-PROOF-009`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-009); [`QT-REM-005`](../assurance/guarantees/remediations/QT-REM-005.md) |
| [`QT-GUAR-RUN-LIFECYCLE-LEDGER-AUTHORITY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-run-lifecycle-ledger-authority) | Owned engineering invariant | CI; DB for transaction or concurrency claims | [`QT-PROOF-010`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-010); [`QT-REM-006`](../assurance/guarantees/remediations/QT-REM-006.md) |
| [`QT-GUAR-RUNTIME-PERSISTENCE-FAILURE-BOUNDARY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-runtime-persistence-failure-boundary) | Owned engineering invariant | CI on persistence buffering and shutdown | [`QT-PROOF-011`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-011); [`QT-REM-007`](../assurance/guarantees/remediations/QT-REM-007.md) |
| [`QT-GUAR-SHARED-APPLICATION-CONTRACT`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-shared-application-contract) | Owned engineering invariant | CI on CLI, API, or MCP operations | [`QT-PROOF-012`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-012); [`QT-REM-008`](../assurance/guarantees/remediations/QT-REM-008.md) |
| [`QT-GUAR-EXTERNAL-ORDER-SUBMISSION-CLOSED`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-external-order-submission-closed) | Core constituent — Promise 4 | CI and every supported release boundary | [`QT-PROOF-104`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-104); [`QT-REM-101`](../assurance/guarantees/remediations/QT-REM-101.md) |
| [`QT-GUAR-CANONICAL-MARKET-IDENTITY-ROUTING`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-canonical-market-identity-routing) | Owned engineering invariant | CI on identity and routing changes | [`QT-PROOF-400`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-400); [`QT-REM-400`](../assurance/guarantees/remediations/QT-REM-400.md) |
| [`QT-GUAR-TYPED-SPARSE-DATA-FAILURE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-typed-sparse-data-failure) | Owned engineering invariant | CI on continuity and failure handling | [`QT-PROOF-401`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-401); [`QT-REM-401`](../assurance/guarantees/remediations/QT-REM-401.md) |
| [`QT-GUAR-BUDGETED-CLOSED-CANDLE-MARKET-STREAM`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-budgeted-closed-candle-market-stream) | Owned engineering invariant | CI on live-stream admission and reconnect logic | [`QT-PROOF-402`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-402); [`QT-REM-402`](../assurance/guarantees/remediations/QT-REM-402.md) |
| [`QT-GUAR-PROVIDER-CAPABILITY-AUTHORIZATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-provider-capability-authorization) | Owned engineering invariant | CI on provider operations | [`QT-PROOF-403`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-403); [`QT-REM-403`](../assurance/guarantees/remediations/QT-REM-403.md) |
| [`QT-GUAR-TYPED-CONSUMER-FACT-REQUIREMENTS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-typed-consumer-fact-requirements) | Owned engineering invariant | CI on consumer requirements | [`QT-PROOF-404`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-404); [`QT-REM-404`](../assurance/guarantees/remediations/QT-REM-404.md) |
| [`QT-GUAR-FENCED-IDEMPOTENT-SCHEDULED-COLLECTION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-fenced-idempotent-scheduled-collection) | Owned engineering invariant | DB on collection ownership or retry changes | [`QT-PROOF-405`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-405); [`QT-REM-405`](../assurance/guarantees/remediations/QT-REM-405.md) |
| [`QT-GUAR-DURABLE-VERIFIED-RAW-ARCHIVE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-durable-verified-raw-archive) | Owned engineering invariant | CI; backend-specific rehearsal when archive support changes | [`QT-PROOF-406`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-406); [`QT-REM-406`](../assurance/guarantees/remediations/QT-REM-406.md) |
| [`QT-GUAR-PIN-SAFE-MARKET-DATA-LIFECYCLE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-pin-safe-market-data-lifecycle) | Owned engineering invariant | CI with disposable destructive integration | [`QT-PROOF-407`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-407); [`QT-REM-407`](../assurance/guarantees/remediations/QT-REM-407.md) |
| [`QT-GUAR-INTERVAL-VALID-ORDER-BOOK-TRUTH`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-interval-valid-order-book-truth) | Owned engineering invariant | CI; DB when checkpoint persistence changes | [`QT-PROOF-408`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-408); [`QT-REM-408`](../assurance/guarantees/remediations/QT-REM-408.md) |
| [`QT-GUAR-CODE-OWNED-AUDITED-COLLECTOR-CONTROL`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-code-owned-audited-collector-control) | Owned engineering invariant | CI; DB for persisted audit semantics | [`QT-PROOF-409`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-409); [`QT-REM-409`](../assurance/guarantees/remediations/QT-REM-409.md) |
| [`QT-GUAR-PROVEN-ZERO-TRADE-COVERAGE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-proven-zero-trade-coverage) | Owned engineering invariant | DB on coverage or aggregation changes | [`QT-PROOF-410`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-410); [`QT-REM-410`](../assurance/guarantees/remediations/QT-REM-410.md) |
| [`QT-GUAR-INDICATOR-OUTPUT-CATALOG-AND-STRATEGY-READS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-indicator-output-catalog-and-strategy-reads) | Owned engineering invariant | CI on Indicator registration or Strategy reads | [`QT-PROOF-116`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-116); [`QT-REM-113`](../assurance/guarantees/remediations/QT-REM-113.md) |
| [`QT-GUAR-INDICATOR-OUTPUT-PRESENCE-AND-READINESS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-indicator-output-presence-and-readiness) | Owned engineering invariant | CI on Indicator lifecycle changes | [`QT-PROOF-117`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-117); [`QT-REM-114`](../assurance/guarantees/remediations/QT-REM-114.md) |
| [`QT-GUAR-INDICATOR-PUBLICATION-AUTHORITY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-indicator-publication-authority) | Owned engineering invariant | CI on engine publication changes | [`QT-PROOF-118`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-118); [`QT-REM-115`](../assurance/guarantees/remediations/QT-REM-115.md) |
| [`QT-GUAR-PROJECTION-DOES-NOT-CHANGE-INDICATOR-TRUTH`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-projection-does-not-change-indicator-truth) | Owned engineering invariant | CI on overlays and projections | [`QT-PROOF-119`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-119); [`QT-REM-116`](../assurance/guarantees/remediations/QT-REM-116.md) |
| [`QT-GUAR-INDICATOR-LIFECYCLE-EVIDENCE-SEPARATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-indicator-lifecycle-evidence-separation) | Owned engineering invariant | CI on lifecycle evidence or Strategy inputs | [`QT-PROOF-120`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-120); [`QT-REM-117`](../assurance/guarantees/remediations/QT-REM-117.md) |
| [`QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-strategy-decision-artifact-separation) | Core constituent — Promise 3 | CI on decision or execution handoff changes | [`QT-PROOF-121`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-121); [`QT-REM-118`](../assurance/guarantees/remediations/QT-REM-118.md) |
| [`QT-GUAR-STRATEGY-VARIANT-OUTPUT-FILTER-BOUNDARY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-strategy-variant-output-filter-boundary) | Owned engineering invariant | CI on variants and output filters | [`QT-PROOF-122`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-122); [`QT-REM-119`](../assurance/guarantees/remediations/QT-REM-119.md) |
| [`QT-GUAR-EFFECTIVE-STRATEGY-RESOLUTION-PARITY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-effective-strategy-resolution-parity) | Owned engineering invariant | CI on effective Strategy resolution | [`QT-PROOF-123`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-123); [`QT-REM-120`](../assurance/guarantees/remediations/QT-REM-120.md) |
| [`QT-GUAR-DETERMINISTIC-SEQUENTIAL-EXPERIMENT-PLAN`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-deterministic-sequential-experiment-plan) | Owned engineering invariant | CI on experiment planning or resume | [`QT-PROOF-124`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-124); [`QT-REM-121`](../assurance/guarantees/remediations/QT-REM-121.md) |
| [`QT-GUAR-MODE-AWARE-RUNTIME-COMPOSITION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-mode-aware-runtime-composition) | Owned engineering invariant | CI on composition-root changes | [`QT-PROOF-200`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-200); [`QT-REM-200`](../assurance/guarantees/remediations/QT-REM-200.md) |
| [`QT-GUAR-STRATEGY-INDEPENDENT-EXECUTION-ECONOMICS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-strategy-independent-execution-economics) | Owned engineering invariant | CI on execution context or economics | [`QT-PROOF-201`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-201); [`QT-REM-201`](../assurance/guarantees/remediations/QT-REM-201.md) |
| [`QT-GUAR-PINNED-EXECUTION-CONTEXTS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-pinned-execution-contexts) | Owned engineering invariant | CI on context resolution or persistence | [`QT-PROOF-202`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-202); [`QT-REM-202`](../assurance/guarantees/remediations/QT-REM-202.md) |
| [`QT-GUAR-EXPLICIT-EXECUTION-EXIT-POLICY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-explicit-execution-exit-policy) | Core constituent — Promise 4 | CI on execution-policy admission | [`QT-PROOF-203`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-203); [`QT-REM-203`](../assurance/guarantees/remediations/QT-REM-203.md) |
| [`QT-GUAR-POST-ONLY-SIGNAL-BAR-CAUSALITY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-post-only-signal-bar-causality) | Owned engineering invariant | CI on post-only order behavior | [`QT-PROOF-204`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-204); [`QT-REM-204`](../assurance/guarantees/remediations/QT-REM-204.md) |
| [`QT-GUAR-PROTECTIVE-EXIT-RESIDUAL-TERMINAL-INTEGRITY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-protective-exit-residual-terminal-integrity) | Core constituent — Promise 4 | CI on protective exits and terminal state | [`QT-PROOF-205`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-205); [`QT-REM-205`](../assurance/guarantees/remediations/QT-REM-205.md) |
| [`QT-GUAR-EXECUTION-MODE-PLAYBACK-SEPARATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-execution-mode-playback-separation) | Core constituent — Promise 3 | CI on execution or playback modes | [`QT-PROOF-206`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-206); [`QT-REM-206`](../assurance/guarantees/remediations/QT-REM-206.md) |
| [`QT-GUAR-RUNTIME-EXECUTION-OWNERSHIP-QUALITY-CEILING`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-runtime-execution-ownership-quality-ceiling) | Core constituent — Promise 3 | CI on execution models or quality labels | [`QT-PROOF-207`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-207); [`QT-REM-207`](../assurance/guarantees/remediations/QT-REM-207.md) |
| [`QT-GUAR-CANONICAL-ORDER-LIFECYCLE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-canonical-order-lifecycle) | Core constituent — Promise 4 | CI on order mutation or replay | [`QT-PROOF-208`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-208); [`QT-REM-208`](../assurance/guarantees/remediations/QT-REM-208.md) |
| [`QT-GUAR-FILL-SETTLEMENT-SINGLE-INGRESS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-fill-settlement-single-ingress) | Core constituent — Promise 4 | CI on fill settlement | [`QT-PROOF-209`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-209); [`QT-REM-209`](../assurance/guarantees/remediations/QT-REM-209.md) |
| [`QT-GUAR-WALLET-INITIALIZATION-AND-LEDGER-REPLAY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-wallet-initialization-and-ledger-replay) | Core constituent — Promise 4 | CI on wallet initialization or replay | [`QT-PROOF-210`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-210); [`QT-REM-210`](../assurance/guarantees/remediations/QT-REM-210.md) |
| [`QT-GUAR-SHARED-WALLET-MARKET-TIME-ARBITRATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-shared-wallet-market-time-arbitration) | Owned engineering invariant | CI on shared-wallet scheduling | [`QT-PROOF-211`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-211); [`QT-REM-211`](../assurance/guarantees/remediations/QT-REM-211.md) |
| [`QT-GUAR-CANONICAL-FILL-ACCOUNTING-RECONCILIATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-canonical-fill-accounting-reconciliation) | Core constituent — Promise 4 | CI on accounting or terminal reconciliation | [`QT-PROOF-212`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-212); [`QT-REM-212`](../assurance/guarantees/remediations/QT-REM-212.md) |
| [`QT-GUAR-REPLAY-CERTIFIED-EXECUTION-BOOK-TAPE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-replay-certified-execution-book-tape) | Owned engineering invariant | CI on book replay or execution-tape admission | [`QT-PROOF-213`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-213); [`QT-REM-213`](../assurance/guarantees/remediations/QT-REM-213.md) |
| [`QT-GUAR-PROJECTOR-ONLY-SELECTED-SYMBOL-READS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-projector-only-selected-symbol-reads) | Owned engineering invariant | CI on BotLens selected-symbol reads | [`QT-PROOF-214`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-214), [`QT-PROOF-215`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-215); [`QT-REM-214`](../assurance/guarantees/remediations/QT-REM-214.md) |
| [`QT-GUAR-BOTLENS-CURSOR-LINEAGE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-botlens-cursor-lineage) | Owned engineering invariant | CI on transport or frontend projection | [`QT-PROOF-216`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-216), [`QT-PROOF-217`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-217); [`QT-REM-215`](../assurance/guarantees/remediations/QT-REM-215.md) |
| [`QT-GUAR-BOTLENS-TYPED-READINESS`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-botlens-typed-readiness) | Owned engineering invariant | CI on readiness-state changes | [`QT-PROOF-218`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-218), [`QT-PROOF-219`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-219); [`QT-REM-216`](../assurance/guarantees/remediations/QT-REM-216.md) |
| [`QT-GUAR-BOTLENS-HOT-STATE-NOT-HISTORY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-botlens-hot-state-not-history) | Owned engineering invariant | CI on hot-state or history reads | [`QT-PROOF-220`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-220), [`QT-PROOF-221`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-221); [`QT-REM-217`](../assurance/guarantees/remediations/QT-REM-217.md) |
| [`QT-GUAR-OVERLAY-COMPLETENESS-ISOLATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-overlay-completeness-isolation) | Owned engineering invariant | CI on overlay completeness | [`QT-PROOF-222`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-222), [`QT-PROOF-223`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-223); [`QT-REM-218`](../assurance/guarantees/remediations/QT-REM-218.md) |
| [`QT-GUAR-BOT-RUN-CONTAINER-IDENTITY-SEPARATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-bot-run-container-identity-separation) | Owned engineering invariant | CI on control-plane or frontend identity | [`QT-PROOF-224`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-224), [`QT-PROOF-225`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-225); [`QT-REM-219`](../assurance/guarantees/remediations/QT-REM-219.md) |
| [`QT-GUAR-OPERATOR-CONSOLE-NONAUTHORITATIVE-SURFACE`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-operator-console-nonauthoritative-surface) | Owned engineering invariant | CI on operator-console capabilities | [`QT-PROOF-226`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-226); no remediation |
| [`QT-GUAR-TRADE-MARKER-CAUSAL-CANDLE-PROJECTION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-trade-marker-causal-candle-projection) | Owned engineering invariant | CI on frontend marker projection | [`QT-PROOF-227`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-227); [`QT-REM-220`](../assurance/guarantees/remediations/QT-REM-220.md) |
| [`QT-GUAR-PROVIDER-CREDENTIAL-REFERENCE-CONFINEMENT`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-provider-credential-reference-confinement) | Core constituent — Promise 5 | CI plus DB with synthetic credentials | [`QT-PROOF-300`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-300); [`QT-REM-300`](../assurance/guarantees/remediations/QT-REM-300.md) |
| [`QT-GUAR-V1-LOCAL-TRUST-BOUNDARY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-v1-local-trust-boundary) | Historical/deferred | Retain; reconsider when a supported deployment boundary exists | [`QT-PROOF-301`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-301); [`QT-REM-301`](../assurance/guarantees/remediations/QT-REM-301.md) |
| [`QT-GUAR-BOT-RUN-LEASE-OWNERSHIP`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-bot-run-lease-ownership) | Owned engineering invariant | CI; DB for persisted lease semantics | [`QT-PROOF-302`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-302); [`QT-REM-302`](../assurance/guarantees/remediations/QT-REM-302.md) |
| [`QT-GUAR-ASYNC-JOB-OWNERSHIP-FENCING`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-async-job-ownership-fencing) | Owned engineering invariant | DB on asynchronous ownership changes | [`QT-PROOF-303`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-303); [`QT-REM-303`](../assurance/guarantees/remediations/QT-REM-303.md) |
| [`QT-GUAR-ENUMERATED-AGENT-MUTATION-GATES`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-enumerated-agent-mutation-gates) | Owned engineering invariant | CI on controlled agent mutations | [`QT-PROOF-304`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-304); [`QT-REM-304`](../assurance/guarantees/remediations/QT-REM-304.md) |
| [`QT-GUAR-SOLE-POSTGRES-PERSISTENCE-AUTHORITY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-sole-postgres-persistence-authority) | Core constituent — Promise 6 | CI; DB on schema or migration changes | [`QT-PROOF-305`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-305); [`QT-REM-305`](../assurance/guarantees/remediations/QT-REM-305.md) |
| [`QT-GUAR-BLOCKING-API-WORK-OFFLOAD`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-blocking-api-work-offload) | Owned engineering invariant | CI on API and service changes | [`QT-PROOF-306`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-306); [`QT-REM-306`](../assurance/guarantees/remediations/QT-REM-306.md) |
| [`QT-GUAR-BOUNDED-NONCANONICAL-OBSERVABILITY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-bounded-noncanonical-observability) | Owned engineering invariant | CI on observability sinks | [`QT-PROOF-307`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-307); [`QT-REM-307`](../assurance/guarantees/remediations/QT-REM-307.md) |
| [`QT-GUAR-SINGLE-LOKI-INGRESS-PER-TOPOLOGY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-single-loki-ingress-per-topology) | Owned engineering invariant | CI on development or server log routing | [`QT-PROOF-308`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-308); [`QT-REM-308`](../assurance/guarantees/remediations/QT-REM-308.md) |
| [`QT-GUAR-DIAGNOSTICS-NOT-EXECUTION-TRUTH`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-diagnostics-not-execution-truth) | Owned engineering invariant | CI on diagnostics and watchdog paths | [`QT-PROOF-309`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-309); [`QT-REM-309`](../assurance/guarantees/remediations/QT-REM-309.md) |
| [`QT-GUAR-BOUNDED-TELEMETRY-CONTROL-DELIVERY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-bounded-telemetry-control-delivery) | Owned engineering invariant | CI on telemetry transport | [`QT-PROOF-310`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-310); [`QT-REM-310`](../assurance/guarantees/remediations/QT-REM-310.md) |
| [`QT-GUAR-ATTESTED-SINGLE-NODE-DEPLOYMENT`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-attested-single-node-deployment) | Historical/deferred | Retain; reconsider when an attested release is supported | [`QT-PROOF-311`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-311); [`QT-REM-311`](../assurance/guarantees/remediations/QT-REM-311.md) |
| [`QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-destructive-recovery-verification) | Core constituent — Promise 6 | CI for deletion guards; separately approved isolated rehearsal | [`QT-PROOF-013`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-013), [`QT-PROOF-014`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-014); [`QT-REM-009`](../assurance/guarantees/remediations/QT-REM-009.md) |
| [`QT-GUAR-SEMANTIC-OPERATIONAL-FINGERPRINT-SEPARATION`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-semantic-operational-fingerprint-separation) | Owned engineering invariant | CI on report fingerprinting | [`QT-PROOF-312`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-312); [`QT-REM-312`](../assurance/guarantees/remediations/QT-REM-312.md) |
| [`QT-GUAR-PR-VERIFICATION-TOPOLOGY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-pr-verification-topology) | Owned engineering invariant | CI; DB isolation check when workflow changes | [`QT-PROOF-313`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-313), [`QT-PROOF-314`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-314); [`QT-REM-313`](../assurance/guarantees/remediations/QT-REM-313.md) |
| [`QT-GUAR-ARCHITECTURE-DOC-INDEX-INTEGRITY`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-architecture-doc-index-integrity) | Owned engineering invariant | CI on architecture documentation | [`QT-PROOF-015`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-015); no remediation |
| [`QT-GUAR-CONTRACT-DRIVEN-GENERIC-SURFACES`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-contract-driven-generic-surfaces) | Owned engineering invariant | CI on providers and generic runtime surfaces | [`QT-PROOF-315`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-315); [`QT-REM-314`](../assurance/guarantees/remediations/QT-REM-314.md) |
| [`QT-GUAR-AGENT-WORKFLOW-BOUNDARIES`](../assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-agent-workflow-boundaries) | Owned engineering invariant | CI on agent-facing workflow surfaces | [`QT-PROOF-316`](../assurance/guarantees/GUARANTEES.md#proof-qt-proof-316); [`QT-REM-315`](../assurance/guarantees/remediations/QT-REM-315.md) |

## Change Rules

- Do not edit `GUARANTEES.md` by hand; it remains generated from the preserved
  registry and proof catalog.
- Do not infer activation from this crosswalk, a passing test, an accepted ADR,
  or completed engineering work.
- Do not create a replacement registry, schema, runner, or set of assurance
  levels for this maintenance model.
- When a listed property changes, update the owning contract, ADR, component
  documentation, and normal tests as appropriate. The historical record changes
  only through an explicitly reviewed audit update.
- Preserve exact historical paths even when their names use older review
  terminology.
