# Phase 2B Whole-System Guarantee Inventory

> **Status:** Non-normative Phase 2B review artifact. This inventory indexes
> classifications and proposed remediation work; it does not create product
> authority, adopt terminology, activate a guarantee, or report proof execution.

## Review frame

- Frozen audit baseline: `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`.
- Registry scope: `whole_system_classification`; Gate 2 approved the calibrated
  model for classification, and all 75 Phase 1 candidates are included. The
  classifications themselves remain review material rather than approved
  outcomes.
- The [registry](../../assurance/guarantees/registry.json), this inventory, and
  the [proof catalog](../../assurance/guarantees/proof-catalog.json) are
  non-normative assurance indexes. Proof definitions describe requirements,
  not executions or results.
- Accepted ADRs provide reviewed decision context but do not activate a
  guarantee on their own. Implementation, tests, and observed behavior are
  evidence only. Source-module contracts and `AGENTS.md` files remain
  nonactivating without separately reviewed authority decisions.
- Proposed, blocked, and deferred glossary entries remain unadopted. Gate 2
  approval does not move them into the normative glossary or read order.
- Every guarantee remains `unactivated`; no row below represents `PASS`,
  approval, or an attestation.
- The 68 linked remediation records are proposed review drafts. Each preserves
  the exact gap, proposed action, acceptance criteria, proof plan, and review
  boundary for its guarantee. None is approved, executed, or resolved.

## Exact counts

### Registry classification

| Axis | Value | Count |
| --- | --- | ---: |
| Guarantees | total | 75 |
| Candidate references | total | 75 |
| Disposition | `partially_enforced` | 65 |
| Disposition | `contradicted` | 3 |
| Disposition | `candidate` | 6 |
| Disposition | `implementation_property` | 1 |
| Conformance | `static_aligned` | 57 |
| Conformance | `partial` | 15 |
| Conformance | `contradicted` | 3 |
| Activation | `unactivated` | 75 |
| Activation | `active` | 0 |
| Enforcement maturity | `adequate` | 9 |
| Enforcement maturity | `defense_in_depth` | 35 |
| Enforcement maturity | `partial` | 31 |
| Proof maturity | `adequate` | 1 |
| Proof maturity | `partial` | 74 |
| Remediation status | `recorded` | 68 |
| Remediation status | `not_required` | 7 |

The 68 nonconforming remediation mappings below are exactly the 65
`partially_enforced` rows plus the 3 `contradicted` rows. This label does not
imply that the other seven rows are conforming, approved, or active.

### Proof catalog

| Measure | Value | Count |
| --- | --- | ---: |
| Proof definitions | total | 85 |
| Environment profiles | total | 4 |
| Proof lifecycle | `active` | 84 |
| Proof lifecycle | `proposed` | 1 |
| Proof kind | `automated_test` | 69 |
| Proof kind | `database_integration` | 7 |
| Proof kind | `static_validation` | 8 |
| Proof kind | `manual_procedure` | 1 |
| Guarantee coverage links | total | 88 |
| Guarantee coverage links | required for full attestation | 86 |
| Guarantee coverage links | optional/supporting | 2 |
| Coverage strength | `complete` | 2 |
| Coverage strength | `partial` | 84 |
| Coverage strength | `supporting` | 2 |

### Concrete remediation records

| Measure | Value | Count |
| --- | --- | ---: |
| Remediation files | total | 68 |
| Remediation lifecycle | `proposed` | 68 |
| Review required | `true` | 68 |
| Review status | `pending` | 68 |
| Review status | `approved` | 0 |
| Execution status | `executed` | 0 |
| Remediation lifecycle | `resolved` | 0 |

## Complete candidate-to-guarantee map

| Candidate | Guarantee | Disposition | Conformance | Enforcement maturity | Proof maturity | Remediation |
| --- | --- | --- | --- | --- | --- | --- |
| `QT-GC-001` | `QT-GUAR-KNOWN-AT-PREFIX-INVARIANCE` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-001`](../../assurance/guarantees/remediations/QT-REM-001.md) |
| `QT-GC-002` | `QT-GUAR-DERIVED-OUTPUT-TIMELINE` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-100`](../../assurance/guarantees/remediations/QT-REM-100.md) |
| `QT-GC-003` | `QT-GUAR-CANONICAL-FACT-APPEND-ONLY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-002`](../../assurance/guarantees/remediations/QT-REM-002.md) |
| `QT-GC-004` | `QT-GUAR-PROVIDER-FREE-CANONICAL-READS` | `partially_enforced` | `static_aligned` | `adequate` | `partial` | [`QT-REM-003`](../../assurance/guarantees/remediations/QT-REM-003.md) |
| `QT-GC-005` | `QT-GUAR-FROZEN-DATASET-REPLAY` | `candidate` | `static_aligned` | `defense_in_depth` | `partial` | — |
| `QT-GC-006` | `QT-GUAR-BACKTEST-FROZEN-BINDING` | `candidate` | `static_aligned` | `defense_in_depth` | `partial` | — |
| `QT-GC-007` | `QT-GUAR-DATASET-REALITY-CONSUMER-ADMISSION` | `candidate` | `static_aligned` | `adequate` | `partial` | — |
| `QT-GC-008` | `QT-GUAR-CHECK-PREVIEW-EVIDENCE-SEPARATION` | `candidate` | `static_aligned` | `defense_in_depth` | `adequate` | — |
| `QT-GC-009` | `QT-GUAR-CHECK-OBSERVATION-ADMISSION` | `contradicted` | `contradicted` | `partial` | `partial` | [`QT-REM-004`](../../assurance/guarantees/remediations/QT-REM-004.md) |
| `QT-GC-010` | `QT-GUAR-CHECK-AUTHORITY-CEILING` | `candidate` | `static_aligned` | `partial` | `partial` | — |
| `QT-GC-011` | `QT-GUAR-REPORTS-DURABLE-TRUTH-PROJECTION` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-005`](../../assurance/guarantees/remediations/QT-REM-005.md) |
| `QT-GC-012` | `QT-GUAR-RUN-LIFECYCLE-LEDGER-AUTHORITY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-006`](../../assurance/guarantees/remediations/QT-REM-006.md) |
| `QT-GC-013` | `QT-GUAR-RUNTIME-PERSISTENCE-FAILURE-BOUNDARY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-007`](../../assurance/guarantees/remediations/QT-REM-007.md) |
| `QT-GC-014` | `QT-GUAR-SHARED-APPLICATION-CONTRACT` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-008`](../../assurance/guarantees/remediations/QT-REM-008.md) |
| `QT-GC-015` | `QT-GUAR-EXTERNAL-ORDER-SUBMISSION-CLOSED` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-101`](../../assurance/guarantees/remediations/QT-REM-101.md) |
| `QT-GC-016` | `QT-GUAR-CANONICAL-MARKET-IDENTITY-ROUTING` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-400`](../../assurance/guarantees/remediations/QT-REM-400.md) |
| `QT-GC-017` | `QT-GUAR-TYPED-SPARSE-DATA-FAILURE` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-401`](../../assurance/guarantees/remediations/QT-REM-401.md) |
| `QT-GC-018` | `QT-GUAR-BUDGETED-CLOSED-CANDLE-MARKET-STREAM` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-402`](../../assurance/guarantees/remediations/QT-REM-402.md) |
| `QT-GC-019` | `QT-GUAR-PROVIDER-CAPABILITY-AUTHORIZATION` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-403`](../../assurance/guarantees/remediations/QT-REM-403.md) |
| `QT-GC-020` | `QT-GUAR-TYPED-CONSUMER-FACT-REQUIREMENTS` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-404`](../../assurance/guarantees/remediations/QT-REM-404.md) |
| `QT-GC-021` | `QT-GUAR-FENCED-IDEMPOTENT-SCHEDULED-COLLECTION` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-405`](../../assurance/guarantees/remediations/QT-REM-405.md) |
| `QT-GC-022` | `QT-GUAR-DURABLE-VERIFIED-RAW-ARCHIVE` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-406`](../../assurance/guarantees/remediations/QT-REM-406.md) |
| `QT-GC-023` | `QT-GUAR-PIN-SAFE-MARKET-DATA-LIFECYCLE` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-407`](../../assurance/guarantees/remediations/QT-REM-407.md) |
| `QT-GC-024` | `QT-GUAR-INTERVAL-VALID-ORDER-BOOK-TRUTH` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-408`](../../assurance/guarantees/remediations/QT-REM-408.md) |
| `QT-GC-025` | `QT-GUAR-CODE-OWNED-AUDITED-COLLECTOR-CONTROL` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-409`](../../assurance/guarantees/remediations/QT-REM-409.md) |
| `QT-GC-026` | `QT-GUAR-PROVEN-ZERO-TRADE-COVERAGE` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-410`](../../assurance/guarantees/remediations/QT-REM-410.md) |
| `QT-GC-027` | `QT-GUAR-INDICATOR-OUTPUT-CATALOG-AND-STRATEGY-READS` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-113`](../../assurance/guarantees/remediations/QT-REM-113.md) |
| `QT-GC-028` | `QT-GUAR-INDICATOR-OUTPUT-PRESENCE-AND-READINESS` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-114`](../../assurance/guarantees/remediations/QT-REM-114.md) |
| `QT-GC-029` | `QT-GUAR-INDICATOR-PUBLICATION-AUTHORITY` | `partially_enforced` | `static_aligned` | `adequate` | `partial` | [`QT-REM-115`](../../assurance/guarantees/remediations/QT-REM-115.md) |
| `QT-GC-030` | `QT-GUAR-PROJECTION-DOES-NOT-CHANGE-INDICATOR-TRUTH` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-116`](../../assurance/guarantees/remediations/QT-REM-116.md) |
| `QT-GC-031` | `QT-GUAR-INDICATOR-LIFECYCLE-EVIDENCE-SEPARATION` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-117`](../../assurance/guarantees/remediations/QT-REM-117.md) |
| `QT-GC-032` | `QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-118`](../../assurance/guarantees/remediations/QT-REM-118.md) |
| `QT-GC-033` | `QT-GUAR-STRATEGY-VARIANT-OUTPUT-FILTER-BOUNDARY` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-119`](../../assurance/guarantees/remediations/QT-REM-119.md) |
| `QT-GC-034` | `QT-GUAR-EFFECTIVE-STRATEGY-RESOLUTION-PARITY` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-120`](../../assurance/guarantees/remediations/QT-REM-120.md) |
| `QT-GC-035` | `QT-GUAR-DETERMINISTIC-SEQUENTIAL-EXPERIMENT-PLAN` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-121`](../../assurance/guarantees/remediations/QT-REM-121.md) |
| `QT-GC-036` | `QT-GUAR-MODE-AWARE-RUNTIME-COMPOSITION` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-200`](../../assurance/guarantees/remediations/QT-REM-200.md) |
| `QT-GC-037` | `QT-GUAR-STRATEGY-INDEPENDENT-EXECUTION-ECONOMICS` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-201`](../../assurance/guarantees/remediations/QT-REM-201.md) |
| `QT-GC-038` | `QT-GUAR-PINNED-EXECUTION-CONTEXTS` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-202`](../../assurance/guarantees/remediations/QT-REM-202.md) |
| `QT-GC-039` | `QT-GUAR-EXPLICIT-EXECUTION-EXIT-POLICY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-203`](../../assurance/guarantees/remediations/QT-REM-203.md) |
| `QT-GC-040` | `QT-GUAR-POST-ONLY-SIGNAL-BAR-CAUSALITY` | `partially_enforced` | `static_aligned` | `adequate` | `partial` | [`QT-REM-204`](../../assurance/guarantees/remediations/QT-REM-204.md) |
| `QT-GC-041` | `QT-GUAR-PROTECTIVE-EXIT-RESIDUAL-TERMINAL-INTEGRITY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-205`](../../assurance/guarantees/remediations/QT-REM-205.md) |
| `QT-GC-042` | `QT-GUAR-EXECUTION-MODE-PLAYBACK-SEPARATION` | `partially_enforced` | `static_aligned` | `adequate` | `partial` | [`QT-REM-206`](../../assurance/guarantees/remediations/QT-REM-206.md) |
| `QT-GC-043` | `QT-GUAR-RUNTIME-EXECUTION-OWNERSHIP-QUALITY-CEILING` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-207`](../../assurance/guarantees/remediations/QT-REM-207.md) |
| `QT-GC-044` | `QT-GUAR-CANONICAL-ORDER-LIFECYCLE` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-208`](../../assurance/guarantees/remediations/QT-REM-208.md) |
| `QT-GC-045` | `QT-GUAR-FILL-SETTLEMENT-SINGLE-INGRESS` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-209`](../../assurance/guarantees/remediations/QT-REM-209.md) |
| `QT-GC-046` | `QT-GUAR-WALLET-INITIALIZATION-AND-LEDGER-REPLAY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-210`](../../assurance/guarantees/remediations/QT-REM-210.md) |
| `QT-GC-047` | `QT-GUAR-SHARED-WALLET-MARKET-TIME-ARBITRATION` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-211`](../../assurance/guarantees/remediations/QT-REM-211.md) |
| `QT-GC-048` | `QT-GUAR-CANONICAL-FILL-ACCOUNTING-RECONCILIATION` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-212`](../../assurance/guarantees/remediations/QT-REM-212.md) |
| `QT-GC-049` | `QT-GUAR-REPLAY-CERTIFIED-EXECUTION-BOOK-TAPE` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-213`](../../assurance/guarantees/remediations/QT-REM-213.md) |
| `QT-GC-050` | `QT-GUAR-PROJECTOR-ONLY-SELECTED-SYMBOL-READS` | `partially_enforced` | `static_aligned` | `adequate` | `partial` | [`QT-REM-214`](../../assurance/guarantees/remediations/QT-REM-214.md) |
| `QT-GC-051` | `QT-GUAR-BOTLENS-CURSOR-LINEAGE` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-215`](../../assurance/guarantees/remediations/QT-REM-215.md) |
| `QT-GC-052` | `QT-GUAR-BOTLENS-TYPED-READINESS` | `partially_enforced` | `static_aligned` | `adequate` | `partial` | [`QT-REM-216`](../../assurance/guarantees/remediations/QT-REM-216.md) |
| `QT-GC-053` | `QT-GUAR-BOTLENS-HOT-STATE-NOT-HISTORY` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-217`](../../assurance/guarantees/remediations/QT-REM-217.md) |
| `QT-GC-054` | `QT-GUAR-OVERLAY-COMPLETENESS-ISOLATION` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-218`](../../assurance/guarantees/remediations/QT-REM-218.md) |
| `QT-GC-055` | `QT-GUAR-BOT-RUN-CONTAINER-IDENTITY-SEPARATION` | `partially_enforced` | `static_aligned` | `adequate` | `partial` | [`QT-REM-219`](../../assurance/guarantees/remediations/QT-REM-219.md) |
| `QT-GC-056` | `QT-GUAR-OPERATOR-CONSOLE-NONAUTHORITATIVE-SURFACE` | `candidate` | `partial` | `partial` | `partial` | — |
| `QT-GC-057` | `QT-GUAR-TRADE-MARKER-CAUSAL-CANDLE-PROJECTION` | `partially_enforced` | `static_aligned` | `adequate` | `partial` | [`QT-REM-220`](../../assurance/guarantees/remediations/QT-REM-220.md) |
| `QT-GC-058` | `QT-GUAR-PROVIDER-CREDENTIAL-REFERENCE-CONFINEMENT` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-300`](../../assurance/guarantees/remediations/QT-REM-300.md) |
| `QT-GC-059` | `QT-GUAR-V1-LOCAL-TRUST-BOUNDARY` | `partially_enforced` | `static_aligned` | `partial` | `partial` | [`QT-REM-301`](../../assurance/guarantees/remediations/QT-REM-301.md) |
| `QT-GC-060` | `QT-GUAR-BOT-RUN-LEASE-OWNERSHIP` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-302`](../../assurance/guarantees/remediations/QT-REM-302.md) |
| `QT-GC-061` | `QT-GUAR-ASYNC-JOB-OWNERSHIP-FENCING` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-303`](../../assurance/guarantees/remediations/QT-REM-303.md) |
| `QT-GC-062` | `QT-GUAR-ENUMERATED-AGENT-MUTATION-GATES` | `contradicted` | `contradicted` | `partial` | `partial` | [`QT-REM-304`](../../assurance/guarantees/remediations/QT-REM-304.md) |
| `QT-GC-063` | `QT-GUAR-SOLE-POSTGRES-PERSISTENCE-AUTHORITY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-305`](../../assurance/guarantees/remediations/QT-REM-305.md) |
| `QT-GC-064` | `QT-GUAR-BLOCKING-API-WORK-OFFLOAD` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-306`](../../assurance/guarantees/remediations/QT-REM-306.md) |
| `QT-GC-065` | `QT-GUAR-BOUNDED-NONCANONICAL-OBSERVABILITY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-307`](../../assurance/guarantees/remediations/QT-REM-307.md) |
| `QT-GC-066` | `QT-GUAR-SINGLE-LOKI-INGRESS-PER-TOPOLOGY` | `contradicted` | `contradicted` | `partial` | `partial` | [`QT-REM-308`](../../assurance/guarantees/remediations/QT-REM-308.md) |
| `QT-GC-067` | `QT-GUAR-DIAGNOSTICS-NOT-EXECUTION-TRUTH` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-309`](../../assurance/guarantees/remediations/QT-REM-309.md) |
| `QT-GC-068` | `QT-GUAR-BOUNDED-TELEMETRY-CONTROL-DELIVERY` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-310`](../../assurance/guarantees/remediations/QT-REM-310.md) |
| `QT-GC-069` | `QT-GUAR-ATTESTED-SINGLE-NODE-DEPLOYMENT` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-311`](../../assurance/guarantees/remediations/QT-REM-311.md) |
| `QT-GC-070` | `QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-009`](../../assurance/guarantees/remediations/QT-REM-009.md) |
| `QT-GC-071` | `QT-GUAR-SEMANTIC-OPERATIONAL-FINGERPRINT-SEPARATION` | `partially_enforced` | `static_aligned` | `defense_in_depth` | `partial` | [`QT-REM-312`](../../assurance/guarantees/remediations/QT-REM-312.md) |
| `QT-GC-072` | `QT-GUAR-PR-VERIFICATION-TOPOLOGY` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-313`](../../assurance/guarantees/remediations/QT-REM-313.md) |
| `QT-GC-073` | `QT-GUAR-ARCHITECTURE-DOC-INDEX-INTEGRITY` | `implementation_property` | `partial` | `partial` | `partial` | — |
| `QT-GC-074` | `QT-GUAR-CONTRACT-DRIVEN-GENERIC-SURFACES` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-314`](../../assurance/guarantees/remediations/QT-REM-314.md) |
| `QT-GC-075` | `QT-GUAR-AGENT-WORKFLOW-BOUNDARIES` | `partially_enforced` | `partial` | `partial` | `partial` | [`QT-REM-315`](../../assurance/guarantees/remediations/QT-REM-315.md) |

## Complete remediation review map

| Remediation | Guarantee | Owner | Required reviewers | Lifecycle | Review status |
| --- | --- | --- | --- | --- | --- |
| [`QT-REM-001`](../../assurance/guarantees/remediations/QT-REM-001.md) | `QT-GUAR-KNOWN-AT-PREFIX-INVARIANCE` | `execution-runtime` | `execution-runtime-owner`, `market-data-owner`, `platform-contract-reviewer` | `proposed` | `pending` |
| [`QT-REM-002`](../../assurance/guarantees/remediations/QT-REM-002.md) | `QT-GUAR-CANONICAL-FACT-APPEND-ONLY` | `database` | `database-owner`, `market-data-owner`, `persistence-owner` | `proposed` | `pending` |
| [`QT-REM-003`](../../assurance/guarantees/remediations/QT-REM-003.md) | `QT-GUAR-PROVIDER-FREE-CANONICAL-READS` | `market-data` | `market-data-owner`, `provider-owner` | `proposed` | `pending` |
| [`QT-REM-004`](../../assurance/guarantees/remediations/QT-REM-004.md) | `QT-GUAR-CHECK-OBSERVATION-ADMISSION` | `research-memory` | `normative-contract-reviewer`, `research-memory-owner`, `research-orchestration-owner` | `proposed` | `pending` |
| [`QT-REM-005`](../../assurance/guarantees/remediations/QT-REM-005.md) | `QT-GUAR-REPORTS-DURABLE-TRUTH-PROJECTION` | `reporting` | `execution-persistence-owner`, `reporting-owner` | `proposed` | `pending` |
| [`QT-REM-006`](../../assurance/guarantees/remediations/QT-REM-006.md) | `QT-GUAR-RUN-LIFECYCLE-LEDGER-AUTHORITY` | `execution-persistence` | `database-owner`, `execution-runtime-owner` | `proposed` | `pending` |
| [`QT-REM-007`](../../assurance/guarantees/remediations/QT-REM-007.md) | `QT-GUAR-RUNTIME-PERSISTENCE-FAILURE-BOUNDARY` | `execution-runtime` | `botlens-projections-owner`, `execution-runtime-owner` | `proposed` | `pending` |
| [`QT-REM-008`](../../assurance/guarantees/remediations/QT-REM-008.md) | `QT-GUAR-SHARED-APPLICATION-CONTRACT` | `application-interfaces` | `api-owner`, `cli-owner`, `mcp-owner`, `platform-contract-reviewer` | `proposed` | `pending` |
| [`QT-REM-009`](../../assurance/guarantees/remediations/QT-REM-009.md) | `QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION` | `recovery` | `data-retention-owner`, `database-operations-owner`, `recovery-owner` | `proposed` | `pending` |
| [`QT-REM-100`](../../assurance/guarantees/remediations/QT-REM-100.md) | `QT-GUAR-DERIVED-OUTPUT-TIMELINE` | `execution-runtime` | `execution-runtime-owner`, `indicator-runtime-owner`, `platform-contract-reviewer`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-101`](../../assurance/guarantees/remediations/QT-REM-101.md) | `QT-GUAR-EXTERNAL-ORDER-SUBMISSION-CLOSED` | `execution-runtime` | `execution-runtime-owner`, `security-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-113`](../../assurance/guarantees/remediations/QT-REM-113.md) | `QT-GUAR-INDICATOR-OUTPUT-CATALOG-AND-STRATEGY-READS` | `indicator-runtime` | `decision-layer-owner`, `indicator-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-114`](../../assurance/guarantees/remediations/QT-REM-114.md) | `QT-GUAR-INDICATOR-OUTPUT-PRESENCE-AND-READINESS` | `indicator-runtime` | `indicator-runtime-owner`, `platform-contract-reviewer`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-115`](../../assurance/guarantees/remediations/QT-REM-115.md) | `QT-GUAR-INDICATOR-PUBLICATION-AUTHORITY` | `indicator-runtime` | `indicator-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-116`](../../assurance/guarantees/remediations/QT-REM-116.md) | `QT-GUAR-PROJECTION-DOES-NOT-CHANGE-INDICATOR-TRUTH` | `indicator-runtime` | `botlens-projection-owner`, `indicator-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-117`](../../assurance/guarantees/remediations/QT-REM-117.md) | `QT-GUAR-INDICATOR-LIFECYCLE-EVIDENCE-SEPARATION` | `indicator-runtime` | `decision-layer-owner`, `indicator-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-118`](../../assurance/guarantees/remediations/QT-REM-118.md) | `QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION` | `decision-layer` | `decision-layer-owner`, `execution-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-119`](../../assurance/guarantees/remediations/QT-REM-119.md) | `QT-GUAR-STRATEGY-VARIANT-OUTPUT-FILTER-BOUNDARY` | `decision-layer` | `decision-layer-owner`, `execution-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-120`](../../assurance/guarantees/remediations/QT-REM-120.md) | `QT-GUAR-EFFECTIVE-STRATEGY-RESOLUTION-PARITY` | `decision-layer` | `decision-layer-owner`, `reporting-owner`, `strategy-service-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-121`](../../assurance/guarantees/remediations/QT-REM-121.md) | `QT-GUAR-DETERMINISTIC-SEQUENTIAL-EXPERIMENT-PLAN` | `experiment-orchestration` | `experiment-orchestration-owner`, `research-orchestration-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-200`](../../assurance/guarantees/remediations/QT-REM-200.md) | `QT-GUAR-MODE-AWARE-RUNTIME-COMPOSITION` | `execution-runtime` | `architecture-owner`, `execution-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-201`](../../assurance/guarantees/remediations/QT-REM-201.md) | `QT-GUAR-STRATEGY-INDEPENDENT-EXECUTION-ECONOMICS` | `execution-runtime` | `execution-runtime-owner`, `instruments-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-202`](../../assurance/guarantees/remediations/QT-REM-202.md) | `QT-GUAR-PINNED-EXECUTION-CONTEXTS` | `execution-runtime` | `execution-runtime-owner`, `security-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-203`](../../assurance/guarantees/remediations/QT-REM-203.md) | `QT-GUAR-EXPLICIT-EXECUTION-EXIT-POLICY` | `execution-runtime` | `decision-layer-owner`, `execution-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-204`](../../assurance/guarantees/remediations/QT-REM-204.md) | `QT-GUAR-POST-ONLY-SIGNAL-BAR-CAUSALITY` | `execution-runtime` | `execution-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-205`](../../assurance/guarantees/remediations/QT-REM-205.md) | `QT-GUAR-PROTECTIVE-EXIT-RESIDUAL-TERMINAL-INTEGRITY` | `execution-runtime` | `execution-runtime-owner`, `positions-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-206`](../../assurance/guarantees/remediations/QT-REM-206.md) | `QT-GUAR-EXECUTION-MODE-PLAYBACK-SEPARATION` | `execution-runtime` | `execution-runtime-owner`, `playback-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-207`](../../assurance/guarantees/remediations/QT-REM-207.md) | `QT-GUAR-RUNTIME-EXECUTION-OWNERSHIP-QUALITY-CEILING` | `execution-runtime` | `accounting-owner`, `execution-model-owner`, `execution-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-208`](../../assurance/guarantees/remediations/QT-REM-208.md) | `QT-GUAR-CANONICAL-ORDER-LIFECYCLE` | `execution-runtime` | `execution-runtime-owner`, `persistence-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-209`](../../assurance/guarantees/remediations/QT-REM-209.md) | `QT-GUAR-FILL-SETTLEMENT-SINGLE-INGRESS` | `accounting` | `accounting-owner`, `execution-runtime-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-210`](../../assurance/guarantees/remediations/QT-REM-210.md) | `QT-GUAR-WALLET-INITIALIZATION-AND-LEDGER-REPLAY` | `execution-runtime` | `execution-runtime-owner`, `persistence-owner`, `testing-owner`, `wallet-owner` | `proposed` | `pending` |
| [`QT-REM-211`](../../assurance/guarantees/remediations/QT-REM-211.md) | `QT-GUAR-SHARED-WALLET-MARKET-TIME-ARBITRATION` | `wallet` | `execution-runtime-owner`, `testing-owner`, `wallet-owner` | `proposed` | `pending` |
| [`QT-REM-212`](../../assurance/guarantees/remediations/QT-REM-212.md) | `QT-GUAR-CANONICAL-FILL-ACCOUNTING-RECONCILIATION` | `accounting` | `accounting-owner`, `execution-runtime-owner`, `reporting-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-213`](../../assurance/guarantees/remediations/QT-REM-213.md) | `QT-GUAR-REPLAY-CERTIFIED-EXECUTION-BOOK-TAPE` | `market-structure` | `data-owner`, `execution-runtime-owner`, `market-structure-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-214`](../../assurance/guarantees/remediations/QT-REM-214.md) | `QT-GUAR-PROJECTOR-ONLY-SELECTED-SYMBOL-READS` | `botlens-projections` | `botlens-projections-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-215`](../../assurance/guarantees/remediations/QT-REM-215.md) | `QT-GUAR-BOTLENS-CURSOR-LINEAGE` | `botlens-transport` | `botlens-transport-owner`, `frontend-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-216`](../../assurance/guarantees/remediations/QT-REM-216.md) | `QT-GUAR-BOTLENS-TYPED-READINESS` | `botlens-projections` | `botlens-projections-owner`, `frontend-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-217`](../../assurance/guarantees/remediations/QT-REM-217.md) | `QT-GUAR-BOTLENS-HOT-STATE-NOT-HISTORY` | `botlens-projections` | `botlens-projections-owner`, `frontend-owner`, `persistence-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-218`](../../assurance/guarantees/remediations/QT-REM-218.md) | `QT-GUAR-OVERLAY-COMPLETENESS-ISOLATION` | `botlens-overlays` | `botlens-projections-owner`, `frontend-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-219`](../../assurance/guarantees/remediations/QT-REM-219.md) | `QT-GUAR-BOT-RUN-CONTAINER-IDENTITY-SEPARATION` | `bot-control-plane` | `bot-control-plane-owner`, `botlens-projections-owner`, `frontend-owner`, `persistence-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-220`](../../assurance/guarantees/remediations/QT-REM-220.md) | `QT-GUAR-TRADE-MARKER-CAUSAL-CANDLE-PROJECTION` | `frontend` | `botlens-projections-owner`, `frontend-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-300`](../../assurance/guarantees/remediations/QT-REM-300.md) | `QT-GUAR-PROVIDER-CREDENTIAL-REFERENCE-CONFINEMENT` | `provider-security` | `data-provider-owner`, `security-owner` | `proposed` | `pending` |
| [`QT-REM-301`](../../assurance/guarantees/remediations/QT-REM-301.md) | `QT-GUAR-V1-LOCAL-TRUST-BOUNDARY` | `deployment-security` | `deployment-owner`, `security-owner` | `proposed` | `pending` |
| [`QT-REM-302`](../../assurance/guarantees/remediations/QT-REM-302.md) | `QT-GUAR-BOT-RUN-LEASE-OWNERSHIP` | `execution-persistence` | `execution-runtime-owner`, `persistence-owner`, `security-owner` | `proposed` | `pending` |
| [`QT-REM-303`](../../assurance/guarantees/remediations/QT-REM-303.md) | `QT-GUAR-ASYNC-JOB-OWNERSHIP-FENCING` | `async-jobs` | `persistence-owner`, `research-orchestration-owner` | `proposed` | `pending` |
| [`QT-REM-304`](../../assurance/guarantees/remediations/QT-REM-304.md) | `QT-GUAR-ENUMERATED-AGENT-MUTATION-GATES` | `application-interfaces` | `application-interface-owner`, `normative-contract-reviewer`, `research-governance-owner` | `proposed` | `pending` |
| [`QT-REM-305`](../../assurance/guarantees/remediations/QT-REM-305.md) | `QT-GUAR-SOLE-POSTGRES-PERSISTENCE-AUTHORITY` | `persistence` | `database-operations-owner`, `persistence-owner` | `proposed` | `pending` |
| [`QT-REM-306`](../../assurance/guarantees/remediations/QT-REM-306.md) | `QT-GUAR-BLOCKING-API-WORK-OFFLOAD` | `api-runtime` | `api-owner`, `runtime-services-owner` | `proposed` | `pending` |
| [`QT-REM-307`](../../assurance/guarantees/remediations/QT-REM-307.md) | `QT-GUAR-BOUNDED-NONCANONICAL-OBSERVABILITY` | `observability` | `observability-owner`, `persistence-owner` | `proposed` | `pending` |
| [`QT-REM-308`](../../assurance/guarantees/remediations/QT-REM-308.md) | `QT-GUAR-SINGLE-LOKI-INGRESS-PER-TOPOLOGY` | `observability` | `deployment-owner`, `observability-owner` | `proposed` | `pending` |
| [`QT-REM-309`](../../assurance/guarantees/remediations/QT-REM-309.md) | `QT-GUAR-DIAGNOSTICS-NOT-EXECUTION-TRUTH` | `runtime-observability` | `execution-runtime-owner`, `observability-owner` | `proposed` | `pending` |
| [`QT-REM-310`](../../assurance/guarantees/remediations/QT-REM-310.md) | `QT-GUAR-BOUNDED-TELEMETRY-CONTROL-DELIVERY` | `runtime-telemetry` | `botlens-owner`, `execution-runtime-owner` | `proposed` | `pending` |
| [`QT-REM-311`](../../assurance/guarantees/remediations/QT-REM-311.md) | `QT-GUAR-ATTESTED-SINGLE-NODE-DEPLOYMENT` | `deployment` | `deployment-owner`, `operations-owner`, `security-owner` | `proposed` | `pending` |
| [`QT-REM-312`](../../assurance/guarantees/remediations/QT-REM-312.md) | `QT-GUAR-SEMANTIC-OPERATIONAL-FINGERPRINT-SEPARATION` | `reporting` | `reporting-owner`, `runtime-owner` | `proposed` | `pending` |
| [`QT-REM-313`](../../assurance/guarantees/remediations/QT-REM-313.md) | `QT-GUAR-PR-VERIFICATION-TOPOLOGY` | `testing` | `ci-owner`, `database-test-owner`, `testing-owner` | `proposed` | `pending` |
| [`QT-REM-314`](../../assurance/guarantees/remediations/QT-REM-314.md) | `QT-GUAR-CONTRACT-DRIVEN-GENERIC-SURFACES` | `platform-engineering` | `platform-contract-owner`, `provider-owner`, `runtime-owner` | `proposed` | `pending` |
| [`QT-REM-315`](../../assurance/guarantees/remediations/QT-REM-315.md) | `QT-GUAR-AGENT-WORKFLOW-BOUNDARIES` | `repository-governance` | `agent-governance-owner`, `application-interface-owner`, `architecture-documentation-owner` | `proposed` | `pending` |
| [`QT-REM-400`](../../assurance/guarantees/remediations/QT-REM-400.md) | `QT-GUAR-CANONICAL-MARKET-IDENTITY-ROUTING` | `market-identity` | `data-owner`, `decision-layer-owner`, `runtime-owner` | `proposed` | `pending` |
| [`QT-REM-401`](../../assurance/guarantees/remediations/QT-REM-401.md) | `QT-GUAR-TYPED-SPARSE-DATA-FAILURE` | `data-continuity` | `data-owner`, `reporting-owner`, `runtime-owner` | `proposed` | `pending` |
| [`QT-REM-402`](../../assurance/guarantees/remediations/QT-REM-402.md) | `QT-GUAR-BUDGETED-CLOSED-CANDLE-MARKET-STREAM` | `live-market-data` | `execution-runtime-owner`, `market-data-owner`, `provider-owner` | `proposed` | `pending` |
| [`QT-REM-403`](../../assurance/guarantees/remediations/QT-REM-403.md) | `QT-GUAR-PROVIDER-CAPABILITY-AUTHORIZATION` | `provider-boundary` | `provider-owner`, `security-owner` | `proposed` | `pending` |
| [`QT-REM-404`](../../assurance/guarantees/remediations/QT-REM-404.md) | `QT-GUAR-TYPED-CONSUMER-FACT-REQUIREMENTS` | `market-data-contracts` | `consumer-contract-owner`, `data-owner`, `instrument-identity-owner` | `proposed` | `pending` |
| [`QT-REM-405`](../../assurance/guarantees/remediations/QT-REM-405.md) | `QT-GUAR-FENCED-IDEMPOTENT-SCHEDULED-COLLECTION` | `collection-runtime` | `collection-owner`, `persistence-owner` | `proposed` | `pending` |
| [`QT-REM-406`](../../assurance/guarantees/remediations/QT-REM-406.md) | `QT-GUAR-DURABLE-VERIFIED-RAW-ARCHIVE` | `raw-archive` | `data-owner`, `raw-archive-owner`, `storage-owner` | `proposed` | `pending` |
| [`QT-REM-407`](../../assurance/guarantees/remediations/QT-REM-407.md) | `QT-GUAR-PIN-SAFE-MARKET-DATA-LIFECYCLE` | `market-storage-lifecycle` | `data-owner`, `operations-owner`, `storage-owner` | `proposed` | `pending` |
| [`QT-REM-408`](../../assurance/guarantees/remediations/QT-REM-408.md) | `QT-GUAR-INTERVAL-VALID-ORDER-BOOK-TRUTH` | `market-structure` | `data-owner`, `market-structure-owner`, `persistence-owner` | `proposed` | `pending` |
| [`QT-REM-409`](../../assurance/guarantees/remediations/QT-REM-409.md) | `QT-GUAR-CODE-OWNED-AUDITED-COLLECTOR-CONTROL` | `collector-control-plane` | `collection-owner`, `operations-owner`, `security-owner` | `proposed` | `pending` |
| [`QT-REM-410`](../../assurance/guarantees/remediations/QT-REM-410.md) | `QT-GUAR-PROVEN-ZERO-TRADE-COVERAGE` | `market-structure-coverage` | `data-owner`, `documentation-assurance-owner`, `market-structure-owner`, `persistence-owner` | `proposed` | `pending` |

## Integrity checks

- Candidate coverage is exactly `QT-GC-001` through `QT-GC-075`: 75 distinct
  candidate IDs, each mapped once to one distinct guarantee row.
- The nonconforming set contains exactly 68 mappings: every
  `partially_enforced` or `contradicted` guarantee maps once to one distinct
  concrete remediation record, and no other disposition carries one.
- The registry remediation IDs, remediation filenames, frontmatter IDs, and
  frontmatter guarantee IDs agree one-to-one.
- Every relative remediation link in both tables resolves to the referenced
  repository file.
- All 68 `required_reviewers` lists are nonempty, sorted, unique, and reproduced
  verbatim from remediation frontmatter.
- Every remediation contains nonempty `Gap`, `Action`, `Acceptance criteria`,
  `Proof plan`, and `Review boundary` sections. These are proposals for later
  owner/reviewer action, not evidence that work occurred.
