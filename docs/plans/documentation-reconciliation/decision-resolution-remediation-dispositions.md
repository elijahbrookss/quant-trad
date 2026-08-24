# Decision Resolution — All 68 Remediation Dispositions

## Purpose And Boundary

This is a proposed planning disposition for every concrete `QT-REM-*` record.
It does not approve, edit, execute, close, or replace any remediation. The
source records remain `lifecycle: proposed` and `review_status: pending` under
`docs/assurance/guarantees/remediations/`.

Structural accounting is clean: 68 files, 68 unique remediation IDs, 68 unique
guarantee mappings, and no duplicate mapping.

## Disposition Vocabulary

| Proposed disposition | Count | Meaning |
| --- | ---: | --- |
| `retain_for_phase3` | 19 | Concrete product/architecture work is worth planning if Phase 3 is later authorized |
| `split_resolution_from_execution` | 20 | Ratify or align the controlling meaning first; keep implementation/proof as a separate later action |
| `defer_for_proof_environment` | 14 | The next honest closure depends on an admitted environment or runner |
| `defer_for_activation_priority` | 11 | Representative defenses are sufficient for now; fund the exhaustive matrix only if the guarantee becomes an activation priority |
| `defer_for_owner_approval` | 4 | No action is safe until owners approve the controlling decision or derived authority reading |
| close now | 0 | Every record still contains unexecuted acceptance or proof work |
| **Total** | **68** | Complete remediation denominator |

`defer_for_owner_approval` does not mean all four records contain an irreducible
semantic choice. DRR-01 and DRR-14 propose derivable readings for
`QT-REM-304` and `QT-REM-308`, but those readings still require owner
ratification before later edits.

Proof shorthand:

- `PDR-01` — exact-commit assurance, identity, snapshot, and locator posture;
- `PDR-02` — frontend Node result semantics and environment;
- `PDR-03` — isolated database plus side-effect-free collection;
- `PDR-04` — isolated recovery rehearsal; and
- `PDR-05` — closed/open denominator rule for universal or absence claims.

## Complete Proposed Disposition

| Remediation | Guarantee | Proposed disposition | Controlling resolution / proof | Why and likely later slice |
| --- | --- | --- | --- | --- |
| `QT-REM-001` | `QT-GUAR-KNOWN-AT-PREFIX-INVARIANCE` | `retain_for_phase3` | DRR-03; PDR-03/PDR-05 | Build the causal source/output inventory and prefix/suffix matrix in one market-data/runtime slice. |
| `QT-REM-002` | `QT-GUAR-CANONICAL-FACT-APPEND-ONLY` | `defer_for_proof_environment` | DRR-04; PDR-03 | Semantics align; schema ownership and real concurrent/delete-rejection proof require isolated PostgreSQL. |
| `QT-REM-003` | `QT-GUAR-PROVIDER-FREE-CANONICAL-READS` | `defer_for_activation_priority` | PDR-03/PDR-05 | Represented enforcement is adequate; build a repository-wide read inventory only if activation is prioritized. |
| `QT-REM-004` | `QT-GUAR-CHECK-OBSERVATION-ADMISSION` | `defer_for_owner_approval` | DRR-07 | Check/Observation authority must be chosen before implementation or proof work. |
| `QT-REM-005` | `QT-GUAR-REPORTS-DURABLE-TRUTH-PROJECTION` | `retain_for_phase3` | DRR-03; PDR-03/PDR-05 | Inventory report builders and make the durable-truth/projection boundary explicit. |
| `QT-REM-006` | `QT-GUAR-RUN-LIFECYCLE-LEDGER-AUTHORITY` | `defer_for_proof_environment` | DRR-04; expanded PDR-03 | Same-transaction, rollback, and concurrency claims require isolated database proof not yet cataloged for this record. |
| `QT-REM-007` | `QT-GUAR-RUNTIME-PERSISTENCE-FAILURE-BOUNDARY` | `retain_for_phase3` | DRR-03; PDR-03 | Add the bounded overflow, drain, flush, and degradation matrix in the runtime-persistence slice. |
| `QT-REM-008` | `QT-GUAR-SHARED-APPLICATION-CONTRACT` | `retain_for_phase3` | DRR-01/DRR-03; PDR-05 | The platform contract supplies authority; build the CLI/API/MCP operation manifest without waiting for glossary adoption. |
| `QT-REM-009` | `QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION` | `defer_for_proof_environment` | PDR-04 | Closure requires the separately approved isolated source-to-restore rehearsal. |
| `QT-REM-100` | `QT-GUAR-DERIVED-OUTPUT-TIMELINE` | `split_resolution_from_execution` | DRR-02/DRR-03; PDR-05 | Set module-contract and registry ownership, then inventory causal, projection, reporting, and reconstruction consumers. |
| `QT-REM-101` | `QT-GUAR-EXTERNAL-ORDER-SUBMISSION-CLOSED` | `retain_for_phase3` | DRR-03; PDR-03/PDR-05 | Authority already closes submission; perform the security-critical adapter, credential, transport, and composition-root absence inventory. |
| `QT-REM-113` | `QT-GUAR-INDICATOR-OUTPUT-CATALOG-AND-STRATEGY-READS` | `split_resolution_from_execution` | DRR-02/DRR-03; PDR-05 | Decide qualifying module contracts and registry ownership, then generate the output/read denominator. |
| `QT-REM-114` | `QT-GUAR-INDICATOR-OUTPUT-PRESENCE-AND-READINESS` | `split_resolution_from_execution` | DRR-02/DRR-03; PDR-05 | Separate source/registry approval from the mechanical readiness matrix. |
| `QT-REM-115` | `QT-GUAR-INDICATOR-PUBLICATION-AUTHORITY` | `split_resolution_from_execution` | DRR-02/DRR-03 | Decide which contracts/registrations qualify, then test engine-owned sequence publication. |
| `QT-REM-116` | `QT-GUAR-PROJECTION-DOES-NOT-CHANGE-INDICATOR-TRUTH` | `split_resolution_from_execution` | DRR-02/DRR-03 | Assign typed and legacy registries, then close projection-family and mode-transition coverage. |
| `QT-REM-117` | `QT-GUAR-INDICATOR-LIFECYCLE-EVIDENCE-SEPARATION` | `split_resolution_from_execution` | DRR-02/DRR-03 | Approve module-contract discovery, then build lifecycle-output and Strategy-admission matrices. |
| `QT-REM-118` | `QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION` | `retain_for_phase3` | DRR-02/DRR-03; PDR-05 | Accepted decisions establish separation; close producer/schema and negative execution-boundary coverage. |
| `QT-REM-119` | `QT-GUAR-STRATEGY-VARIANT-OUTPUT-FILTER-BOUNDARY` | `retain_for_phase3` | DRR-03 | Inventory public outputs/operators and excluded ownership using existing authoritative terms. |
| `QT-REM-120` | `QT-GUAR-EFFECTIVE-STRATEGY-RESOLUTION-PARITY` | `retain_for_phase3` | DRR-02/DRR-03; PDR-05 | Build the preview/Bot/runtime/reporting/provenance call-site manifest. |
| `QT-REM-121` | `QT-GUAR-DETERMINISTIC-SEQUENTIAL-EXPERIMENT-PLAN` | `defer_for_activation_priority` | DRR-03; PDR-05 | Current behavior aligns; the property/tamper/resume matrix is activation-oriented assurance work. |
| `QT-REM-200` | `QT-GUAR-MODE-AWARE-RUNTIME-COMPOSITION` | `split_resolution_from_execution` | DRR-09/DRR-03; PDR-05 | Ratify current paper/live wording, then inventory composition-root collaborators. |
| `QT-REM-201` | `QT-GUAR-STRATEGY-INDEPENDENT-EXECUTION-ECONOMICS` | `split_resolution_from_execution` | DRR-10/DRR-03; PDR-05 | Choose profile-input versus resolved-context authority, then inventory economics consumers. |
| `QT-REM-202` | `QT-GUAR-PINNED-EXECUTION-CONTEXTS` | `split_resolution_from_execution` | DRR-10/DRR-03; PDR-05 | Choose immutable run authority, then test material, hashes, handoffs, and tamper rejection. |
| `QT-REM-203` | `QT-GUAR-EXPLICIT-EXECUTION-EXIT-POLICY` | `defer_for_activation_priority` | PDR-05 | Representative behavior aligns; exhaustive migration/flag/adapter coverage should follow activation priority. |
| `QT-REM-204` | `QT-GUAR-POST-ONLY-SIGNAL-BAR-CAUSALITY` | `split_resolution_from_execution` | DRR-09 | Ratify the narrow resting-limit interpretation, then execute the post-only adapter matrix. |
| `QT-REM-205` | `QT-GUAR-PROTECTIVE-EXIT-RESIDUAL-TERMINAL-INTEGRITY` | `defer_for_activation_priority` | PDR-05 | Defenses are representative; the full adapter/race matrix is assurance work for a selected activation. |
| `QT-REM-206` | `QT-GUAR-EXECUTION-MODE-PLAYBACK-SEPARATION` | `split_resolution_from_execution` | DRR-09 | Ratify common causality without identical outcomes, then build the FAST/FULL/playback cross-product. |
| `QT-REM-207` | `QT-GUAR-RUNTIME-EXECUTION-OWNERSHIP-QUALITY-CEILING` | `split_resolution_from_execution` | DRR-11; PDR-05 | Owners must adopt the quality denominator before label-conformance checks can be specified. |
| `QT-REM-208` | `QT-GUAR-CANONICAL-ORDER-LIFECYCLE` | `defer_for_activation_priority` | PDR-05 | Lifecycle behavior is aligned; complete reachability/race coverage only if activation is prioritized. |
| `QT-REM-209` | `QT-GUAR-FILL-SETTLEMENT-SINGLE-INGRESS` | `retain_for_phase3` | DRR-03; PDR-05 | Resolve the partial single accounting-ingress conformance in the execution-accounting slice. |
| `QT-REM-210` | `QT-GUAR-WALLET-INITIALIZATION-AND-LEDGER-REPLAY` | `defer_for_activation_priority` | activation planning | Existing behavior aligns; exhaustive initialization/recovery/event-family proof can remain assurance backlog. |
| `QT-REM-211` | `QT-GUAR-SHARED-WALLET-MARKET-TIME-ARBITRATION` | `defer_for_activation_priority` | activation planning | Arbitration is defended; the full source/clock/permutation matrix is required only for activation. |
| `QT-REM-212` | `QT-GUAR-CANONICAL-FILL-ACCOUNTING-RECONCILIATION` | `retain_for_phase3` | DRR-03; PDR-05 | Create one accounting-invariant slice across positions, margin, fees, and reports. |
| `QT-REM-213` | `QT-GUAR-REPLAY-CERTIFIED-EXECUTION-BOOK-TAPE` | `defer_for_activation_priority` | PDR-05 | Representative evidence is strong; full acquisition/reconstruction certification is costly activation work. |
| `QT-REM-214` | `QT-GUAR-PROJECTOR-ONLY-SELECTED-SYMBOL-READS` | `split_resolution_from_execution` | DRR-13; PDR-02 | Repair the unrelated backend locator as trace alignment, then defer full cross-stack proof until Node is admitted. |
| `QT-REM-215` | `QT-GUAR-BOTLENS-CURSOR-LINEAGE` | `defer_for_proof_environment` | DRR-13; PDR-02 | Complete cross-stack state-machine closure requires the admitted frontend runner. |
| `QT-REM-216` | `QT-GUAR-BOTLENS-TYPED-READINESS` | `defer_for_proof_environment` | DRR-13; PDR-02 | Backend/frontend readiness parity depends on reproducible Node evidence. |
| `QT-REM-217` | `QT-GUAR-BOTLENS-HOT-STATE-NOT-HISTORY` | `defer_for_proof_environment` | DRR-13; PDR-02/PDR-05 | Hot-versus-history proof spans frontend and backend; admit Node before closure. |
| `QT-REM-218` | `QT-GUAR-OVERLAY-COMPLETENESS-ISOLATION` | `defer_for_proof_environment` | DRR-13; PDR-02 | The completeness model may be planned, but attestation v1 lacks a PASS-capable Node path. |
| `QT-REM-219` | `QT-GUAR-BOT-RUN-CONTAINER-IDENTITY-SEPARATION` | `defer_for_proof_environment` | DRR-13; PDR-02, conditional PDR-03 | Closure spans frontend, persistence, and control plane; admit Node and real DB proof where claimed. |
| `QT-REM-220` | `QT-GUAR-TRADE-MARKER-CAUSAL-CANDLE-PROJECTION` | `defer_for_proof_environment` | DRR-13; PDR-02 | Represented enforcement is adequate; the explicit blocker is a PASS-capable Node path. |
| `QT-REM-300` | `QT-GUAR-PROVIDER-CREDENTIAL-REFERENCE-CONFINEMENT` | `retain_for_phase3` | security integration; conditional PDR-03 | Add encrypted round-trip, bad-key, rotation, and plaintext-absence checks; use real DB proof if persisted rows are inspected. |
| `QT-REM-301` | `QT-GUAR-V1-LOCAL-TRUST-BOUNDARY` | `retain_for_phase3` | deployment boundary; conditional isolated-deployment profile | Rendered-port inventory is straightforward; instantiated topology may share the profile needed by REM-311. |
| `QT-REM-302` | `QT-GUAR-BOT-RUN-LEASE-OWNERSHIP` | `defer_for_proof_environment` | expanded PDR-03 | Persisted PostgreSQL lease behavior is omitted from the current DB-ceiling coverage. |
| `QT-REM-303` | `QT-GUAR-ASYNC-JOB-OWNERSHIP-FENCING` | `defer_for_proof_environment` | PDR-03/PDR-05 | Static effect inventory can be prepared, but transactional closure requires isolated PostgreSQL. |
| `QT-REM-304` | `QT-GUAR-ENUMERATED-AGENT-MUTATION-GATES` | `defer_for_owner_approval` | DRR-01 | Ratify ADR 0048’s offline-research scope before narrowing wording or changing enforcement. |
| `QT-REM-305` | `QT-GUAR-SOLE-POSTGRES-PERSISTENCE-AUTHORITY` | `defer_for_proof_environment` | DRR-01/DRR-04; expanded PDR-03/PDR-05 | Contract-backed semantics align; bootstrap, migration, and drift closure need isolated PostgreSQL. |
| `QT-REM-306` | `QT-GUAR-BLOCKING-API-WORK-OFFLOAD` | `retain_for_phase3` | DRR-03; PDR-05 | Replace the hand-maintained mixed-file list with generated async-surface discovery. |
| `QT-REM-307` | `QT-GUAR-BOUNDED-NONCANONICAL-OBSERVABILITY` | `defer_for_activation_priority` | PDR-05 | Representative bounds are defended; whole-sink completeness is activation-level assurance. |
| `QT-REM-308` | `QT-GUAR-SINGLE-LOKI-INGRESS-PER-TOPOLOGY` | `defer_for_owner_approval` | DRR-14 | Ratify Alloy for native server, decide whether local Promtail is supported or historical, and only then align the topology wording. |
| `QT-REM-309` | `QT-GUAR-DIAGNOSTICS-NOT-EXECUTION-TRUTH` | `retain_for_phase3` | diagnostic authority; conditional PDR-03 | Inventory diagnostic families; use real DB proof if durable persistence is asserted. |
| `QT-REM-310` | `QT-GUAR-BOUNDED-TELEMETRY-CONTROL-DELIVERY` | `defer_for_activation_priority` | PDR-05 | Reliability is representative; exhaustive scheduler/fallback combinations can remain activation backlog. |
| `QT-REM-311` | `QT-GUAR-ATTESTED-SINGLE-NODE-DEPLOYMENT` | `defer_for_proof_environment` | PDR-01; proposed isolated-deployment profile | Static checks cannot execute clean deploy, registry mismatch, rollback, or recovery. |
| `QT-REM-312` | `QT-GUAR-SEMANTIC-OPERATIONAL-FINGERPRINT-SEPARATION` | `defer_for_activation_priority` | PDR-05 | Current semantics align; exhaustive field mutation belongs to an activation-selected reporting claim. |
| `QT-REM-313` | `QT-GUAR-PR-VERIFICATION-TOPOLOGY` | `split_resolution_from_execution` | DRR-12/DRR-03; PDR-03 | Align four-job and job-versus-step wording, then separately execute topology and DB proof. |
| `QT-REM-314` | `QT-GUAR-CONTRACT-DRIVEN-GENERIC-SURFACES` | `retain_for_phase3` | DRR-03; PDR-05 | Generic-surface and dynamic-registration inventories are central platform-maintenance work. |
| `QT-REM-315` | `QT-GUAR-AGENT-WORKFLOW-BOUNDARIES` | `defer_for_owner_approval` | DRR-01/DRR-02/DRR-03 | Until precedence, scope, and owner discovery are approved, missing checks cannot become required enforcement. |
| `QT-REM-400` | `QT-GUAR-CANONICAL-MARKET-IDENTITY-ROUTING` | `split_resolution_from_execution` | DRR-06/DRR-03; PDR-05 | Ratify aliases as hints/compatibility only, then execute the complete route inventory. |
| `QT-REM-401` | `QT-GUAR-TYPED-SPARSE-DATA-FAILURE` | `retain_for_phase3` | DRR-03; PDR-05 | Inventory downstream conversions because silent zero/synthetic continuity would change system meaning. |
| `QT-REM-402` | `QT-GUAR-BUDGETED-CLOSED-CANDLE-MARKET-STREAM` | `retain_for_phase3` | DRR-03; PDR-05 | Add fatal-error, heartbeat-staleness, and shutdown-race coverage in the live-data slice. |
| `QT-REM-403` | `QT-GUAR-PROVIDER-CAPABILITY-AUTHORIZATION` | `retain_for_phase3` | DRR-03; PDR-05 | Build the provider-operation capability and authorization matrix. |
| `QT-REM-404` | `QT-GUAR-TYPED-CONSUMER-FACT-REQUIREMENTS` | `retain_for_phase3` | DRR-03; PDR-05 | Generate and validate the typed consumer-requirement denominator. |
| `QT-REM-405` | `QT-GUAR-FENCED-IDEMPOTENT-SCHEDULED-COLLECTION` | `defer_for_proof_environment` | DRR-03; PDR-03/PDR-05 | Handler inventory may be prepared, but races/retries/missed schedules require isolated PostgreSQL. |
| `QT-REM-406` | `QT-GUAR-DURABLE-VERIFIED-RAW-ARCHIVE` | `split_resolution_from_execution` | DRR-08 | Ratify lifecycle wording and supported archive backends, then run crash/durability coverage. |
| `QT-REM-407` | `QT-GUAR-PIN-SAFE-MARKET-DATA-LIFECYCLE` | `split_resolution_from_execution` | DRR-08 | Separate lifecycle-policy alignment from disposable-object pin/compaction rehearsal. |
| `QT-REM-408` | `QT-GUAR-INTERVAL-VALID-ORDER-BOOK-TRUTH` | `split_resolution_from_execution` | DRR-08; expanded PDR-03 | Ratify the state/lifecycle denominator, then add restart/checkpoint persistence proof. |
| `QT-REM-409` | `QT-GUAR-CODE-OWNED-AUDITED-COLLECTOR-CONTROL` | `split_resolution_from_execution` | DRR-08; expanded PDR-03 | Align market-structure scope, then inventory operations and prove immutable audit persistence. |
| `QT-REM-410` | `QT-GUAR-PROVEN-ZERO-TRADE-COVERAGE` | `split_resolution_from_execution` | DRR-08; PDR-01/PDR-03 | Preserve and correct frozen trace lineage, then separately approve scope and run persistence proof. |

## Cross-Cutting Corrections To Carry Forward

- The current `QT-PROOF-CEILING-003` is accurate for the seven cataloged
  database definitions. Approved remediation would add isolated-DB work for at
  least `QT-REM-006`, `302`, `305`, `408`, and `409`; `219`, `300`, and
  `309` are conditional on using real PostgreSQL.
- `QT-REM-311` needs a reviewed isolated deployment/Compose rehearsal profile;
  none of the current four environment profiles can honestly execute it.
- `QT-REM-406` may need object-store-capable proof if its approved backend
  denominator extends beyond disposable local storage.
- DRR-04’s schema-authority review must include dependencies from
  `QT-REM-002` and `006` even though `QT-RM-DATA-002` itself names only
  `QT-GC-006`/`007`.
- PDR-05 records the frozen 42-guarantee universe. Broad remediation wording may
  introduce new complete/full/every denominators; coverage expands only after
  those plans are approved.
- Glossary adoption is not a prerequisite for technical work. Use guarantee IDs
  and existing authority language until DRR-15 is separately approved.
- `AGENTS.md` precedence does not block work already supported by platform
  contracts; it matters only when `AGENTS.md` itself is offered as authority or
  activation support.

## Stop Condition

All 68 records have a proposed disposition. None is approved, edited, executed,
closed, or treated as proof. Owner approval and a separate Phase 3 authorization
are required before any disposition becomes work.
