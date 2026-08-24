# Decision Resolution — Proof And Environment Dispositions

## Purpose And Boundary

This packet proposes dispositions for all nine Phase 2B proof ceilings. It
reduces them to five owner-facing choices without executing a proof, changing
the proof/attestation model, creating an attestation, or reporting a result.

The 85 catalog entries remain proof **definitions**. No PASS, FAIL, MANUAL,
PARTIAL, UNAVAILABLE, or other product-proof result is created here.

## Five Owner-Facing Choices

### PDR-01 — Conservative First-Cycle Assurance Posture

**Recommendation.** Keep the conservative version 1 posture for QT’s first
stable activation cycle:

- proof definitions remain obligations, never results;
- attestations bind an exact clean commit, full registry semantics, the full
  proof catalog, referenced authority/enforcement material, proof targets,
  lockfiles, environments, and hashed output;
- protected CI/repository identity provides run provenance;
- an external activation reviewer verifies the human’s canonical role authority
  and binds the exact attestation ID and digest;
- frozen baseline locators never move; a reviewed current locator is layered
  alongside the frozen one with commit, reviewer, and reason; and
- whole-registry/catalog snapshot binding remains until the first stable cycle
  shows that its conservative coupling is operationally excessive.

This combines ceilings `001`, `005`, `006`, and `007`. Definition/result
separation and frozen-locator preservation are derivable. Identity trust and
whole-snapshot scope require owner judgment.

**Alternatives and consequences.**

- Cryptographic/Sigstore-style provenance now provides stronger identity at
  higher setup cost.
- Per-claim dependency closure now reduces re-execution but adds substantial
  invalidation complexity.
- Free-form identity strings or silent attestation reuse are insufficient.

**Affected.** All 75 candidates/guarantees and all 68 remediations. Canonical
ownership from DRR-02 is a prerequisite; a role slug is not authenticated
authority.

**Exact evidence.** `docs/assurance/guarantees/attestations/README.md:20-90`;
`docs/assurance/guarantees/schemas/attestation.v1.schema.json:177-208,246-270,467-550`;
`scripts/docs/guarantees.py:3059-3153`;
`docs/plans/documentation-reconciliation/authority-matrix.md:79-88`;
`docs/plans/documentation-reconciliation/phase-2b-review-map.json:2901-2918`.

### PDR-02 — Native Frontend Automated Results

**Recommendation.** Approve a later model extension for the existing native
Node runner:

- retain a shell-free `node --test ...` invocation and amend the catalog to
  bind an explicit deterministic reporter/event-wrapper transport;
- retain the current `frontend-node` profile bindings—Python
  `>=3.12,<3.13`, Node `>=20,<21`, `requirements.lock`, and
  `portal/frontend/package-lock.json`—unless a later review proves and adopts a
  narrower Node-only profile;
- require an exact expected test-name set or match count;
- emit a typed result summary from the bound reporter/event wrapper rather than
  inferring it from a console regex;
- require every target to collect and the exact selected names to pass with
  exit zero and zero failed, cancelled, todo, or explicitly skipped selected
  results; `--test-name-pattern` nonmatches are recorded as excluded rather
  than treated as proof failures; and
- hash stdout, stderr, and the typed summary.

The eight definitions remain unexecuted and have no result. A later attestation
may record `NOT_RUN` or `UNAVAILABLE` until the runner semantics are admitted.

**Alternatives and consequences.** Keeping pytest-only v1 leaves the eight Node
definitions unable to supply activation-eligible automated PASS and blocks
activation of affected guarantees. Manual
frontend evidence is weaker and more costly. Adding another runner solely for
assurance is unnecessary because QT already uses `node --test`.

**Affected.**

| Proof | Candidate | Guarantee | Remediation |
| --- | --- | --- | --- |
| `QT-PROOF-215` | `QT-GC-050` | `QT-GUAR-PROJECTOR-ONLY-SELECTED-SYMBOL-READS` | `QT-REM-214` |
| `QT-PROOF-217` | `QT-GC-051` | `QT-GUAR-BOTLENS-CURSOR-LINEAGE` | `QT-REM-215` |
| `QT-PROOF-219` | `QT-GC-052` | `QT-GUAR-BOTLENS-TYPED-READINESS` | `QT-REM-216` |
| `QT-PROOF-221` | `QT-GC-053` | `QT-GUAR-BOTLENS-HOT-STATE-NOT-HISTORY` | `QT-REM-217` |
| `QT-PROOF-223` | `QT-GC-054` | `QT-GUAR-OVERLAY-COMPLETENESS-ISOLATION` | `QT-REM-218` |
| `QT-PROOF-225` | `QT-GC-055` | `QT-GUAR-BOT-RUN-CONTAINER-IDENTITY-SEPARATION` | `QT-REM-219` |
| `QT-PROOF-226` | `QT-GC-056` | `QT-GUAR-OPERATOR-CONSOLE-NONAUTHORITATIVE-SURFACE` | none; candidate disposition |
| `QT-PROOF-227` | `QT-GC-057` | `QT-GUAR-TRADE-MARKER-CAUSAL-CANDLE-PROJECTION` | `QT-REM-220` |

**Exact evidence.**
`docs/assurance/guarantees/proof-catalog.json:5-13,1113-1420`;
`portal/frontend/package.json:6-11`;
`scripts/docs/guarantees.py:1802-1812,3294-3307`.

### PDR-03 — Disposable Database Proof And Clean Collection

**Recommendation.** Approve a later profile contract using frozen CI behavior
as reference evidence:

- a fresh disposable PostgreSQL 15 / TimescaleDB 2.14.2 service;
- required `timescaledb` and `pgcrypto` extensions;
- Python 3.12 and the bound requirements lockfile;
- a per-session isolated database identity, achieved by a unique database or a
  proven reset/cleanup boundary, plus `RUN_DB_TESTS=1` and
  `QT_DB_TEST_ISOLATED=1`;
- exact image/digest, server/extension versions, database identity, bootstrap
  log, timing, argv, result summary, and cleanup evidence; and
- CI as the canonical execution site. A local container may satisfy the
  environment profile if it proves the same constraints, but it is
  activation-eligible only if it also satisfies PDR-01's separately approved
  provenance and identity model.

Collection-only validation must not resolve a DSN, initialize schema, or access
persistence. Supplying a database merely to hide the import diagnostic would
mask the defect. Actual proof execution and a clean collection rerun wait for
Phase 3.

**Alternatives and consequences.** CI-only simplifies trust but reduces local
reproducibility. Any attested disposable container matching the profile is the
recommended balance. Shared development, live, or production databases are
prohibited.

Frozen CI currently shares `quanttrad_contracts` within its database-marked
job. It is reference evidence for the proposed versions and bootstrap, not an
assertion that the present job already satisfies the proposed per-session
isolation contract.

**Affected.**

| Proof | Candidate coverage | Current remediation coverage |
| --- | --- | --- |
| `QT-PROOF-002` | `QT-GC-003` | `QT-REM-002` |
| `QT-PROOF-004` | `QT-GC-004`, `008`; supporting `005` | `QT-REM-003`; none for `008`/`005` |
| `QT-PROOF-005` | `QT-GC-005` | none |
| `QT-PROOF-303` | `QT-GC-061` | `QT-REM-303` |
| `QT-PROOF-314` | `QT-GC-072` | `QT-REM-313` |
| `QT-PROOF-405` | `QT-GC-021` | `QT-REM-405` |
| `QT-PROOF-410` | `QT-GC-026` | `QT-REM-410` |

Collection-side-effect scope is all 76 pytest definitions, 73
candidates/guarantees, and 67 remediations—everything except frontend-only
`QT-GC-056`/`057` and `QT-REM-220`.

**Exact evidence.** `.github/workflows/test.yaml:100-165`;
`tests/conftest.py:226-240`; `portal/backend/db/session.py:276-305`;
`docs/assurance/guarantees/proof-catalog.json:25-33`;
`docs/plans/documentation-reconciliation/phase-2b-report.md:187-211`.

### PDR-04 — Recovery Rehearsal

**Recommendation.** Approve the procedure design without creating a present
result:

- before execution, separately review the proposed `QT-PROOF-014` definition
  and admit its catalog lifecycle as active;
- a disposable, non-production backup source with representative schema/data;
- a separately identified empty restore target;
- backup identity and checksum;
- restoration of schema, extensions, constraints, indexes/triggers,
  representative content, and application reads;
- negative checksum and active-retention-pin cases;
- commands, timing, source revision, operator identity, and independent
  reviewer identity; and
- an authorized immutable attestation records `MANUAL` if the evidence lacks
  independent acceptance, PASS if acceptance is already bound, or
  `UNAVAILABLE` if prerequisites are absent.

**Alternatives and consequences.** Deferral remains honest and unproved; a
later authorized attestation may record `UNAVAILABLE` if prerequisites are
absent. Parts may be automated later while retaining independent operational
review. Production cannot be used as the rehearsal target.

**Affected.** `QT-PROOF-014` → `QT-GC-070` →
`QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION` → `QT-REM-009`.

**Exact evidence.** `docs/engineering/server-deployment.md:344-363`;
`docs/plans/documentation-reconciliation/phase-2a-calibration.md:432-459`;
`docs/assurance/guarantees/proof-catalog.json:364-386`;
`docs/assurance/guarantees/remediations/QT-REM-009.md`;
`docs/assurance/guarantees/attestations/README.md:68-90`.

### PDR-05 — Universal And Absence Claims

**Recommendation.** Approve one repository-wide rule, with each subsystem owner
selecting the applicable branch:

1. **Closed static surface:** commit-bound generated inventory plus a static
   dependency/import/routing rule.
2. **Closed runtime surface:** attestation captures built-ins, loaded
   extensions, and the extension-admission rule.
3. **Intentionally open surface:** wording is limited to built-ins or enumerated
   entrypoints and states extension behavior separately.
4. **No defensible closure:** narrow the claim or keep it partial.

Representative examples never establish “all,” “every,” “never,” or absence.

**Alternatives and consequences.** Representative tests as whole-system proof
are invalid. Permanently freezing open registries contradicts extensibility.
Narrowing every claim is safe but discards meaningful intended commitments.
Manual inventories are acceptable temporarily but weak as durable proof.

**Affected.** The exact 42 candidates and guarantees recorded under
`QT-PROOF-CEILING-008`. Current remediation impact is 38 records:
`QT-REM-001`, `003`–`005`, `008`, `100`, `101`, `113`, `114`, `118`,
`120`, `121`, `200`–`203`, `205`, `207`–`209`, `212`, `213`, `217`,
`219`, `303`, `305`–`307`, `309`, `310`, `312`, `314`, `400`–`405`.

**Exact evidence.**
`docs/plans/documentation-reconciliation/phase-2a-calibration.md:517-522`;
`docs/plans/documentation-reconciliation/phase-2b-review-map.json:2921-3025`;
`docs/contracts/platform/03_engineering_contract.md:10-31`.

## Exact-Once Nine-Ceiling Disposition

| Ceiling | Class | Recommended disposition | Exact required reviewers | Work reserved for a later authorization |
| --- | --- | --- | --- | --- |
| `QT-PROOF-CEILING-001` | `execution/proof` | Accept definition/result separation as settled; close only through fresh commit-specific attestations | assurance-model owner, proof-catalog owner, testing owner | verification, execution, evidence, attestations |
| `QT-PROOF-CEILING-002` | `owner-judgment` | Approve PDR-02 runner-specific Node semantics; definitions remain unexecuted and a later authorized attestation may record `NOT_RUN`/`UNAVAILABLE` | frontend owner, proof-model reviewer, testing owner | schema/validator/runner changes and execution |
| `QT-PROOF-CEILING-003` | `execution/proof` | Admit PDR-03 versioned disposable DB profile; never use live/shared data | database-test owner, persistence owner, proof-environment owner, testing owner | environment implementation and seven cataloged DB runs |
| `QT-PROOF-CEILING-004` | `execution/proof` | Approve PDR-04 isolated recovery protocol; defer catalog admission, rehearsal, and attestation | operations owner, proof-environment owner, recovery owner, security owner | catalog lifecycle admission, provisioning, rehearsal, independent review |
| `QT-PROOF-CEILING-005` | `owner-judgment` | Use protected CI/repository identity plus external activation review for v1, conditional on canonical role ownership | activation-review owner, assurance-model owner, repository-governance owner | provenance binding and future cryptographic hardening |
| `QT-PROOF-CEILING-006` | `owner-judgment` | Retain whole-registry/catalog binding through stabilization; defer per-claim narrowing | assurance-model owner, proof-catalog owner, registry owner | optional v2 dependency-slice model |
| `QT-PROOF-CEILING-007` | `derivable` | Preserve frozen locators and layer reviewed current locators; never silently retarget | documentation-assurance owner, proof-catalog owner, registry owner | validator and relocation-record implementation |
| `QT-PROOF-CEILING-008` | `owner-judgment` | Approve PDR-05 four-branch denominator rule and route only genuine domain ambiguity to owners | architecture-documentation owner, component-owner reviewer, proof-model reviewer, testing owner | generated inventories/static rules and claim narrowing |
| `QT-PROOF-CEILING-009` | `derivable` | Keep collection as selector validation only; require side-effect-free import/collection before calling it clean | database-test owner, portal-persistence owner, proof-environment owner, testing owner | isolation change and clean collection rerun |

Count check: nine source ceilings, nine mapped occurrences, nine unique IDs.
Their exact source records are
`docs/plans/documentation-reconciliation/phase-2b-review-map.json:2716-3059`.

## Newly Surfaced Profile Limits

These are proposed follow-ups only. This pass does not add a profile or expand
the proof model.

### Database profile underspecification

The current `python-db-isolated` profile names Python, a lockfile, and an
abstract service key. The validator checks tool ranges, lockfile hashes, and a
nonempty service value, but not PostgreSQL/Timescale versions, image digest,
extensions, bootstrap, cleanup, or actual isolation. Frozen CI supplies
`timescale/timescaledb:2.14.2-pg15` and isolation flags, but implementation
evidence is not an adopted profile contract.

If the related remediation plans are approved, isolated-DB coverage must expand
beyond the current seven catalog definitions to at least `QT-REM-006`, `302`,
`305`, `408`, and `409`. `QT-REM-219`, `300`, and `309` also require it if
their persistence assertions use real PostgreSQL rather than fixtures.

Exact evidence:
`docs/assurance/guarantees/proof-catalog.json:25-33`;
`scripts/docs/guarantees.py:3191-3228`;
`.github/workflows/test.yaml:100-165`.

### No deployment execution profile

`QT-PROOF-311` is a static pytest definition in `python-nondb`; it does not
execute a deployment. `QT-REM-311:18-35` requires an isolated Compose fixture,
fake registry, negative migration case, rollback, and recovery. A later review
should consider an `isolated-deployment` profile binding Docker/Compose
versions, host OS/architecture, image registry and identities, rollback target,
and negative evidence.

Exact impact: `QT-GC-069`,
`QT-GUAR-ATTESTED-SINGLE-NODE-DEPLOYMENT`, `QT-REM-311`, and
`QT-PROOF-311`. `QT-REM-301` may share this profile if its host-network proof
is instantiated rather than static.

### Other conditional environment expansion

- `QT-REM-406` may need an object-store-capable environment if “every
  configured backend” extends beyond disposable local storage.
- New broad remediation denominators may extend the frozen 42-guarantee scope
  of ceiling 008. Coverage changes only after those plans are approved.

None of these observations supplies a result, changes an environment profile,
or authorizes execution.

## Stop Condition

These dispositions await owner approval alongside DRR-01 through DRR-15.
Phase 3, model changes, environment provisioning, dependency installation,
proof execution, attestations, and guarantee activation remain unauthorized.
