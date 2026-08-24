# Gate 2 — Model And Calibration Review

## Decision Requested

Approve or reject the Phase 2A vocabulary, registry, proof, attestation, human
view, and twelve-record calibration models as the basis for Phase 2B
whole-system classification.

Approval authorizes classification of the remaining 63 Phase 1 candidates
under this model. It does **not**:

- activate any guarantee;
- adopt the proposed or blocked glossary entries;
- resolve `QT-CONFLICT-007` or any other normative conflict;
- change product behavior or repair product semantics;
- supersede an ADR or change the existing authority hierarchy;
- delete, archive, consolidate, or move existing material; or
- authorize integration into `develop`.

Phase 2A stops at this gate. Phase 2B requires explicit approval.

## Frozen Subject And Isolation

- audited ref: `origin/develop`;
- audited commit: `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`;
- campaign branch: `feat/docs-guarantee-reconciliation`;
- Gate 1 approval: 2026-08-23;
- product-semantic changes in Phase 2A: none;
- guarantee activations in Phase 2A: zero;
- adopted glossary entries in Phase 2A: zero;
- deleted or archived material in Phase 2A: none;
- whole-system classification complete: no.

The Phase 1 inventories, findings, conflicts, and frozen coverage denominator
remain intact. Phase 2A adds a review layer over them; it does not rewrite their
results.

## Deliverables Ready For Review

1. [Proposed glossary](proposed-glossary.md) — 21 calibration-scoped entries:
   19 proposed, with `QT-TERM-006` (Research Observation) and `QT-TERM-012`
   (Check preview) blocked by unresolved semantics. All other
   Phase 1 terms and all 20 alias findings remain deferred/unadopted.
2. [Calibration record](phase-2a-calibration.md) — twelve representative
   candidates with exact frozen authority, enforcement, proof, environment,
   wording, and conflict findings.
3. [Machine registry](../../assurance/guarantees/registry.json) — independent
   claim lifecycle, registry disposition, conformance, enforcement maturity,
   proof maturity, proof mode, authority, enforcement, terminology, finding,
   remediation, and replacement axes.
4. [Proof catalog](../../assurance/guarantees/proof-catalog.json) — fifteen
   stable proof definitions across non-database, isolated-database, and manual
   recovery profiles. It stores requirements and coverage, never results.
5. [Generated human view](../../assurance/guarantees/GUARANTEES.md) — a
   deterministic projection of the registry and proof catalog, including
   reverse claim-to-proof navigation.
6. [Versioned schemas](../../assurance/guarantees/schemas/) and
   [attestation rules](../../assurance/guarantees/attestations/README.md) —
   strict JSON interchange and immutable, exact-commit, multi-environment
   verification semantics.
7. `scripts/docs/guarantees.py`, repository contract tests, and Make/CI-profile
   integration — deterministic render/check plus structural, cross-file,
   activation, authority, proof, and attestation validation.

The assurance directory is explicitly an index into existing authority. It is
not a new normative layer. The proposed glossary remains in this campaign
directory and outside the platform-contract read order through Gate 2 and until
a separate terminology-adoption review.

## Calibration Outcome

The batch retains the ten Gate 1 recommendations and adds two model pressure
tests: destructive recovery (`QT-GC-070`) for manual/multi-environment evidence,
and architecture-doc validation (`QT-GC-073`) for an explicit validator
capability ceiling.

| Registry disposition | Count | Meaning at Gate 2 |
| --- | ---: | --- |
| `enforced` | 0 | Reserved for an authoritative claim that is adequately backed; no calibration row clears that bar |
| `partially_enforced` | 8 | The intended claim is clear, but enforcement or proof remains incomplete |
| `candidate` | 2 | Desirable or intended behavior is represented, but the cited source is not an adopted normative promise |
| `contradicted` | 1 | `QT-GC-009` preserves incompatible accepted authorities and a legacy implementation seam |
| `implementation_property` | 1 | `QT-GC-073` records what the current validator does and, importantly, does not prove |

All twelve records have `activation_status: unactivated`; zero are `active`.
The classification counts above therefore do not imply adoption.

The eight partially enforced rows and the contradicted row have
`remediation_status: pending`; the two candidate rows and the implementation
property are `not_required`. No Phase 1 finding alias is presented as a
remediation plan. Concrete `QT-REM-*` records remain owner/reviewer work for
Phase 2B; the model requires exact ownership/review metadata and nonempty gap,
action, acceptance-criteria, and proof-plan sections. Such records are required
before whole-system partial or contradicted classification can be complete.

Static conformance is six aligned, five partial, and one contradicted. Proof
maturity is one adequate and eleven partial. Those are model assessments of
named evidence, not fresh proof results.

The twelve records exercise:

- causal/prefix invariance;
- append-only database enforcement and deterministic revision pinning;
- provider-free canonical reads;
- frozen replay and evidence hashing;
- preview/persistence separation;
- an unresolved terminology and accepted-ADR conflict;
- durable reporting projections;
- lifecycle-ledger authority;
- strict durable persistence versus degradable projections;
- CLI/API/MCP application-contract parity;
- destructive recovery with manual and unavailable-environment outcomes; and
- a documentation validator with an explicit capability ceiling.

This is calibration, not sampling evidence for the other 63 candidates. No
whole-system coverage claim follows from the batch.

## Proof And Attestation Boundary

Durable registry and proof files cannot contain execution-result fields or
tokens. Result states exist only in attestations:

`PASS`, `FAIL`, `NOT_RUN`, `MANUAL`, `PARTIAL`, and `UNAVAILABLE`.

An attestation binds an exact Git commit, clean-state check, assurance-material
hash, catalog and glossary inputs, per-guarantee authority/enforcement material,
per-required-proof target material, one or more named environments,
tool/service identities, the exact shell-free executed argument vector, proof
collection/exit/output evidence (including explicit pytest outcome counts),
hashed manual evidence, and derived per-guarantee results. It composes required
proofs across environments and aggregates conservatively. A manual PASS
requires distinct operator and reviewer identities; it never masquerades as
automated PASS. A zero-collection, skipped, xfailed, or xpassed test cannot
pass.

Phase 2A creates no synthetic or mutable attestation. The named calibration
proofs remain unrun as guarantee attestations. The model's own contract tests
validate the machinery, not the product claims.

## Activation Guard

The validator rejects `activation_status: active` while Gate 2 is pending. Gate
2 approves only the classification model and can never serve as an activation
decision. After Gate 2, activation would still require a distinct, explicit
per-guarantee activation review and all of the following:

- current claim lifecycle;
- an active primary authority from the actual normative hierarchy;
- no conflicting authority or unresolved remediation;
- static alignment;
- adequate or defense-in-depth enforcement with complete coverage;
- adequate proof maturity and an active, complete, required proof mapping; and
- only adopted, non-blocked glossary dependencies;
- an exact reviewed activation-decision reference whose decision type is
  guarantee activation, not Gate 2 model approval; and
- an immutable referenced attestation whose derived result for the guarantee is
  PASS.

An accepted ADR alone cannot activate a platform behavior guarantee. A proof
result does not independently activate or deactivate durable classification.

## Validation Evidence

- strict registry and proof validation: 12 claims / 15 proofs;
- deterministic generated-view check: clean;
- focused registry/attestation contract suite: 36 passed;
- combined guarantee and architecture-document contract suite: 38 passed;
- documentation CI pytest profile: 38 passed, 20 deselected;
- all automated catalog selectors collected successfully: 46 concrete tests
  across 14 pytest proof definitions; collection is not a proof execution;
- proposed-glossary local links: 62 occurrences, 36 unique path/anchor
  targets across 17 files, zero failures;
- calibration path/line citations: 79 occurrences, 78 unique citations across
  61 files and 112 individual line/range segment checks, zero missing or
  out-of-bounds references;
- versioned registry, proof-catalog, and attestation schemas: valid JSON and
  checked against executable versions/enums;
- documentation sync workflow: ran and safely skipped because no destination
  is configured.

No architecture document changed in Phase 2A. The active architecture-index
generator was therefore not used to rewrite repository documentation; its
read-only contract suite passed as reported above.

No line above is a product-guarantee PASS.

## Known Model Ceilings Retained For Review

- Owner values are structurally validated provisional slugs. Phase 1 found no
  adopted canonical owner registry, so Phase 2A does not invent one.
- Version 1 permits automatic activation authority only from an active
  normative platform contract. Source-module contracts remain represented as
  in-scope authority, but cannot activate a record until a reviewed component
  owner/discovery allowlist exists. Repository-agent governance (`AGENTS.md`)
  likewise remains non-activating while its precedence is unresolved.
- Attestation validation proves repository bindings, hashes, typed result
  summaries, and internal runner/result consistency; it does not
  cryptographically authenticate runner or reviewer identity, or establish
  that a named reviewer holds the required authority. The existing authority
  hierarchy must validate that outside this model. The distinct external
  activation review is the trust boundary and must bind the exact attestation
  it verified.
- Version 1 binds the full registry semantic projection and proof catalog.
  Unrelated later additions therefore conservatively require re-attestation;
  per-claim snapshot narrowing is deferred rather than implied.
- Automated PASS is limited to pytest in version 1. Other runner kinds may be
  cataloged, but cannot satisfy an activation until their result and count
  semantics receive explicit modeling and review.
- Exact line locators are baseline-bound calibration evidence and may move in
  later edits; authority identity remains path/heading based where a durable
  heading exists.
- Many candidate claims retain partial proof because absence/universality
  claims need generated surface inventories or static dependency rules.
- Real database and destructive-recovery proof environments remain separately
  required; Phase 2A does not use a live or production database as a shortcut.
- `QT-CONFLICT-007`, `QT-CONFLICT-010`, the blocked glossary entries, and the
  split implementation seam remain unresolved by design.

## Gate 2 Review Questions

Approval should confirm only that:

1. the proposed glossary entry model is appropriately subordinate to existing
   authority and keeps blocked/deferred vocabulary unadopted;
2. the registry's independent axes and activation guard prevent implementation
   evidence from becoming authority by implication;
3. the proof catalog and multi-environment attestation model are sufficiently
   conservative for whole-system classification;
4. the twelve-record batch exercises enough distinct claim and proof shapes to
   calibrate Phase 2B; and
5. the stated ceilings, conflicts, and subsystem/normative review gates remain
   visible and unresolved.

If approved, Phase 2B will classify the remaining 63 candidates and stop at
the required subsystem/normative reviews. If rejected, Phase 2A should revise
the model or batch without changing product semantics.
