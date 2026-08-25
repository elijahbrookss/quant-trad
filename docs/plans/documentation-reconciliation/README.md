# QT Documentation Reconciliation Campaign

This directory is the non-normative execution record for the whole-system
documentation, terminology, architecture-conformance, and guarantee-traceability
campaign. It records what was inspected and what remains unresolved; it does not
replace `AGENTS.md`, the platform contracts, accepted ADRs, or component owners.

## Frozen Baseline

- Baseline branch: `origin/develop`
- Baseline commit: `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`
- Campaign branch: `feat/docs-guarantee-reconciliation`
- Current phase: Phase 3 authorized and in progress
- Current gate: final residual-risk and integration review after Phase 3

`develop` remains unchanged for the duration of the campaign. The baseline is a
fixed audit subject rather than a moving integration target.

## Authority During The Audit

The repository's existing source classes remain in force as the subject of this
audit. `AGENTS.md` governs agent and contributor work within its repository
scope; its precedence relative to platform behavior contracts is not stated
precisely enough for this campaign to invent one. The platform-document and
evidence order is:

1. `docs/contracts/` state normative platform behavior.
2. Accepted ADRs explain durable decisions and their tradeoffs.
3. Architecture and concept documents explain current boundaries and flows.
4. Implementation, database constraints, tests, and runtime surfaces provide
   conformance and enforcement evidence.
5. Plans, validation records, incidents, and campaign dossiers retain working or
   historical evidence without silently becoming current contracts.

The audit may identify a normative document as apparently stale or contradicted,
but implementation does not replace that authority automatically. Any change to
normative meaning requires an explicit reviewed decision. For assurance-index
purposes, `AGENTS.md` remains non-activating until its precedence and ownership
model is reviewed, and source-module contracts remain non-activating until their
discovery and ownership model is reviewed.

## Phase 1 Coverage Policy

The coverage denominator is enumerated from the frozen Git tree. It includes:

- every tracked Markdown document, including the six outside `docs/`;
- every architecture component represented by frontmatter and the generated
  component index;
- every referenced architecture `code_paths` owner;
- 74 database models/tables, clean-bootstrap ownership, and every tracked SQL or
  Python migration/schema artifact;
- all 203 CLI command nodes (178 executable leaves), 218 API routes, and 59 MCP
  tools plus 10 resources and 29 templates;
- CI, Make, pytest, and documentation-validation surfaces;
- 249 Python suites, 44 frontend suites, test-support files, and each applicable
  execution profile, including tests that are tracked but not wired;
- Mermaid sources, generated render counterparts, unverified source-less assets,
  and research evidence assets;
- 648 reproducibly selected contract-language passages across 471 Python files
  under `src/`, `portal/backend/`, `cli/`, and `scripts/`.

The contract-language selection is deliberately mechanical and auditable. It
selects full module, class, function, and async-function docstrings (including
private and nested symbols) when a word-bounded contract token occurs. It groups
consecutive standalone comments only when their indentation and line continuity
match, retains each inline comment as a singleton passage, and emits one ledger
row per matching docstring or comment block. The token expression is versioned
in `scripts/docs/build_reconciliation_inventory.py`; selection is triage, not a
claim that every selected passage is a valid guarantee.

Rendered assets are inventoried for lineage but are not treated as independent
semantic authorities when a tracked source exists. A source-less SVG is instead
an explicitly unverified lineage unit. Vendor, cache, environment, and ignored
runtime artifacts are outside the Git-based denominator.

Each covered unit receives separate values for:

- **authority** — normative, decision, explanatory, operational, historical,
  generated, evidence, or implementation;
- **lifecycle** — active, proposed, draft, accepted, superseded, archived,
  historical, missing, or unclear;
- **audit status** — verified, stale, conflicting, duplicate, unverified, or
  intentionally retained;
- **owning boundary** — the component or subsystem responsible for meaning.

These axes are deliberately independent. For example, an active normative claim
can be contradicted by current implementation, and a historical ADR can be both
verified and intentionally retained.

## Phase Gates

1. **Gate 1 — discovery:** review the complete corpus, authority matrix,
   terminology and claim inventory, conflicts, and blind spots.
2. **Gate 2 — calibration:** review the glossary, registry schema, proof model,
   and initial 10–15 guarantees before whole-system expansion.
3. **Subsystem gates — consolidation:** review each subsystem before deletion,
   archival, or a change to normative meaning.
4. **Final gate:** review residual risks, proof attestations, validation, and the
   clean campaign branch before any integration into `develop`.

Gate 1 was approved by the user on 2026-08-23, and Gate 2 was approved on
2026-08-24. During Phase 2A and Phase 2B, existing product semantics and
normative documentation remain read-only. Writes are limited to the proposed
glossary, assurance-index model, deterministic validation and human view,
classification and remediation records, and review packets. Product-semantic
discrepancies remain findings rather than silent repairs.

## Phase 1 Deliverables

- machine-readable coverage ledger and coverage summary;
- document authority and ownership matrix;
- terminology and deprecated-alias inventory;
- explicit and implicit guarantee-candidate inventory;
- implementation, constraint, test, and surface evidence map;
- exact code-owned and dynamic registry inventory;
- contradiction, ambiguity, duplication, and blind-spot findings;
- Gate 1 report with a precise accounted/unaccounted denominator.

Phase 1 is not complete merely because searches stop finding new material. It is
complete when every enumerated unit has an owner and explicit status, and every
remaining uncertainty is itself represented in the ledger.

## Phase 2A Deliverables

- `proposed-glossary.md` — 21 calibration-scoped term entries; 19 remain
  proposed and `QT-TERM-006`/`QT-TERM-012` remain blocked;
- `phase-2a-calibration.md` — the representative 12-record batch and its exact
  frozen-baseline authority, enforcement, proof, conflict, and environment
  findings;
- `../../assurance/guarantees/registry.json` — machine-readable provisional
  classifications with zero active guarantees;
- `../../assurance/guarantees/proof-catalog.json` — stable proof requirements
  without execution results;
- `../../assurance/guarantees/GUARANTEES.md` — generated human view;
- `../../assurance/guarantees/schemas/` and `attestations/README.md` — executable
  model documentation and commit-bound attestation semantics;
- `gate-2-report.md` — decision packet and explicit stop before Phase 2B.

Phase 2A completion does not adopt the glossary, activate a guarantee, classify
the remaining 63 candidates, settle a conflict, repair product semantics, or
authorize deletion/archive. Gate 2 approval authorized application of the
calibrated model; it did not authorize any of those other actions.

## Phase 2B Deliverables

- `phase-2b-work/batch-a.json` through `batch-d.json` — frozen-baseline
  classification working records for the remaining 63 candidates;
- `proposed-glossary.md` — all 55 Phase 1 term IDs accounted for as 34 proposed,
  2 blocked, 19 deferred, and 0 adopted;
- `../../assurance/guarantees/registry.json` — all 75 candidates classified,
  with 65 `partially_enforced`, 3 `contradicted`, 6 `candidate`, 1
  `implementation_property`, and 0 active guarantees;
- `../../assurance/guarantees/remediations/` — 68 concrete remediation records,
  one for every `partially_enforced` or `contradicted` guarantee;
- `../../assurance/guarantees/proof-catalog.json` — 85 proof definitions without
  execution results;
- `../../assurance/guarantees/GUARANTEES.md` — regenerated human view;
- `phase-2b-review-map.json` and `phase-2b-report.md` — the complete required
  review routing and human decision packet before Phase 3.

Phase 2B completion does not activate a guarantee, adopt terminology, resolve a
normative conflict, repair product semantics, change a contract, execute proof,
or authorize deletion, archival, consolidation, or integration into `develop`.

## Decision Resolution Deliverables

After Phase 2B was accepted, a separately authorized, bounded Decision
Resolution pass translated its machine-level review map into an owner-readable
system model:

- `decision-resolution-packet.md` explains how QT's authority and truth flows
  work, consolidates all 40 review routes exactly once into 15 resolutions, and
  distinguishes 5 derivable readings, 9 owner judgments, and 1
  execution/proof program;
- `decision-resolution-remediation-dispositions.md` proposes a disposition for
  all 68 remediation records without approving, editing, executing, or closing
  one; and
- `decision-resolution-proof-decisions.md` reduces all 9 proof ceilings to 5
  owner-facing choices while preserving exact ceiling and reviewer routing.

The pass also records, without implementing them, that the isolated-database
profile is underspecified for several proposed remediation plans and that
`QT-REM-311` would require a separately reviewed deployment-rehearsal profile.

These deliverables are non-normative review proposals. They do not change
product behavior, normative documents, adopted terminology, guarantees,
remediation state, proof results, or the frozen Phase 1/2 findings. Phase 3
was subsequently authorized in
`phase-3-authorization-and-plan.md`; that authorization does not itself change
any of those states.

## Phase 3 Authorization

`phase-3-authorization-and-plan.md` preserves the approved Decision Resolution
recommendations, the local-Promtail/native-server-Alloy clarification, proof
safety boundaries, required sequencing, bounded work packages, and the final
approval gate. Phase 3 may change approved normative and product artifacts in
that order, but it may not merge into `develop` or activate a guarantee without
the separately required final review.

### Phase 3 Forward Corrections

- [`phase-3-forward-corrections.md`](phase-3-forward-corrections.md) records
  approved forward-only locator lineage without editing the frozen audit
  artifacts. Its references are not proof results, remediation closures,
  attestations, guarantee reclassifications, or activations.

### Phase 3 Terminology Artifacts

- `phase-3-terminology-dispositions.json` is the non-normative, machine-readable
  decision ledger for all 55 term candidates and 20 alias findings.
- `phase-3-terminology-dispositions.md` is its generated readable audit view.
- `../../contracts/platform/04_glossary.md` is the normative vocabulary index
  for individually adopted terms and ratified alias rules.

The frozen proposal remains byte-for-byte historical evidence. Vocabulary
adoption remains separate from remediation, proof, attestation, guarantee
reclassification, and activation.
