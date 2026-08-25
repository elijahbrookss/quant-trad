# Phase 3 Authorization And Execution Plan

## Authorization Record

Phase 3 was authorized by the repository owner on 2026-08-24 against the
accepted Decision Resolution packet on branch
`feat/docs-guarantee-reconciliation`.

The authorization approves:

- the recommended resolution for `DRR-01` through `DRR-15`;
- `PDR-01` through `PDR-05`;
- all 68 remediation dispositions as planning inputs rather than instructions
  to execute every remediation mechanically; and
- preservation of the frozen-baseline audit trail and historical evidence.

The owner supplied this binding clarification for `DRR-14`:

- Promtail remains supported for local development;
- Alloy is the supported native-server shipper; and
- every supported topology retains exactly one out-of-process Loki shipper and
  no in-process application hot-path shipping.

This file records authorization and execution boundaries. It is not a platform
contract, a proof result, an attestation, a remediation closure, a guarantee
activation decision, or permission to merge into `develop`.

## Frozen Subject And Working State

- Frozen audit subject: `origin/develop` at
  `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`.
- Phase 3 working branch: `feat/docs-guarantee-reconciliation`.
- Phase 3 starts after Decision Resolution commit
  `7e2561eb34594bc8fcdcb5df56452c33e6337c57`.
- Frozen Phase 1 and Phase 2 evidence remains unchanged. Forward corrections
  must identify their current commit and retain the original locator.

## Required Order

Cross-system work follows this order:

```text
approved meaning
    -> normative reconciliation
    -> terminology
    -> documentation
    -> enforcement
    -> proof
    -> attestation
    -> guarantee reclassification
    -> final residual-risk review
```

Within one subsystem the order is:

```text
meaning -> docs -> enforcement -> proof -> attestation
```

No later layer is evidence that an earlier layer was completed. In particular,
a remediation success or passing test does not activate a guarantee.

## Safety And Evidence Rules

- External exchange-order submission remains closed. Phase 3 must not enable,
  exercise, or simulate authorization for an external submission path.
- Production and live systems must not be used to obtain proof.
- Database, frontend, recovery, and deployment proof may run only in the
  approved isolated profiles defined by `PDR-02` through `PDR-04` and any
  separately validated profile contract.
- A missing prerequisite is recorded as `UNAVAILABLE`; incomplete coverage is
  `PARTIAL`; an unattempted proof is `NOT_RUN`. These states must not be hidden
  or converted into PASS.
- Proof definitions remain distinct from proof results. Attestations are
  immutable and commit-specific.
- Guarantee reclassification is distinct from activation. Activation requires
  its own final reviewed decision and attestation bindings.
- Historical ADRs, incidents, campaign records, frozen line locators, and
  superseded evidence remain available with explicit forward lineage.
- Changes are committed in bounded subsystem-sized slices. `develop` remains
  untouched until the final gate is approved.

## Bounded Work Packages

### P3-01 — Authority, ownership, and normative reconciliation

Implement approved `DRR-01`, `DRR-02`, `DRR-04`, `DRR-07`, `DRR-10`, and
`DRR-11` readings in the existing authority hierarchy. Establish semantic
ownership/reviewer metadata and reviewed module-contract discovery without
turning campaign files, `CODEOWNERS`, or `AGENTS.md` into product authority.

### P3-02 — Terminology and semantic documentation

Apply `DRR-15` entry by entry after its owning meaning is reconciled. Adopt only
qualified terms supported by the approved authority, preserve explicit aliases
and historical spellings, and leave any still-unresolved entry deferred.
Reconcile the data, research, execution, and market-structure documentation
covered by `DRR-05` through `DRR-10` without rewriting frozen audit artifacts.

### P3-03 — Repository documentation and supported operating surfaces

Implement `DRR-12` through `DRR-14`: indexes and lifecycle labels, truthful CI
topology, QT-specific frontend guidance and supported tests, current component
navigation, the retained-unverified asset disposition, local Promtail/native
server Alloy topology, and an accurate Grafana backup/restore workflow.

### P3-04 — Validation and assurance mechanics

Implement `DRR-03` and `PDR-01` through `PDR-05`: closed-denominator
validation, deterministic Node result semantics, approved profile constraints,
side-effect-free collection, recovery/deployment profile reporting, durable
locator lineage, `verify-guarantees`, and immutable attestation generation.

### P3-05 — Prioritized enforcement slices

Select remediation work by causal value and proof feasibility rather than by
record number. Prefer shared enforcement foundations, security-critical closed
boundaries, and gaps required by multiple proofs. Keep expensive exhaustive
matrices and activation-priority work visible as residual risk when they are
not justified by the Phase 3 slice.

### P3-06 — Isolated verification, attestations, and classification

Run every available approved proof at a clean source commit. Record PASS, FAIL,
PARTIAL, MANUAL, NOT_RUN, and UNAVAILABLE results exactly. Create immutable
attestations, then reclassify each guarantee from authority, enforcement, and
proof evidence without activating it automatically.

## Final Gate Deliverables

Phase 3 stops with:

- the resulting state of all 75 guarantees, separately reporting activation;
- proof and attestation results, including every unresolved or unavailable
  result;
- remediation outcomes and residual risks;
- validation results and the exact clean branch commit;
- a proposed integration plan; and
- an explicit request for final approval before any merge into `develop`.

### Final packet identity

The final packet has two identities that must not be conflated:

1. proof source commit `S`, whose registry, proof catalog, and Phase 3 policy
   bytes define the assessment and whose exact identity is shared by every
   bound attestation; and
2. clean packet-input commit and tree `P`, which already contains the immutable
   attestations, evidence, and sorted validation-result document used to render
   the packet.

The rendered JSON and Markdown are then committed alone as packet commit `C`.
A checked-in file cannot truthfully contain the hash of its own future commit,
so the packet does not claim that `C` equals an embedded `HEAD`. The strict
external final-gate check instead verifies that the current worktree is clean,
the branch is the recorded feature branch, `C` has the single parent `P`, only
the generated review pair changed in `C`, and `develop` is still the recorded
commit. The exact `C` commit/tree and any post-commit validation results are
reported to the owner outside the self-excluding packet at the approval gate.

The ordinary review check remains permissive for intermediate and historical
pre-attestation packets. Only the explicit strict final-gate check requires a
nonempty sorted set of same-source attestations, an explicit result for all 85
active proof definitions, the immutable sorted validation-result document, the
repository evidence above, and the integration approval request. Neither mode
activates a guarantee.
