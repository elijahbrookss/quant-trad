---
remediation_id: QT-REM-004
guarantee_ids: QT-GUAR-CHECK-OBSERVATION-ADMISSION
lifecycle: proposed
owner: research-memory
required_reviewers: normative-contract-reviewer,research-memory-owner,research-orchestration-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-004

This record preserves the Check-to-Observation conflict. It does not select an
authority, adopt the blocked terms, or repair either implementation seam.

## Gap

Accepted ADR 0034 and accepted ADR 0062 make incompatible statements about
when a Check creates or links a Research Observation, while legacy and V2
persistence paths coexist. `QT-CONFLICT-007` and the blocked meanings in
`QT-TERM-006` and `QT-TERM-012` prevent a single current admission rule.

## Action

Convene research-memory, research-orchestration, and normative-contract review
to choose explicit authority and lifecycle treatment for both accepted ADRs
and both implementation seams. Only after that decision may owners reconcile
contracts, explanatory documentation, implementation, or terminology.

## Acceptance criteria

- A reviewed decision explicitly states the current Check-to-Observation
  admission rule and the status of older authority.
- Legacy and V2 behavior receive an intentional retain, migrate, or supersede
  disposition with no silent fallback.
- The decision states preview, incomplete, blocked, and replay-ineligible
  behavior as well as the positive eligible path.
- Terminology adoption remains separately reviewed after the normative conflict
  is settled.

## Proof plan

After the decision, map positive eligible evidence-to-Observation creation and
support linkage plus negative preview, incomplete, blocked, legacy, and
provider-backed cases. Execute only against the exact approved semantics and
retain the result in a later attestation; no current selector proves a chosen
winner.

## Review boundary

Normative-contract reviewer, research-memory owner, and
research-orchestration owner.
