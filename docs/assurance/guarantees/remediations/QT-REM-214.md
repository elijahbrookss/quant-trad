---
remediation_id: QT-REM-214
guarantee_ids: QT-GUAR-PROJECTOR-ONLY-SELECTED-SYMBOL-READS
lifecycle: proposed
owner: botlens-projections
required_reviewers: botlens-projections-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-214

**Close selected-symbol projector-read coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Selected-symbol bootstrap reads and unavailable state are represented, but the cited backend test at line 310 is unrelated and no closed inventory proves that every hot selected-symbol read avoids replay and history reconstruction.

## Action

Correct the evidence map, enumerate all selected-symbol hot-read entry points, and add a static dependency rule plus backend/frontend cases proving projector-only reads and explicit unavailable state.

## Acceptance criteria

- Every admitted selected-symbol hot-read endpoint uses current projector snapshots and never reconstructs durable history on the hot path.
- Missing projection state is returned and displayed as explicit unavailable state without fabrication.
- The evidence map contains only exact relevant selectors and a closed read-entrypoint denominator.

## Proof plan

Required proof definitions: `QT-PROOF-214`, `QT-PROOF-215`.

- Additional evidence: A reviewed selected-symbol hot-read entry-point and dependency inventory.

## Review boundary

BotLens-projections review owns the hot-read denominator; the draft corrects evidence mapping but does not change product behavior.
