---
remediation_id: QT-REM-204
guarantee_ids: QT-GUAR-POST-ONLY-SIGNAL-BAR-CAUSALITY
lifecycle: proposed
owner: execution-runtime
required_reviewers: execution-runtime-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-204

**Close post-only signal-bar causality coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Named tests protect limit-maker orders from using the signal bar's range and reject marketable post-only submission, but the Phase 1 wording was broader than the active contract, which permits an immediate market entry at signal close.

## Action

Review and retain the narrow resting limit-maker scope, enumerate every post-only entry adapter, and add boundary tests for signal-close submission, later-bar eligibility, cancellation, and marketable-cross behavior.

## Acceptance criteria

- The reviewed claim excludes immediate market entry permitted by the active contract.
- No admitted resting limit-maker path fills from the signal bar's intrabar range.
- Every marketable post-only request follows the reviewed canceled-or-rejected behavior.

## Proof plan

Required proof definitions: `QT-PROOF-204`.

- Additional evidence: A reviewed post-only adapter inventory and parameterized causal-boundary matrix.

## Review boundary

Execution-runtime review must approve the narrowed scope; this draft does not change entry semantics or resolve a normative conflict.
