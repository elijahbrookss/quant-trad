---
remediation_id: QT-REM-205
guarantee_ids: QT-GUAR-PROTECTIVE-EXIT-RESIDUAL-TERMINAL-INTEGRITY
lifecycle: proposed
owner: execution-runtime
required_reviewers: execution-runtime-owner,positions-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-205

**Close protective-exit lifecycle and residual coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Representative partial, rejected, and trailing-stop paths preserve residual state, but the proof denominator does not cover every protective exit, race, adapter outcome, and terminal projection combination.

## Action

Define a reviewed protective-exit lifecycle matrix and add fault/race tests for partial fills, rejection, cancellation, replacement, duplicate delivery, and terminal evidence across all admitted adapters.

## Acceptance criteria

- Every nonterminal outcome preserves the exact residual position and canonical order lineage.
- Terminal position metadata is emitted only after terminal fill evidence settles.
- Protective prices only move in the reviewed risk-tightening direction.

## Proof plan

Required proof definitions: `QT-PROOF-205`.

- Additional evidence: A reviewed protective-exit outcome and race matrix with exact selectors.

## Review boundary

Execution-runtime and positions owners must approve the lifecycle denominator; no new stop or liquidation semantics are adopted here.
