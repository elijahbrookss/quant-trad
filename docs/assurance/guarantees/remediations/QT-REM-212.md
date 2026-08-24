---
remediation_id: QT-REM-212
guarantee_ids: QT-GUAR-CANONICAL-FILL-ACCOUNTING-RECONCILIATION
lifecycle: proposed
owner: accounting
required_reviewers: accounting-owner,execution-runtime-owner,reporting-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-212

**Close canonical fill-accounting reconciliation**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Representative settlement cases reconcile positions, margin, and wallet state, but no reviewed invariant spans all instruments, fee currencies, partial exits, terminal outcomes, and reporting projections.

## Action

Define the canonical fill-accounting invariant and complete event-family denominator, then add replay and reconciliation tests across partial/terminal settlement, margin release, fees, sparse instrument payloads, and projections.

## Acceptance criteria

- Canonical fill events reconcile executed quantity, position state, wallet balances, fees, and locked margin for every admitted instrument family.
- Terminal settlement releases all reviewed residual margin exactly once.
- Replay and reporting projections derive the same accounting result from the same ordered fill evidence.

## Proof plan

Required proof definitions: `QT-PROOF-212`.

- Additional evidence: A reviewed accounting invariant and instrument/fee-currency settlement matrix.

## Review boundary

Accounting, execution-runtime, and reporting reviewers own the invariant; classification cannot repair accounting semantics.
