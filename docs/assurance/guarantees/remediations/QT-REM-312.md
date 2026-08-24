---
remediation_id: QT-REM-312
guarantee_ids: QT-GUAR-SEMANTIC-OPERATIONAL-FINGERPRINT-SEPARATION
lifecycle: proposed
owner: reporting
required_reviewers: reporting-owner,runtime-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-312

**Close fingerprint input-surface coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The selected tests cover key ordering, context, and run-identifier cases but no closed field inventory proves that all semantic and operational inputs are assigned to the correct fingerprint.

## Action

Generate and review the fingerprint input projection inventory, then mutation-test every classified semantic and operational field family.

## Acceptance criteria

- Every input field to each fingerprint is present in the reviewed projection inventory.
- Operational-only field mutations preserve semantic identity while changing operational identity where specified.
- Semantically material field mutations change semantic identity.

## Proof plan

- Generate the field-to-fingerprint projection inventory from the implementation.
- Add table-driven field mutation tests for both projections.
- Retain QT-PROOF-312 as representative behavior coverage.

## Review boundary

Reporting and runtime owners review semantic materiality and operational classification; this draft does not resolve QT-CONFLICT-024 terminology.
