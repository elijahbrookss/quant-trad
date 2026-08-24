---
remediation_id: QT-REM-201
guarantee_ids: QT-GUAR-STRATEGY-INDEPENDENT-EXECUTION-ECONOMICS
lifecycle: proposed
owner: execution-runtime
required_reviewers: execution-runtime-owner,instruments-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-201

**Close execution-economics ownership coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Accepted decisions and representative tests separate strategy logic from fees, increments, rounding, and venue capabilities, but no closed inventory proves that every execution path consumes the resolved context rather than strategy-owned economics.

## Action

Review the profile-versus-resolved-context ownership wording, enumerate every execution-economics consumer, and add a static or generated rule plus representative conformance tests for uncovered consumers.

## Acceptance criteria

- The reviewed model names Instrument Execution Profile as an input/compiler source and Resolved Execution Context as immutable run-scoped authority.
- Every admitted economics consumer is mapped to the resolved context or an explicitly reviewed compatibility boundary.
- Strategy code cannot select venue identity, fee schedules, increments, or currency rounding.

## Proof plan

Required proof definitions: `QT-PROOF-201`.

- Additional evidence: A reviewed execution-economics consumer inventory and static dependency check.

## Review boundary

Execution-runtime and instruments reviewers must settle the ownership wording before it can become normative; tests remain evidence only.
