---
remediation_id: QT-REM-209
guarantee_ids: QT-GUAR-FILL-SETTLEMENT-SINGLE-INGRESS
lifecycle: proposed
owner: accounting
required_reviewers: accounting-owner,execution-runtime-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-209

**Close fill-settlement ingress coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Representative entry and wallet settlement paths are idempotent, but there is no closed inventory proving that every fill mutation crosses the canonical settlement ingress exactly once.

## Action

Inventory all position, wallet, fee, and reporting mutations caused by fills; require a canonical settlement event at each entry point; and add duplicate, divergence, partial-fill, and bypass tests.

## Acceptance criteria

- Every admitted fill-driven state mutation is reachable only through the reviewed canonical settlement ingress.
- Reapplying an identical settled event is idempotent and divergent reuse fails closed.
- Partial fills accumulate exact executed quantity and mutate wallet and position once per event.

## Proof plan

Required proof definitions: `QT-PROOF-209`.

- Additional evidence: A reviewed fill-mutation call-site inventory and bypass-prevention rule.

## Review boundary

Accounting and execution-runtime reviewers own the settlement denominator; this draft does not change product accounting.
