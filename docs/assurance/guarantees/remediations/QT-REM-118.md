---
remediation_id: QT-REM-118
guarantee_ids: QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION
lifecycle: proposed
owner: decision-layer
required_reviewers: decision-layer-owner,execution-runtime-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-118

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The named evaluator and domain-event tests cover representative accepted and rejected decisions, but they do not close every decision producer, stable identity field, rejection-reason variant, or downstream execution boundary.

## Action

After decision-layer and execution-runtime review, define the owned decision-artifact schema and producer denominator, then add property tests for typed identity and rejection information plus negative tests proving that decision construction cannot perform or masquerade as execution.

## Acceptance criteria

- The reviewed denominator enumerates every accepted and rejected decision producer.
- Every enumerated artifact satisfies its owner-approved typed identity and rejection-information requirements.
- Negative tests establish separation between decision construction and every admitted execution boundary.
- Decision-layer, execution-runtime, and testing reviewers approve the scope before any disposition or activation change.

## Proof plan

Required proof definitions: `QT-PROOF-121`.

Required environment profile: `python-nondb`.

Run only after owner and reviewer approval; retain a clean commit-bound result and the reviewed producer/schema denominator. This proof definition is not a result.

## Review boundary

Classification proposal only; it does not resolve identity terminology, change decision or execution semantics, adopt glossary entries, or activate the guarantee.
