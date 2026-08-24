---
remediation_id: QT-REM-409
guarantee_ids: QT-GUAR-CODE-OWNED-AUDITED-COLLECTOR-CONTROL
lifecycle: proposed
owner: collector-control-plane
required_reviewers: collection-owner,operations-owner,security-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-409

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Manifest and service tests cover code ownership, confirmation, preconditions, and replay, but immutable database audit persistence and the full enrollment-to-operation surface are not proved end to end, and DOC-MARKET-STRUCTURE-001 remains open.

## Action

Inventory all definition, enrollment, and mutation surfaces and add isolated PostgreSQL proofs for immutable operation events, conflicting request reuse, precondition failure, and idempotent replay.

## Acceptance criteria

- Every executable collector behavior originates in deployed code or a reviewed immutable manifest.
- Every mutating operation is confirmed, preconditioned, and recorded with immutable prior and resulting state.
- Reused request identifiers are idempotent for identical intent and reject conflicting intent.
- The collector operation inventory has no unowned mutation path.

## Proof plan

Required proof definitions: `QT-PROOF-409`.

Required environment profile: `python-nondb`.

Add and run an isolated database proof after the operation denominator is approved; the current proof definition is not a result.

## Review boundary

Collection, operations, and security owners review authority and audit behavior; P1-C01 remains finding-only and product semantics are unchanged.
