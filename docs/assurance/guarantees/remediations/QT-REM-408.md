---
remediation_id: QT-REM-408
guarantee_ids: QT-GUAR-INTERVAL-VALID-ORDER-BOOK-TRUTH
lifecycle: proposed
owner: market-structure
required_reviewers: data-owner,market-structure-owner,persistence-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-408

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Reducer and property tests cover representative invalidation and replay equivalence, but no isolated persistence proof closes validity-version, checkpoint, restart, and causal source-position behavior, and DOC-MARKET-STRUCTURE-001 remains open.

## Action

Approve the complete order-book state transition matrix, add isolated repository restart and checkpoint proofs, and separately reconcile explanatory lifecycle wording.

## Acceptance criteria

- Every update-before-snapshot and sequence-gap transition suppresses valid publication until a fresh snapshot.
- Validity interval and checkpoint writes are atomic and restart-safe.
- Full replay and checkpoint-plus-delta replay match across the reviewed randomized cut matrix.
- The explanatory lifecycle conflict is resolved only by data-owner review.

## Proof plan

Required proof definitions: `QT-PROOF-408`.

Required environment profile: `python-nondb`.

Extend the definition with isolated persistence proof after the transition denominator is approved; this proof definition alone is not a result.

## Review boundary

Data, market-structure, and persistence owners review validity and replay scope; P1-C01 is not remediation and deferred terminology remains unadopted.
