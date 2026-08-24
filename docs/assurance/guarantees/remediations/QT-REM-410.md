---
remediation_id: QT-REM-410
guarantee_ids: QT-GUAR-PROVEN-ZERO-TRADE-COVERAGE
lifecycle: proposed
owner: market-structure-coverage
required_reviewers: data-owner,documentation-assurance-owner,market-structure-owner,persistence-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-410

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Domain and repository tests cover the core zero-trade rule, but the full exact-scope coverage matrix and persisted watermark lineage remain incomplete, while the Phase 1 candidate points to unrelated data-plane lines 380 and 1051.

## Action

Review DOC-CANDIDATE-LOCATOR-001 without editing Phase 1 in this batch, approve the exact coverage denominator, and add isolated persistence tests for product, channel, range, gap, archive, ordering, and canonicalization states.

## Acceptance criteria

- Only exact product, channel, and bucket coverage can support a complete zero-trade aggregate.
- Pending archive, intersecting gap, invalid ordering, and lagging canonicalization cases remain incomplete.
- Persisted coverage and aggregate evidence bind immutable coverage revision and watermarks.
- The candidate-locator discrepancy is dispositioned through documentation-assurance and data-owner review rather than silently rewritten.

## Proof plan

Required proof definitions: `QT-PROOF-410`.

Required environment profile: `python-db-isolated`.

Run against a fresh isolated PostgreSQL service after the locator and scope reviews; this proof definition is not a result.

## Review boundary

Data, documentation-assurance, market-structure, and persistence owners review scope and references; P1-C01 is not remediation and no Phase 1 or product semantics are silently repaired.
