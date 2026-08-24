---
remediation_id: QT-REM-407
guarantee_ids: QT-GUAR-PIN-SAFE-MARKET-DATA-LIFECYCLE
lifecycle: proposed
owner: market-storage-lifecycle
required_reviewers: data-owner,operations-owner,storage-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-407

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Planning tests cover bounds, dry-run defaults, and pins, but no end-to-end lifecycle proof closes pin appearance races, compaction replacement expiry, evidence unavailability, and the prohibition on derived fallback.

## Action

Build an isolated lifecycle rehearsal over disposable archive objects and checkpoints, including concurrent pin changes and expired replacement paths, and separately review the data-plane lifecycle wording.

## Acceptance criteria

- A pin added after planning blocks deletion at execution.
- Expired or missing evidence is reported explicitly unavailable.
- Lifecycle execution remains bounded, policy-gated, and dry-run by default.
- No derived fallback substitutes for unavailable source evidence.

## Proof plan

Required proof definitions: `QT-PROOF-407`.

Required environment profile: `python-nondb`.

Run the isolated lifecycle rehearsal after policy and backend owners approve the matrix; this proof definition is not a result.

## Review boundary

Data, storage, and operations owners review retention and unavailability behavior; P1-C01 remains finding-only and no deletion policy changes are authorized.
