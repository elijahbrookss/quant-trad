---
remediation_id: QT-REM-405
guarantee_ids: QT-GUAR-FENCED-IDEMPOTENT-SCHEDULED-COLLECTION
lifecycle: proposed
owner: collection-runtime
required_reviewers: collection-owner,persistence-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-405

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

One isolated repository test and service fakes cover key fencing and gap cases, but the full scheduled collector family, retry matrix, and missed-schedule idempotency denominator remain open.

## Action

Inventory all scheduled collection handlers and add isolated PostgreSQL race, retry, duplicate, missed-schedule, and exhausted-failure tests for each classified family.

## Acceptance criteria

- Every scheduled collector handler carries the current claim fence into accepted writes.
- Stale owners cannot complete, fail, or persist collection effects.
- Duplicate attempts and replays do not create a second effective accepted fact.
- Missed schedules and exhausted failures append explicit gap evidence rather than successful coverage.

## Proof plan

Required proof definitions: `QT-PROOF-405`.

Required environment profile: `python-db-isolated`.

Run against a fresh isolated PostgreSQL service after the handler inventory is reviewed; this proof definition is not a result.

## Review boundary

Collection and persistence owners review retry and handler scope; the remediation does not resolve collector terminology or change scheduling semantics.
