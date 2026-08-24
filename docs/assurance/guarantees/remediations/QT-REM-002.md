---
remediation_id: QT-REM-002
guarantee_ids: QT-GUAR-CANONICAL-FACT-APPEND-ONLY
lifecycle: proposed
owner: database
required_reviewers: database-owner,market-data-owner,persistence-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-002

This proposal records missing assurance work only. It does not choose a new
schema owner or change Canonical Fact correction behavior.

## Gap

The frozen schema, triggers, and repository path align with append-only Fact
revisions, but the mapped proof does not directly exercise delete rejection or
concurrent revision allocation in an isolated PostgreSQL/Timescale database.
`SCHEMA-AUTH-001` also leaves schema ownership distributed.

## Action

Have database, market-data, and persistence owners review the authoritative
schema surface, then add isolated database contract cases for concurrent
corrections and rejected update/delete operations. Capture the relevant
constraint, function, and trigger fingerprint with the proof environment.

## Acceptance criteria

- A reviewed schema-ownership map identifies the enforcement source for each
  append-only rule exercised by the proof.
- Concurrent corrections allocate distinct monotonic revisions under one
  logical observation identity.
- Direct update and delete attempts fail without altering existing revisions.
- Successful correction remains INSERT-only and retains the prior revision.

## Proof plan

Bootstrap a disposable PostgreSQL/Timescale database, exercise concurrent
allocation plus update/delete rejection, and capture schema fingerprints and
pytest results in a commit-bound `python-db-isolated` attestation. This proposal
does not attest that run.

## Review boundary

Database owner, market-data owner, and persistence owner.
