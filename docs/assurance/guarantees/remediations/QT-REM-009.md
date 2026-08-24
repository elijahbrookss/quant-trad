---
remediation_id: QT-REM-009
guarantee_ids: QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION
lifecycle: proposed
owner: recovery
required_reviewers: data-retention-owner,database-operations-owner,recovery-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-009

This proposal defines missing recovery evidence only. It authorizes no
destructive operation and no production database may be used to satisfy it.

## Gap

Archive checksum and retention-pin deletion guards are tested, but the active
deployment procedure does not have an executed isolated backup/restore rehearsal
bound to the guarantee. Real database recovery therefore remains manual,
environment-dependent, and unproved.

## Action

After data-retention, database-operations, and recovery-owner review, define an
isolated restore rehearsal with a disposable source and target, explicit backup
identity, checksum verification, restored-schema/data checks, and negative
destructive-operation cases.

## Acceptance criteria

- The procedure cannot target production and requires independently identified
  disposable source and restore environments.
- Backup identity and checksums are verified before destructive replacement.
- Restored schema and representative durable data are validated before the
  rehearsal is considered complete.
- Archive deletion remains blocked by checksum mismatch or an active retention
  pin.

## Proof plan

Run the automated archive guards plus the reviewed manual rehearsal in the
`manual-recovery` profile. Capture commands, identities, checksums, validation
evidence, reviewer identity, and timing in a later attestation; this record
contains no result.

## Review boundary

Data-retention owner, database-operations owner, and recovery owner.
