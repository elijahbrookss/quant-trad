---
remediation_id: QT-REM-302
guarantee_ids: QT-GUAR-BOT-RUN-LEASE-OWNERSHIP
lifecycle: proposed
owner: execution-persistence
required_reviewers: execution-runtime-owner,persistence-owner,security-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-302

**Close Bot Run lease database coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The selected tests establish service wiring and representative contention behavior but do not directly cover the persisted token hash, renewal race, mismatch rejection, expiry, reclaim, and post-loss mutation matrix in PostgreSQL.

## Action

Define an isolated PostgreSQL lease-state proof that covers the full ownership transition matrix while preserving caller ownership of raw tokens.

## Acceptance criteria

- Concurrent acquisition and renewal races preserve one effective owner.
- Mismatch, expiry, reclaim, and post-loss mutation attempts are rejected.
- Persisted lease rows contain only the expected token digest and never the raw token.

## Proof plan

- Add isolated database selectors for acquisition, renewal, expiry, reclaim, release, and stale-owner mutation.
- Capture persisted-row evidence for token-hash confinement.
- Retain QT-PROOF-302 as supporting service-boundary evidence.

## Review boundary

Execution-runtime, persistence, and security owners review transition and token-handling expectations; this draft does not redefine lease semantics.
