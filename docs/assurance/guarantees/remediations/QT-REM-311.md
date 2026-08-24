---
remediation_id: QT-REM-311
guarantee_ids: QT-GUAR-ATTESTED-SINGLE-NODE-DEPLOYMENT
lifecycle: proposed
owner: deployment
required_reviewers: deployment-owner,operations-owner,security-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-311

**Close deployment workflow execution evidence**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The selected static tests inspect the script and workflow but do not execute a clean reviewed deployment, image-attestation mismatch, migration prohibition, or rollback-state recovery scenario.

## Action

Create an isolated deployment rehearsal with immutable source, rendered-image, command, migration, and rollback evidence, subject to operations and security review.

## Acceptance criteria

- A dirty or mismatched source revision is rejected before rollout.
- Every rendered application image is verified and a mismatch blocks rollout.
- No implicit migration occurs and rollback state is recorded and usable in the isolated rehearsal.

## Proof plan

- Build an isolated Compose deployment fixture with fake registry and rollback targets.
- Capture typed source, image, command, and rollback evidence artifacts.
- Retain QT-PROOF-311 as the static prerequisite.

## Review boundary

Deployment, operations, and security owners review the rehearsal and evidence; no live server action or external execution authority is authorized.
