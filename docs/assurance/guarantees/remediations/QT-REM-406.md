---
remediation_id: QT-REM-406
guarantee_ids: QT-GUAR-DURABLE-VERIFIED-RAW-ARCHIVE
lifecycle: proposed
owner: raw-archive
required_reviewers: data-owner,raw-archive-owner,storage-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-406

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The local archive tests exercise fsync, sealing, acknowledgement, recovery, and replay, but no reviewed cross-process crash and object-store matrix closes durability across every configured archive backend, and DOC-MARKET-STRUCTURE-001 remains open.

## Action

Define the supported archive-backend and crash-boundary matrix, add isolated durability and immutable-object verification rehearsals, and separately submit the explanatory lifecycle wording for data-owner review.

## Acceptance criteria

- Every canonicalized frame is traceable to fsynced raw bytes.
- Acknowledgement is withheld until immutable object bytes are verified.
- Crash and retry at each reviewed boundary preserve exact replayable order and content.
- The data-plane lifecycle wording is reconciled only through separate owner approval.

## Proof plan

Required proof definitions: `QT-PROOF-406`.

Required environment profile: `python-nondb`.

Run the supported crash matrix after owner review with typed filesystem and object evidence; this definition is not a result.

## Review boundary

Raw-archive, storage, and data owners review durability and DOC-MARKET-STRUCTURE-001; P1-C01 remains only a finding alias and no product or normative repair is authorized.
