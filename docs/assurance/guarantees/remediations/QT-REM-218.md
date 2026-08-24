---
remediation_id: QT-REM-218
guarantee_ids: QT-GUAR-OVERLAY-COMPLETENESS-ISOLATION
lifecycle: proposed
owner: botlens-overlays
required_reviewers: botlens-projections-owner,frontend-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-218

**Close overlay completeness isolation coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Terminal checkpoints, clock gaps, truncation, and frontend readiness isolation are represented, but every overlay family, pagination boundary, and recovery transition is not covered by a reviewed completeness model.

## Action

Define the overlay-completeness state model and family denominator, then add backend/frontend cases for clock gaps, missing checkpoints, truncation, pagination, stale commits, recovery, and independent candle/trade readiness.

## Acceptance criteria

- Overlay incompleteness never invalidates otherwise valid candle or trade history readiness.
- No overlay family claims complete geometry without the reviewed checkpoint, clock, and payload evidence.
- Every pagination and recovery transition preserves an explicit completeness reason and lineage.

## Proof plan

Required proof definitions: `QT-PROOF-222`, `QT-PROOF-223`.

- Additional evidence: A reviewed overlay-family and completeness-state matrix.

## Review boundary

BotLens-projections and frontend reviewers own overlay completeness semantics; runner and overlay gap terminology remains proposed.
