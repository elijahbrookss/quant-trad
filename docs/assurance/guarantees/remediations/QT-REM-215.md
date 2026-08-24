---
remediation_id: QT-REM-215
guarantee_ids: QT-GUAR-BOTLENS-CURSOR-LINEAGE
lifecycle: proposed
owner: botlens-transport
required_reviewers: botlens-transport-owner,frontend-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-215

**Close BotLens cursor-lineage coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Resume replay, stale-window bootstrap, selected-symbol handoff, and stale overlay rejection are tested, but no reviewed denominator covers every delta family, reconnect race, cursor expiry, and projection lineage.

## Action

Enumerate all streamed delta families and cursor transitions, define the valid lineage state machine, and add backend/frontend race tests for disconnect, replay, expiry, bootstrap handoff, stale commit, and duplicate delivery.

## Acceptance criteria

- Every admitted delta family carries and validates the reviewed run/cursor lineage.
- Expired or unavailable replay windows force one fresh bootstrap before later deltas are applied.
- Stale, duplicate, cross-run, and out-of-lineage deltas cannot overwrite newer projection state.

## Proof plan

Required proof definitions: `QT-PROOF-216`, `QT-PROOF-217`.

- Additional evidence: A reviewed cursor-lineage state machine and delta-family inventory.

## Review boundary

BotLens-transport and frontend reviewers own cursor semantics; Node evidence remains non-PASS under attestation v1.
