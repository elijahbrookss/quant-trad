---
remediation_id: QT-REM-220
guarantee_ids: QT-GUAR-TRADE-MARKER-CAUSAL-CANDLE-PROJECTION
lifecycle: proposed
owner: frontend
required_reviewers: botlens-projections-owner,frontend-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-220

**Close trade-marker projection proof and environment coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The accepted decision and Node tests cover exact, containing-candle, gap, and outside-window projection, but the loaded-window spacing assumptions and frontend runner cannot support a full automated PASS under attestation v1.

## Action

Review the projection-domain assumptions, add property and boundary cases across variable spacing, gaps, duplicate times, window edges, and evidence identity, and provide an admitted PASS-capable proof path before any activation request.

## Acceptance criteria

- Exact and containing-candle projection retain stable original evidence identity and time.
- Gaps, irregular spacing, duplicate candle times, and outside-window events never trigger nearest-candle snapping or fabrication.
- A later reviewed proof environment can produce admissible evidence for every required case.

## Proof plan

Required proof definitions: `QT-PROOF-227`.

- Additional evidence: A reviewed projection-domain matrix and future PASS-capable proof definition; the current Node proof remains requirements-only and non-PASS.

## Review boundary

Frontend and BotLens-projections reviewers own the candle-projection domain; this draft neither changes mapping semantics nor claims a Node PASS.
