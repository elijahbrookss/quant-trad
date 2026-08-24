---
remediation_id: QT-REM-219
guarantee_ids: QT-GUAR-BOT-RUN-CONTAINER-IDENTITY-SEPARATION
lifecycle: proposed
owner: bot-control-plane
required_reviewers: bot-control-plane-owner,botlens-projections-owner,frontend-owner,persistence-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-219

**Close Bot, BotRun, and container identity coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Structural models and representative control/frontend tests separate definitions and run instances, but the complete creation, restart, stop, persistence, container, API, and projection identity denominator is unproved.

## Action

Review the identity model, enumerate all definition/run/container mappings and operations, and add database, control-plane, transport, and frontend tests for sibling runs, exact stop, restart, terminal exclusion, and cross-run isolation.

## Acceptance criteria

- Bot definition identity, BotRun identity, and container/process identity remain distinct on every admitted path.
- Start, restart, and stop operations target an exact reviewed identity and cannot mutate a sibling run.
- Terminal runs remain durable inventory without appearing as current live-run projections.

## Proof plan

Required proof definitions: `QT-PROOF-224`, `QT-PROOF-225`.

- Additional evidence: A reviewed definition/run/container identity and operation crosswalk.

## Review boundary

Control-plane, persistence, BotLens, and frontend reviewers own identity and operation scope; no lifecycle semantics are changed here.
