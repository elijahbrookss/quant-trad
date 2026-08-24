---
remediation_id: QT-REM-310
guarantee_ids: QT-GUAR-BOUNDED-TELEMETRY-CONTROL-DELIVERY
lifecycle: proposed
owner: runtime-telemetry
required_reviewers: botlens-owner,execution-runtime-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-310

**Close telemetry transport failure-matrix coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Representative tests cover control delivery, terminal flush, duplicate suppression, and direct fallback, but no reviewed matrix closes queue saturation, shutdown races, retry, and repeated fallback outcomes.

## Action

Define the bounded telemetry failure matrix and add deterministic tests for each reviewed queue, flush, deduplication, and fallback state.

## Acceptance criteria

- Control-lane capacity and priority behavior are tested under saturation.
- Duplicate suppression is scoped to the reviewed session and payload identity rules.
- Terminal flush and fallback failures remain visible across shutdown and retry races.

## Proof plan

- Add deterministic scheduler fixtures for saturation and shutdown races.
- Bind fallback evidence to the caller path at portal/backend/service/bots/container_runtime.py.
- Retain QT-PROOF-310 as the representative behavior set.

## Review boundary

BotLens and execution-runtime owners review transport and deduplication expectations; this draft does not create delivery guarantees beyond ADR 0026.
