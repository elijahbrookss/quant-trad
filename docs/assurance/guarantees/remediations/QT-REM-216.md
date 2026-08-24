---
remediation_id: QT-REM-216
guarantee_ids: QT-GUAR-BOTLENS-TYPED-READINESS
lifecycle: proposed
owner: botlens-projections
required_reviewers: botlens-projections-owner,frontend-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-216

**Close typed BotLens readiness coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Representative bootstrap and terminal-unavailable cases separate runtime and retrieval ownership, but readiness states and transitions are not exhaustively modeled across run lifecycle, symbol selection, projection gaps, and history retrieval.

## Action

Define the reviewed readiness state machine and ownership of each transition, then add backend/frontend matrix tests for bootstrap, live, terminal, unavailable, incomplete, stale, and recovery states.

## Acceptance criteria

- Each admitted readiness state is typed, mutually intelligible across backend and frontend, and owned by one reviewed transition source.
- Terminal or unavailable state cannot claim live readiness or fabricate data.
- Runtime, selected-symbol, history, and overlay readiness remain separately represented.

## Proof plan

Required proof definitions: `QT-PROOF-218`, `QT-PROOF-219`.

- Additional evidence: A reviewed readiness-state transition table shared by backend and frontend contracts.

## Review boundary

BotLens-projections and frontend reviewers own readiness semantics; this draft does not adopt disputed vocabulary.
