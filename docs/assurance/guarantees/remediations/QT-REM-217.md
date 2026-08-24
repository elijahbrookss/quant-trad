---
remediation_id: QT-REM-217
guarantee_ids: QT-GUAR-BOTLENS-HOT-STATE-NOT-HISTORY
lifecycle: proposed
owner: botlens-projections
required_reviewers: botlens-projections-owner,frontend-owner,persistence-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-217

**Close BotLens hot-state and history separation**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Bounded frontend hot state and durable backend history reconstruction are tested, but no closed path inventory proves that all full-history reads bypass hot projections and all hot state remains bounded.

## Action

Inventory every chart, trade, overlay, and run-history read path; enforce explicit hot-versus-durable ownership; and add bounds, eviction, restart, range-completeness, and provider-trap tests.

## Acceptance criteria

- Every full-history request reconstructs from reviewed durable domain truth rather than bounded projector or client state.
- Every hot projection and client cache has an explicit reviewed bound and eviction behavior.
- Restart and focused-symbol changes cannot cause bounded hot state to masquerade as complete history.

## Proof plan

Required proof definitions: `QT-PROOF-220`, `QT-PROOF-221`.

- Additional evidence: A reviewed hot-versus-history read-path inventory and static dependency rule.

## Review boundary

BotLens-projections, persistence, and frontend reviewers own storage/read boundaries; no retention policy is changed here.
