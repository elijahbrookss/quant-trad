---
remediation_id: QT-REM-315
guarantee_ids: QT-GUAR-AGENT-WORKFLOW-BOUNDARIES
lifecycle: proposed
owner: repository-governance
required_reviewers: agent-governance-owner,application-interface-owner,architecture-documentation-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-315

**Close distributed agent workflow governance proof**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

AGENTS.md distributes broad workflow expectations across frontend, application, validation, documentation, and database boundaries, while named tests cover only a small subset and AGENTS precedence remains nonactivating.

## Action

Inventory each governance rule, its authoritative owner, enforcement point, and proof selector, then obtain explicit authority review before adopting or enforcing missing boundaries.

## Acceptance criteria

- Each agent workflow rule maps to one owning boundary and a reviewed authority source.
- Each enforceable rule has a deterministic check or a documented manual review boundary.
- No AGENTS.md instruction is treated as automatic guarantee activation authority.

## Proof plan

- Generate the governance-rule-to-check traceability matrix.
- Add focused checks for unguarded application, database, and parallel-truth paths after owner review.
- Retain QT-PROOF-316 as representative MCP and architecture-index evidence.

## Review boundary

Agent-governance, application-interface, and architecture-documentation owners review authority and enforcement; this draft neither changes AGENTS.md nor repairs product semantics.
