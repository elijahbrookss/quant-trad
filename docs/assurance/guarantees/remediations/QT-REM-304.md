---
remediation_id: QT-REM-304
guarantee_ids: QT-GUAR-ENUMERATED-AGENT-MUTATION-GATES
lifecycle: proposed
owner: application-interfaces
required_reviewers: application-interface-owner,normative-contract-reviewer,research-governance-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-304

**Resolve agent mutation scope authority**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

ADR 0048 simultaneously describes a broad enumerated mutation boundary and limits the extra evidence gate to research promotion, leaving DOC-MUTATION-SCOPE-001 unresolved while named implementation guards remain nonuniform.

## Action

Present the conflicting clauses and a complete mutation-surface inventory for owner review, then record a reviewed authority decision before changing normative text or product behavior.

## Acceptance criteria

- A reviewed decision distinguishes baseline mutation controls from research-promotion evidence requirements.
- Every agent-facing mutation surface is inventoried with its dry-plan, apply, confirmation, audit, and evidence behavior.
- The accepted wording and implementation classification no longer rely on contradictory clauses.

## Proof plan

- Generate a closed CLI and MCP mutation-surface inventory.
- Retain QT-PROOF-304 as representative named-path evidence.
- Specify additional selectors only after the authority decision fixes the intended denominator.

## Review boundary

Application-interface, research-governance, and normative-contract reviewers decide scope; this remediation does not choose semantics or authorize product repair.
