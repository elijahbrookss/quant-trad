---
remediation_id: QT-REM-308
guarantee_ids: QT-GUAR-SINGLE-LOKI-INGRESS-PER-TOPOLOGY
lifecycle: proposed
owner: observability
required_reviewers: deployment-owner,observability-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-308

**Resolve Loki ingress authority conflict**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

ADR 0033's amendment describes Alloy on the server while its retained body prescribes Promtail as the runtime ingress, leaving DOC-LOG-INGRESS-001 unresolved despite aligned frozen Compose files.

## Action

Present the conflicting ADR clauses and rendered development and server topology inventories for joint review, then record the selected authority before any normative edit.

## Acceptance criteria

- A reviewed decision identifies the one permitted shipper for each supported topology.
- Application services and direct Loki handlers are inventoried and excluded from duplicate ingress.
- The accepted authority text no longer contradicts itself.

## Proof plan

- Generate rendered development and server logging-topology inventories.
- Retain QT-PROOF-308 as representative configuration evidence.
- Add a duplicate-ingress failure fixture after the authority decision.

## Review boundary

Deployment and observability owners decide topology authority; this draft does not select Promtail or Alloy beyond reporting the frozen implementation.
