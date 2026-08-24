---
remediation_id: QT-REM-301
guarantee_ids: QT-GUAR-V1-LOCAL-TRUST-BOUNDARY
lifecycle: proposed
owner: deployment-security
required_reviewers: deployment-owner,security-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-301

**Close the Version 1 deployment trust-boundary proof**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The current proof parses Compose port bindings but does not validate an instantiated host network boundary or continuously guard the explicit capability ceiling.

## Action

Add a reviewed deployment-boundary inventory and isolated topology check covering every published service and the absence of claimed production access-control capabilities.

## Acceptance criteria

- Every server Compose published port is inventoried and bound to loopback in the reviewed topology.
- The capability statement explicitly excludes production authentication, authorization, rate limiting, and multi-user isolation.
- The proof detects any newly published remotely reachable service.

## Proof plan

- Generate a deterministic published-port inventory from the rendered server Compose configuration.
- Run an isolated topology inspection against the rendered configuration.
- Retain QT-PROOF-301 as the static configuration prerequisite.

## Review boundary

Deployment and security owners review any expansion beyond local-trusted access; the remediation cannot infer authentication or external-trading authority.
