---
remediation_id: QT-REM-008
guarantee_ids: QT-GUAR-SHARED-APPLICATION-CONTRACT
lifecycle: proposed
owner: application-interfaces
required_reviewers: api-owner,cli-owner,mcp-owner,platform-contract-reviewer
required_review: true
review_status: pending
---

# Remediation QT-REM-008

This proposal records incomplete interface-family coverage. It does not change
CLI, API, MCP, or application-service semantics and does not rely on unresolved
`AGENTS.md` precedence for activation.

## Gap

The Research Check family has strong shared-operation evidence, but there is no
generated operation-family manifest proving application-contract parity across
all corresponding CLI, API, and MCP surfaces. `ARCH-COVERAGE-001` and
`CI-TRACE-001` remain open.

## Action

After CLI, API, MCP, and platform-contract review, generate a cross-interface
operation-family manifest with explicit intentional omissions. Add structural
checks that each mapped adapter routes to the shared application operation and
keeps only interface-specific rendering, sequencing, authorization, and
confirmation behavior.

## Acceptance criteria

- Every reviewed cross-interface operation family is accounted for once in the
  manifest.
- Each mapped interface routes to one shared application operation with aligned
  payload, result, and error semantics.
- Intentional interface-only operations are explicit and owner-reviewed.
- No operation relies on `AGENTS.md` as activating authority while precedence
  remains unresolved.

## Proof plan

Run the generated manifest/static routing checks and representative
credential-free CLI/API/MCP parity tests in `python-nondb`. Bind the exact
operation set and results in a later attestation.

## Review boundary

API owner, CLI owner, MCP owner, and platform-contract reviewer.
