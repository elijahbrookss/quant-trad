---
component: offline-research-governance
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - governance
  - promotion
  - audit
  - policy
  - autonomy
  - offline
code_paths:
  - src/research_governance
  - portal/backend/service/research/governance.py
  - portal/backend/service/research/governance_repository.py
  - portal/backend/controller/research.py
  - portal/backend/db/models.py
  - scripts/db/manual_migration_offline_research_governance_v1.sql
  - tests/test_research_governance
  - tests/test_portal/test_research_authority.py
---
# Offline Research Governance

## Implemented lifecycle and ceiling

Phase 6 governs research-registry promotion only:

Diagram source: [offline autonomy Phase 4-6](diagrams/offline-autonomy-phase4-6.mmd).

```text
OBSERVATION
  -> HYPOTHESIS
  -> PROTOCOL_PROPOSED
  -> PROTOCOL_APPROVED
  -> TRIALS_RUNNING
  -> EVIDENCE_PRODUCED
  -> CANDIDATE_NOMINATED
  -> VALIDATION_PASSED
  -> HOLDOUT_ELIGIBLE
  -> HOLDOUT_EVALUATED
  -> RESEARCH_CERTIFIED
```

Weak work can transition to `REJECTED` and then `ARCHIVED`. Certified work may
become `RESEARCH_DEGRADED` on new immutable deterioration evidence and can then
be rejected or archived. `RESEARCH_CERTIFIED` is the maximum positive state.

`SHADOW`, `PAPER`, `CONTROLLED_LIVE`, `LIVE`, `DEPLOYED`, external submission,
capital approval, and equivalent states are not merely unauthorized; they are
absent from the enum and explicitly rejected before proposal persistence.

## Plan/apply authority

Each transition has two durable records:

1. An immutable proposal pins the case/version, source and target, binding
   updates, rationale, evidence hashes, proposer identity, and request identity.
2. An immutable decision pins approve/reject, authorizer identity, policy
   evidence, resulting state/version, request identity, and decision hash.

The proposer cannot authorize the same proposal. The case projection is updated
only in the decision transaction under a row lock and exact expected version.
Stale or duplicate decisions fail or return the existing idempotent record.
There is no audit-disable path.

## Policy gates

Authorization re-resolves canonical persisted evidence rather than trusting the
proposal:

- observations and hypotheses must be existing typed research-memory items;
- family/protocol identity and hashes must agree;
- the protocol must be active before approval;
- trials require an open family;
- evidence requires at least one attempt and all attempts terminal;
- nomination binds the exact family candidate;
- validation requires its completed validation source attempt;
- the candidate freezer cannot authorize validation promotion;
- holdout eligibility requires the closed family, current frozen candidate,
  complete accounting, and no prior holdout use;
- holdout evaluation requires the one-use record completed for that candidate;
- research certification requires a qualified scientific certificate; and
- the certificate issuer cannot authorize its own certification transition.

Agents can propose, run permitted trials, reject weak work, nominate, request
holdout operations, and archive. They cannot authorize their own proposal,
increase protocol budgets, edit protocols or frozen candidates, reopen a
consumed holdout, fabricate certificates, bypass audit, mutate runtime state,
deploy, access trading credentials, submit orders, or obtain capital.

## Honest deployment statement

This is one application with logical actor roles, not cryptographic institutional
separation. API actor identity remains an application identity assertion; a
future authenticated principal/authorization service can harden that seam.
Within the implemented workflow, policy and evidence checks are transactional
and durable. No operational trading transition is connected.
