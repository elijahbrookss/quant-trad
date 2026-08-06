---
component: adr-in-app-scientific-authority-offline-ceiling
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - research
  - scientific-controls
  - strategy-generation
  - governance
  - offline
code_paths:
  - src/research_science
  - src/strategies/typed_graph.py
  - src/research_governance
  - portal/backend/service/research/authority.py
  - portal/backend/service/research/governance.py
  - portal/backend/db/models.py
---
# ADR 0059: Use In-App Scientific Authority and an Offline Certification Ceiling

## Status

Accepted on 2026-08-06 for scientific authority, bounded strategy generation,
and offline research governance.

## Context

Autonomous variant generation is unsafe if agents can choose datasets per
trial, spend unaccounted search, reuse holdouts, modify frozen candidates, or
reinterpret exploratory runs as selection evidence. The platform needs these
controls before its strategy language expands. It does not yet need multiple
deployed services pretending to provide organizational independence, and the
owner is not ready to open shadow, paper, live, credential, or capital paths.

## Decision

Keep one application and primary database. Implement scientific authority as
immutable protocol contracts, controlled service operations, database-unique
holdout use, and append-only evidence. Express strategy invention in a bounded
typed data graph whose creation consumes the same search budget. Govern
research-registry changes with immutable transition proposals and separate
decisions. Stop positive progression at `RESEARCH_CERTIFIED`.

Normal research operations receive redacted holdout protocols and derived
train/validation bindings. They cannot choose datasets or fetch providers.
`PLATFORM_CONTROLLED_HISTORICAL` claims only workflow non-exposure; it does not
claim that globally accessible historical data was unknowable. Strong external
attestation and forward-unseen classes fail closed until their authorities
exist.

## Consequences

- Agents can conduct bounded, reproducible search and invent typed variants.
- Scientific quality S0-S4 and execution quality X0-X5 remain independent.
- Failed work, proposals, lineage, certificates, and decisions remain durable.
- Proposal and authorization identities are separate, but application identity
  is not yet institution-grade authentication.
- No strategy graph is code or order-submission authority.
- No research certification state implies shadow, paper, deployment, capital,
  or external trading authorization.

## Rejected alternatives

- Build allocator/researcher/certifier/governance/auditor microservices before
  the single-application workflow has operational need for them.
- Let agents provide datasets directly on trial requests.
- Use repeated historical holdout evaluation as ordinary validation.
- Expand strategy mutation with arbitrary Python.
- Reuse runtime/live promotion states for offline research certification.

## Evidence

Persisted tests execute the complete path from observation through qualified
scientific evidence and `RESEARCH_CERTIFIED`, prove proposal self-authorization
fails, prove the holdout is one-use and redacted, and prove operational/capital
states are structurally rejected.
