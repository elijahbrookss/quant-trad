---
component: adr-boundary-first-architecture-docs
subsystem: architecture-docs
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - documentation
  - architecture
  - boundaries
code_paths:
  - docs/architecture
  - docs/architecture/ARCHITECTURE_DOCS_MODEL.md
  - scripts/docs/build_architecture_index.py
---
# ADR 0001: Use Boundary-First Architecture Documentation

## Status

Accepted, backfilled on 2026-05-13.

## Context

The architecture tree used to mirror packages and implementation history. That
made ownership hard to audit because BotLens, persistence, observability,
runtime execution, strategy signals, and storage were mixed across several
package-shaped notes.

The current architecture docs have moved toward conceptual boundaries:
system, engine, identity, data, indicator runtime, decision layer, execution
runtime, persistence, BotLens projections, reporting, and observability.

## Decision

Architecture documentation is organized by truth and ownership boundary, not by
source package. Package paths remain important, but they are listed as
implementation references through frontmatter `code_paths` and the generated
component index.

ADRs live under `docs/architecture/decisions/` because they explain durable
architecture choices. They use frontmatter so they remain discoverable without
becoming normative contracts.

## Consequences

- Contributors should start from contracts and boundary docs before editing
  architecture-sensitive code.
- New component docs should be targeted and boundary-oriented.
- Historical package notes should be consolidated into the relevant boundary
  doc or ADR instead of revived as parallel sources of truth.
- The architecture component index must be regenerated after frontmatter
  changes.

## References

- [Architecture docs model](../ARCHITECTURE_DOCS_MODEL.md)
- [Architecture README](../README.md)
- [Engineering contract](../../contracts/platform/03_engineering_contract.md)

