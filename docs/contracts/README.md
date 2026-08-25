# Quant-Trad Platform Contracts

This folder defines the platform-level contracts for agents and contributors.

If implementation conflicts with these docs, the docs are the source of truth.

For explanatory reading paths, start at [../index.md](../index.md). Return here when behavior needs normative detail.

## Authority And Precedence

QT uses one authority hierarchy rather than parallel sources of product truth:

1. The platform contracts in this directory own platform-wide product
   behavior.
2. A reviewed component or source-module contract may own narrower behavior
   only inside its declared component scope. It remains subordinate to the
   platform contracts and must be discoverable from the owning architecture
   component document.
3. Accepted ADRs preserve decisions and rationale. A newer ADR may authorize a
   contract reconciliation, but it does not silently override unreconciled
   platform-contract wording.
4. Architecture documents explain the current system and its boundaries.
5. Implementation and tests provide conformance evidence; they do not become
   normative merely because behavior exists.

`AGENTS.md` owns contributor and agent workflow. It may summarize product rules
but cannot override these contracts or activate a guarantee. Architecture
roadmaps remain explanatory unless an approved rule is reconciled into this
hierarchy. Assurance registries, proof catalogs, attestations, and campaign
records index authority and evidence; they are not an additional source of
product requirements.

An adopted glossary is a vocabulary index into these owners. It may qualify
terms and aliases but cannot create or broaden behavioral authority.

## Read Order

1. `platform/00_system_contract.md`
2. `platform/01_runtime_contract.md`
3. `platform/02_execution_playback_contract.md`
4. `platform/03_engineering_contract.md`

## Writing Standard

When authoring component-level architecture docs, follow:
- `../engineering/documentation/component-documentation-standard.md`

## Scope Rule

These files are platform-wide contracts only.
Indicator-specific semantics belong in indicator module docs.
