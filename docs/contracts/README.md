# Quant-Trad Platform Contracts

This folder defines the platform-level contracts for agents and contributors.

If implementation conflicts with these docs, the docs are the source of truth.

For explanatory reading paths, start at [../index.md](../index.md). Return here when behavior needs normative detail.

## Authority And Precedence

QT uses one authority hierarchy rather than parallel sources of product truth:

1. The platform contracts in this directory own platform-wide product
   behavior.
2. Accepted ADRs preserve decisions and rationale. A newer ADR may authorize a
   contract reconciliation, but it does not silently override unreconciled
   platform-contract wording.
3. Architecture and component documents explain the current system, its
   boundaries, and narrower implementation semantics. They remain subordinate
   to platform contracts and accepted decisions.
4. Implementation and tests provide conformance evidence; they do not become
   normative merely because behavior exists.

`AGENTS.md` defines contributor and agent workflow. It may summarize product
rules but cannot override these contracts. Architecture roadmaps remain
explanatory unless their decisions are reconciled into this hierarchy. The
[six core promises](../core-promises.md) summarize important system outcomes
without replacing these contracts.

The [adopted platform glossary](platform/04_glossary.md) is a vocabulary
index into these owners. It may qualify terms and aliases but cannot create
or broaden behavioral authority.

## Read Order

1. `platform/00_system_contract.md`
2. `platform/01_runtime_contract.md`
3. `platform/02_execution_playback_contract.md`
4. `platform/03_engineering_contract.md`
5. `platform/04_glossary.md`

## Writing Standard

When authoring component-level architecture docs, follow:
- `../engineering/documentation/component-documentation-standard.md`

## Scope Rule

These files are platform-wide contracts only.
Indicator-specific semantics belong in indicator module docs.
