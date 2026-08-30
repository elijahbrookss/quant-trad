# Documentation

The documentation homepage is [index.md](index.md). It starts with the reason
to use QT and introduces internal detail only when a workflow needs it.

## Recommended Reading

1. [Overview](overview.md) — what QT does, who it helps, and how to interpret a
   result.
2. [Getting started](getting-started.md) — install and verify the local stack.
3. [Core research workflow](guides/research-workflow.md) — move from a trading
   idea to evidence, a Strategy, backtest, paper simulation, and comparison.
4. [Concepts](concepts/) and the
   [platform glossary](contracts/platform/04_glossary.md) — learn the mental
   model and look up exact QT wording when needed.
5. [Architecture](architecture/README.md) — follow truth, ownership, and
   interface boundaries.
6. [Six core promises](core-promises.md) — the high-consequence outcomes QT
   protects.
7. [Current system](current-system.md) — implemented scope, normal validation,
   historical internals, and honest limits.

After that, choose the depth you need:

- [Concepts](concepts/) explain runtime, execution, Strategies, BotLens, and
  reporting in approachable terms.
- [Guides](guides/) cover research work and supported extensions.
- [Operator handbook](operators/README.md) covers deployment, providers,
  collectors, updates, backup, and recovery.
- [Architecture](architecture/README.md) maps truth and ownership boundaries.
- [Engineering](engineering/README.md) covers implementation and validation.
- [Contracts](contracts/README.md) contain the exact normative rules.
- [Architecture decisions](architecture/decisions/README.md) preserve durable
  choices and their tradeoffs.
- [Incidents](incidents/README.md) preserve dated investigations.

Contracts remain the source of truth. Explanatory pages should make QT easier
to understand without redefining its behavior.
