---
component: typed-strategy-action-graph
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - strategies
  - expressions
  - signals
  - actions
  - execution-policy
  - autonomy
code_paths:
  - src/strategies/typed_graph.py
  - portal/backend/service/research/authority.py
  - portal/backend/service/research/authority_repository.py
  - portal/backend/db/models.py
  - tests/test_strategies/test_typed_graph.py
  - tests/test_portal/test_research_authority.py
---
# Typed Strategy and Action Graph

## Implemented capability

Phase 5 gives research agents bounded creative authority over data-only strategy
graphs. A graph declares approved facts, typed expressions, prioritized rules,
canonical actions, sizing, risk limits, and execution policy. It compiles to a
stable hash and emits a `canonical_action_intent.v1`; it does not submit orders.

```text
approved causal facts
  -> typed expression tree
  -> deterministic priority/tie-break evaluation
  -> enter / exit / add / reduce / reverse / cancel / hold
  -> canonical sizing, risk, and execution-policy intent
  -> existing strategy/risk/execution boundaries resolve concrete orders
```

Approved fact namespaces are `market`, `indicator`, `regime`, `time`, current
and previous `signal`, `position`, `risk`, and `order`. Values are exactly
boolean, finite number, or string. Expressions support constants, fact reads,
boolean composition, comparisons, and a small numeric vocabulary. Unknown
nodes, fields, facts, types, and divide-by-zero fail deterministically.

Execution policy supports market, aggressive limit, passive limit, stop, and
staged intent; time in force, expiration, bounded chasing, price offsets, and
stage count remain separate from the action's meaning. The compiler maps policy
styles to the existing canonical order vocabulary such as `market`,
`limit_aggressive`, `limit_resting`, and `stop_market`. Concrete quantity,
price, fee, venue validation, lifecycle, and accounting remain owned by their
existing canonical boundaries.

## Generation authority

Every persisted graph:

- belongs to one open protocol family;
- inherits the protocol hash and actor identity from the service, not caller
  overrides;
- consumes a train attempt from that family's attempt/runtime/compute budget;
- records parent graph and parent attempt lineage;
- accounts for declared validation feedback;
- accepts only dimensions allow-listed by both the platform schema and the
  immutable family protocol;
- has immutable graph and compiled hashes; and
- is retained beside its search attempt and family audit trail.

An unsafe graph is rejected before it consumes a trial. A parent must already
exist in the same family. Generated graphs cannot escape into an undeclared
family.

## Capability denial

The schema recursively rejects code, Python, callables, modules, imports,
evaluation, files, paths, shell commands, network/URLs, providers, credentials,
secrets/tokens, deployment, runtime mutation, external orders, and capital.
There is no arbitrary-code node or extension hook.

Phase 5 grants strategy invention for offline experiments. Runtime support for
every new action still depends on the existing canonical action/risk/execution
adapters; a compiled graph is never deployment or trading authorization.
