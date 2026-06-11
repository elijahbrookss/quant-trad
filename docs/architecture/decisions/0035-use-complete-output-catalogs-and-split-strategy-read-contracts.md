---
component: adr-complete-output-catalogs-split-strategy-reads
subsystem: strategy-research
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - indicators
  - strategy
  - research
  - mcp
  - cli
code_paths:
  - portal/backend/controller/indicators.py
  - portal/backend/service/indicators
  - portal/backend/controller/strategies.py
  - portal/backend/service/strategies/strategy_service/facade.py
  - portal/frontend/src/adapters/strategy.adapter.js
  - cli/main.py
  - cli/mcp_server.py
  - docs/architecture/indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md
  - docs/architecture/decision-layer/DECISION_LAYER_BOUNDARY.md
---
# ADR 0035: Use Complete Output Catalogs And Split Strategy Read Contracts

## Status

Accepted on 2026-06-09.

## Context

Indicator signal outputs were previously persisted with output preferences that
could mark a public signal output as disabled. That made a consumer display
choice look like indicator truth. It also forced strategy authoring, signal
preview, reports, replay, and future observation mining to reason about whether
an output exists, is emitted, or is merely hidden by a preference.

Strategy reads also returned bundled detail payloads. Inventory rows could carry
full bindings, full indicator metadata, rules, and variants even when callers
only needed counts or IDs. That increased payload size and made agent workflows
depend on a broad route instead of asking for the contract they actually need.

## Decision

Public indicator output catalogs are complete. Indicators declare every public
typed output they can emit. Persisted indicator identity does not include
output preferences, enabled signal output flags, or disabled-output state.

Output selection belongs to consumers:

- strategy rules select trigger and guard outputs,
- strategy variants select output filters,
- signal preview and research checks may request focused output names or event
  keys,
- UI visibility is display state, not indicator identity.

Strategy reads are split by concern:

- `strategy_inventory.v1`
- `strategy_definition.v1`
- `strategy_bindings.v1`
- `strategy_rules.v1`
- `strategy_variants.v1`
- `effective_strategy.v1`
- `strategy_decision_inputs.v1`

The effective and decision-input contracts are read-only inspection surfaces
over the same compiler and variant resolver used by preview, bot loading, and
runtime.

Frontend strategy reads must consume the split contracts. The frontend strategy
authoring surface is dormant while workflows are proven through `qt`; frontend
strategy mutation calls fail loud instead of writing through the older bundled
detail worldview.

## Consequences

- Reports, replay, and observation mining can see every output an indicator
  publicly declares.
- Hidden UI state cannot silently change strategy-visible indicator truth.
- Agents can read compact strategy inventory without fetching full indicator
  manifests and rules.
- Decision-input discovery has a clear place to ask which signal events,
  context states, and metric fields exist and which ones are selected by the
  effective strategy.
- Callers that used the old bundled strategy detail response must request the
  specific read contract they need.
- The existing frontend may visualize strategy inventory/detail by composing
  split read contracts, but it is not the strategy authoring source of truth.

## Guardrails

- Do not persist indicator output preferences or disabled signal output maps.
- Do not add fallback aliases such as `enabled_signal_outputs` to request
  contracts. Focused signal requests use `output_names` and `event_keys`.
- Do not make frontend state, overlays, or debug projections strategy truth.
- Do not keep old frontend strategy CRUD services as a parallel contract layer.
- Do not let dormant frontend authoring paths mutate strategy rows until the UI
  is rebuilt around proven `qt` workflows.
- Do not rebuild effective strategy state outside the existing compiler and
  variant resolver.

## References

- [Indicator Runtime Boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Decision Layer Boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [ADR 0018: Use Output Filters As The Strategy Variant Contract](0018-use-output-filters-as-strategy-variant-contract.md)
