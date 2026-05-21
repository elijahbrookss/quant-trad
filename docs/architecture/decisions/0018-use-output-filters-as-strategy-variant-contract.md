---
component: adr-strategy-variant-output-filters
subsystem: strategy-research
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - strategy
  - variants
  - indicators
  - research
  - experiments
code_paths:
  - portal/backend/db/models.py
  - portal/backend/controller/strategies.py
  - portal/backend/service/strategy_variant_resolution.py
  - portal/backend/service/strategies/strategy_service/facade.py
  - portal/backend/service/bots/config_service.py
  - portal/backend/service/bots/strategy_loader.py
  - src/strategies/compiler.py
  - src/strategies/evaluator.py
  - cli
---
# ADR 0018: Use Output Filters As The Strategy Variant Contract

## Status

Accepted on 2026-05-17.

## Context

Quant-Trad strategy research starts with a broad base decision idea and then
narrows that idea using evidence from completed runs. A common workflow is:

1. attach indicators to a strategy,
2. run the unfiltered base strategy,
3. inspect emitted signals, decisions, trades, world state, and report context,
4. create a named variant that adds one or more conditions over already-emitted
   indicator outputs,
5. rerun and compare.

The previous variant system did not model that workflow directly. A saved
strategy variant stored `param_overrides` and could also select an
`atm_template_id`.

That shape creates ambiguity:

- `param_overrides` only affect behavior when base rules already reference
  those params through `$params.*`.
- A variant param like `expansion_state=expanding` is a no-op unless a rule was
  authored to consume it.
- Indicator tuning params such as confirmation bars, lookbacks, and thresholds
  belong to indicator configuration because they change what the indicator
  emits.
- ATM templates control sizing, stops, targets, and execution-risk behavior,
  which is a different research axis from decision eligibility.
- Combining decision filters and ATM changes in one variant muddies attribution
  during report comparison.

The existing compiler and evaluator already support deterministic guards over
attached indicator outputs. The missing contract is a strategy variant shape
that exposes those guards as the primary research primitive without pretending
they are params.

## Decision

A `StrategyVariant` is a named set of output filters over indicator outputs
already attached to the base strategy.

Strategy variants do not own:

- indicator configuration,
- indicator params,
- ATM templates,
- risk sizing,
- stop or target policy,
- wallet, order, trade, fee, slippage, or execution semantics.

The target mental model is:

- `Strategy`: base decision idea and base rules.
- `Indicator config`: controls what typed outputs, context, metrics, and signals
  are emitted.
- `StrategyVariant`: filters base strategy decisions using emitted outputs from
  attached indicators.
- `ATMTemplate`: controls sizing, stops, targets, and execution-risk behavior.
- `Experiment` or run config: selects the strategy, strategy variant, ATM
  template, wallet, run window, and execution mode.
- `run_strategy_snapshot`: freezes the exact effective decision configuration
  used at run start.

An output filter is a declarative condition over an attached indicator output.
For example:

```json
{
  "scope": {
    "intent": ["enter_long", "enter_short"]
  },
  "indicator_id": "ba2a253d-00ed-41ba-8846-10e38210dc79",
  "output_name": "market_regime",
  "field": "expansion_state",
  "operator": "equals",
  "value": "expanding"
}
```

At preview, compile, bot loading, and run start, variant output filters are
resolved once against the base strategy and materialized into the existing
compiled rule guard contract. The compiler remains the validation boundary for
attached indicator IDs, output names, output types, fields, operators, and
supported guard semantics.

The initial supported filter forms should stay small:

- context output equality filters,
- metric output comparison filters if already supported by the compiler,
- optional rule or intent scope so a filter can apply to specific base rules
  without cloning the strategy.

The first implementation should not introduce a broader rule-diff framework.
It should only materialize output filters into deterministic rule guards.

## Consequences

- A variant can express "only take confirmed balance breakout entries when
  `market_regime.expansion_state == expanding`" without changing indicator
  outputs or base strategy rules.
- Creating a variant from report evidence becomes direct and machine-friendly:
  agents can inspect report context and propose output filters against attached
  indicator outputs.
- Variant behavior is visible in compiled rules, strategy hash, effective
  strategy config, and run strategy snapshots.
- Output-filter behavior is auditable from compact decision artifact
  `output_filter_trace` records. The trace records each materialized variant
  filter's output ref, field, operator, expected value, actual value,
  readiness, and match result without changing the underlying guard result.
- Indicator experiments remain separate: changing confirmation bars, lookbacks,
  or thresholds requires changing indicator configuration or a future experiment
  layer that snapshots indicator configs explicitly.
- ATM experiments remain separate: selecting a different ATM template belongs to
  experiment/run configuration, not `StrategyVariant`.
- Research attribution improves because one variant changes decision
  eligibility instead of mixing decision filters with stop, target, or sizing
  changes.
- The `param_overrides`-centric variant shape is removed from the target design
  instead of expanded.

## Implementation Notes

Implementation seams:

- `portal_strategy_variants` persists `output_filters`; ATM ownership is not
  part of the variant row.
- Strategy variant API DTOs expose `output_filters`, not indicator params or ATM
  template IDs.
- `resolve_strategy_variant()` resolves effective params from the base strategy
  and effective output filters from the selected/default variant.
- Bot config or experiment config selects ATM independently from variants.
- Strategy preview and bot runtime must call the same variant resolver so
  preview, compile, runtime loading, execution, and report metadata agree on
  the same effective decision configuration.
- Reports should preserve both authored `output_filters` and materialized guards
  in `effective_strategy_config` / `run_strategy_snapshot`.
- Materialized guards should preserve compact provenance identifying their
  source `variant_output_filter`, but that provenance is audit metadata. It must
  not change match semantics or behavioral strategy hashes.
- Decision artifacts should carry `output_filter_trace` when a rule includes
  materialized variant output filters. The trace is report/research evidence for
  why a variant allowed or blocked a rule at that bar.

Guardrails:

- Output filters may only reference indicators attached to the strategy.
- Output filters may only reference public typed outputs declared by indicator
  metadata.
- Output filters must not mutate indicator state or output payloads.
- Output filters must not read overlays, details, or indicator internals.
- Output filters must fail loud during validation if an output, field, operator,
  scope, or rule target is invalid.
- Output filter materialization must be deterministic and included in strategy
  hashing.

## References

- [System contract](../../contracts/platform/00_system_contract.md)
- [Runtime contract](../../contracts/platform/01_runtime_contract.md)
- [Keep strategy decisions separate from execution](0005-keep-strategy-decisions-separate-from-execution.md)
- [Use an API-backed CLI for research orchestration](0017-use-api-backed-cli-for-research-orchestration.md)
- [Research orchestration boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
