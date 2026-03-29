---
component: strategy-decision-layer
subsystem: strategy
layer: architecture
doc_type: architecture
status: active
tags:
  - strategy
  - decision
  - contract
  - preview
  - runtime
  - rules
code_paths:
  - src/strategies
  - portal/backend/service/strategies
  - src/engines/bot_runtime/strategy
  - src/engines/bot_runtime/runtime/mixins/execution_loop.py
  - portal/backend/service/bots/strategy_loader.py
---
# Strategy Decision Layer Architecture (North Star)

## Documentation Header

- `Component`: StrategySpec compiler, decision evaluation, preview/runtime parity
- `Owner/Domain`: Strategy / Bot Runtime
- `Doc Version`: 1.0
- `Related Contracts`: [[00_system_contract]], [[01_runtime_contract]], [[02_execution_playback_contract]], [[ENGINE_OVERVIEW]], [[SIGNAL_PIPELINE_ARCHITECTURE]], [[INSTRUMENT_CONTRACT_FUTURES_V1_READINESS]]

## 1) Problem and scope

This document defines the target architecture for the strategy decision layer.

The goal is one clean strategy contract that:
- consumes canonical typed indicator outputs,
- evaluates decisions through one canonical path,
- keeps decision logic separate from risk and execution behavior,
- and gives strategy preview and bot runtime the same semantics.

In scope:
- canonical `StrategySpec` and `CompiledStrategySpec`,
- decision rule authoring and compile-time validation,
- decision evaluation from typed indicator outputs,
- readonly strategy preview using the same evaluator as bot runtime,
- boundaries between decision logic, position policy, risk policy, and execution policy.

### Non-goals

- YAML as the canonical backend contract,
- portfolio-level netting or cross-symbol allocation logic in v1,
- multiple concurrent trades per symbol in v1,
- replacing the indicator runtime contract,
- replacing the ATM template or instrument runtime profile contracts.

Upstream assumptions:
- indicators already publish canonical typed outputs,
- instruments already validate through canonical instrument/runtime readiness checks,
- bot runtime remains the source of truth for execution realism.

## 2) Architecture at a glance

Boundary:
- inside: strategy spec, compiler, decision evaluator, canonical decision artifacts, preview/runtime parity
- outside: indicator authoring, QuantLab research execution, wallet/execution adapters, provider-specific metadata

```mermaid
flowchart LR
    A[IndicatorExecutionEngine] --> B[Typed Outputs Per Bar]
    B --> C[StrategyCompiler]
    C --> D[CompiledStrategySpec]
    D --> E[Decision Evaluator]
    E --> F[Decision Artifacts + Selected Candidate]
    F --> G[Readonly Strategy Preview]
    F --> H[Position Policy Gate]
    H --> I[Risk Policy]
    I --> J[Execution Policy / ATM / Bot Runtime]
    K[QuantLab Research] --> A
```

## 3) Inputs, outputs, and side effects

- Inputs: `StrategySpec`, typed indicator outputs, canonical instrument bindings, bot/runtime configuration.
- Dependencies: typed indicator output contract, instrument contract, execution profile compiler, ATM template contract.
- Outputs: canonical decision artifacts, zero-or-one selected decision candidate per bar/instrument, downstream rejection artifacts, preview markers/read models.
- Side effects: none inside the decision evaluator; persistence and execution happen downstream.

## 4) Core components and data flow

### 4.1 StrategySpec

`StrategySpec` is the canonical persisted strategy contract.

It is a normalized object contract, not a YAML document and not a loose dict blob.

`StrategySpec` owns:
- strategy identity and metadata,
- market context (`timeframe`, `datasource`, `exchange`),
- indicator bindings,
- instrument bindings as explicit inclusion,
- decision rules,
- position policy,
- risk policy references,
- execution policy references.

### 4.2 CompiledStrategySpec

Raw strategy specs do not execute directly.

The compiler validates and resolves:
- attached indicator references,
- output names and output types,
- event keys / context state keys / metric fields,
- rule normalization,
- required output refs for evaluation,
- policy defaults.

`CompiledStrategySpec` is the only executable form.

### 4.3 Decision evaluator

The evaluator is pure and readonly.

It accepts:
- `CompiledStrategySpec`,
- current-bar typed outputs,
- current bar time/epoch.

It emits:
- one decision artifact per evaluated enabled rule for the current bar,
- zero or one selected decision candidate for the current bar/instrument after deterministic rule resolution.

The evaluator does not:
- inspect overlays,
- inspect indicator internals,
- size orders,
- read wallet state,
- place orders,
- mutate execution state.

### 4.4 Strategy preview

Strategy preview is a readonly replay surface.

It must:
- run the same indicator engine semantics as runtime,
- use the same `CompiledStrategySpec`,
- use the same decision evaluator,
- emit preview read models from the resulting decision artifacts and selected decision candidates.

Preview is not allowed to implement a parallel decision grammar or reconstruct decisions through separate rule logic.

Preview derives its read models from the same decision artifacts and selected decision candidates that bot runtime uses.

### 4.5 Bot runtime integration

Bot runtime remains responsible for:
- position policy enforcement,
- risk sizing,
- execution realism,
- fills, costs, and lifecycle events.

Bot runtime consumes the selected decision candidate and may reject it downstream for explicit reasons represented by a canonical rejection artifact.

## 5) State model

Authoritative state:
- persisted `StrategySpec`,
- canonical indicator typed outputs,
- bot runtime execution state and runtime event stream.

Derived state:
- `CompiledStrategySpec`,
- decision artifacts,
- selected decision candidates,
- rejection artifacts,
- preview marker rows,
- playback/debug views.

Persistence boundaries:
- persisted: strategy specs, rule definitions, strategy/instrument/indicator bindings, runtime events, decision/rejection read models where required by downstream consumers.
- in-memory: compiled specs, per-bar outputs, per-bar decision artifacts during current evaluation.

## 6) Canonical contract set

## 6.1 StrategySpec

`StrategySpec` v1 should stay intentionally small.

Representative shape:

```json
{
  "spec_version": "strategy_spec_v1",
  "strategy_id": "uuid",
  "name": "Breakout With Regime Filter",
  "market_context": {
    "timeframe": "5m",
    "datasource": "interactive_brokers",
    "exchange": "cme"
  },
  "indicator_bindings": [
    { "indicator_id": "ind_breakout" },
    { "indicator_id": "ind_regime" }
  ],
  "instrument_bindings": [
    {
      "instrument_id": "instrument-es",
      "symbol": "ES"
    }
  ],
  "position_policy": {
    "mode": "single_active_trade_per_symbol"
  },
  "risk_policy": {
    "base_risk_per_trade": 250.0,
    "global_risk_multiplier": 1.0
  },
  "execution_policy": {
    "atm_template_id": "atm-template-id"
  },
  "decision_rules": [
    {
      "id": "rule-1",
      "name": "Long breakout in trend",
      "enabled": true,
      "priority": 100,
      "intent": "enter_long",
      "trigger": {
        "type": "signal_match",
        "indicator_id": "ind_breakout",
        "output_name": "signal",
        "event_key": "breakout_long"
      },
      "guards": [
        {
          "type": "context_match",
          "indicator_id": "ind_regime",
          "output_name": "context",
          "field": "state",
          "value": "trend"
        },
        {
          "type": "metric_match",
          "indicator_id": "ind_volatility",
          "output_name": "score",
          "field": "atr_zscore",
          "operator": ">",
          "value": 0.5
        },
        {
          "type": "signal_absent_within_bars",
          "indicator_id": "ind_breakout",
          "output_name": "signal",
          "event_key": "breakout_short",
          "lookback_bars": 3
        }
      ]
    }
  ]
}
```

## 6.2 DecisionRuleSpec

V1 rule contract:
- exactly one signal trigger,
- zero or more guards,
- one emitted intent when matched,
- optional integer priority for deterministic conflict handling.

Rule shape:

```json
{
  "id": "rule-1",
  "name": "Long breakout in trend",
  "enabled": true,
  "priority": 100,
  "intent": "enter_long",
  "trigger": {
    "type": "signal_match",
    "indicator_id": "ind_breakout",
    "output_name": "signal",
    "event_key": "breakout_long"
  },
  "guards": [
    {
      "type": "context_match",
      "indicator_id": "ind_regime",
      "output_name": "context",
      "field": "state",
      "value": "trend"
    }
  ]
}
```

Allowed authored nodes in v1:
- trigger:
  - `signal_match`
- guards:
  - `context_match`
  - `metric_match`
  - `holds_for_bars`
  - `signal_seen_within_bars`
  - `signal_absent_within_bars`

V1 authored rules do not expose:
- `any`,
- `not`,
- arbitrary boolean trees,
- overlay predicates,
- position-aware predicates.

This is intentional.
The goal is a narrow rule contract that is easy to audit and hard to misuse.

Instrument bindings follow the same principle.
If an instrument is part of the strategy, it is active.
If it should not participate, remove it from the strategy.
V1 does not carry a soft-disabled instrument state in the core contract.

### 6.2.1 Context guards

`context_match` is the canonical string/enum equality guard for v1.

Shape:

```json
{
  "type": "context_match",
  "indicator_id": "ind_regime",
  "output_name": "context",
  "field": "state",
  "value": "trend"
}
```

Expected value types:
- `value` must be a string or enum-like string value,
- referenced context field value must resolve to a string or enum-like string value.

Semantics:
- resolve the referenced typed `context` output,
- read the named `field`,
- evaluate as exact equality between `actual` and `value`,
- matching is case-sensitive in v1.

Validation boundary:
- compile time:
  - referenced output must exist,
  - referenced output must be declared as `context`,
  - `field` must be a non-empty string,
  - `value` must be a string,
  - referenced field must be declared by indicator metadata when available.
- runtime:
  - if the referenced output ref is missing from the published runtime surface, fail loud,
  - if the context output is `ready=false`, the guard evaluates `false`,
  - if the output is `ready=true` but the referenced field is missing or is not string-like, fail loud as a contract violation.

### 6.2.2 Metric comparison guards

`metric_match` is the canonical numeric comparison guard for v1.

Shape:

```json
{
  "type": "metric_match",
  "indicator_id": "ind_volatility",
  "output_name": "score",
  "field": "atr_zscore",
  "operator": ">=",
  "value": 0.5
}
```

Allowed operators:
- `>`
- `>=`
- `<`
- `<=`
- `==`
- `!=`

Expected value types:
- `value` must be numeric,
- referenced metric field value must be numeric,
- booleans are invalid metric values for v1.

Validation boundary:
- compile time:
  - referenced output must exist,
  - referenced output must be declared as `metric`,
  - referenced field must be declared by indicator metadata when available,
  - operator must be one of the allowed operators,
  - expected value must be numeric.
- runtime:
  - if the referenced output ref is missing from the published runtime surface, fail loud,
  - if the metric output is `ready=false`, the guard evaluates `false`,
  - if the output is `ready=true` but the referenced field is missing or non-numeric, fail loud as a contract violation.

V1 does not support:
- string comparisons,
- set membership,
- cross-field arithmetic,
- comparing one metric output directly to another metric output.

### 6.2.3 Temporal guards

Temporal guards are intentionally narrow in v1.

They exist only to support bounded, inspectable lookback semantics over already-published typed outputs.

Allowed temporal guards:
- `holds_for_bars`
- `signal_seen_within_bars`
- `signal_absent_within_bars`

Temporal guards read only from the same strategy/instrument runtime timeline.
They do not trigger new data fetches, reconstruct hidden history, or inspect overlays.

`holds_for_bars` shape:

```json
{
  "type": "holds_for_bars",
  "bars": 3,
  "guard": {
    "type": "metric_match",
    "indicator_id": "ind_volatility",
    "output_name": "score",
    "field": "atr_zscore",
    "operator": ">",
    "value": 0.5
  }
}
```

`holds_for_bars` allowed inner guard types:
- `context_match`
- `metric_match`

Semantics:
- evaluate the inner guard on the current bar and the previous `bars - 1` bars,
- all evaluations must be true,
- if fewer than `bars` evaluated bars exist in the current runtime timeline, the guard evaluates `false`,
- if any referenced output is `ready=false` on a required bar, the guard evaluates `false`,
- if any referenced output ref is missing or violates the declared type contract, fail loud.

`signal_seen_within_bars` shape:

```json
{
  "type": "signal_seen_within_bars",
  "indicator_id": "ind_breakout",
  "output_name": "signal",
  "event_key": "breakout_long",
  "lookback_bars": 3
}
```

`signal_absent_within_bars` shape:

```json
{
  "type": "signal_absent_within_bars",
  "indicator_id": "ind_breakout",
  "output_name": "signal",
  "event_key": "breakout_short",
  "lookback_bars": 3
}
```

Signal temporal semantics:
- lookback is inclusive of the current bar,
- `signal_seen_within_bars` is true when the referenced signal event occurs on any bar in the bounded window,
- `signal_absent_within_bars` is true when the referenced signal event occurs on no bar in the bounded window,
- if fewer than `lookback_bars` evaluated bars exist, evaluate over the available window only,
- if a referenced signal output is `ready=false` on a checked bar, that bar contributes no matching event,
- if a referenced signal output ref is missing or is not typed as `signal`, fail loud.

Validation boundary:
- compile time:
  - `bars` / `lookback_bars` must be positive integers,
  - referenced output types must match the guard type,
  - inner guard type for `holds_for_bars` must be allowed.
- runtime:
  - temporal guards use only prior published outputs from the current replay/runtime timeline,
  - no implicit warmup extension or parallel history path is allowed.

## 6.3 DecisionArtifact

`DecisionArtifact` is the canonical per-rule decision read model for v1.

One decision artifact is produced for each evaluated enabled rule on each evaluated bar.

Canonical identity:
- `decision_id = "{strategy_id}:{instrument_id}:{bar_epoch}:{rule_id}"`

Canonical shape:

```json
{
  "decision_id": "strategy-1:instrument-es:1712083200:rule-1",
  "strategy_id": "strategy-1",
  "instrument_id": "instrument-es",
  "symbol": "ES",
  "timeframe": "5m",
  "bar_epoch": 1712083200,
  "bar_time": "2024-04-02T14:40:00Z",
  "decision_time": "2024-04-02T14:40:00Z",
  "rule_id": "rule-1",
  "rule_name": "Long breakout in trend",
  "priority": 100,
  "trigger": {
    "type": "signal_match",
    "output_ref": "ind_breakout.signal",
    "event_key": "breakout_long",
    "ready": true,
    "matched": true
  },
  "guard_results": [
    {
      "guard_index": 0,
      "type": "context_match",
      "output_ref": "ind_regime.context",
      "field": "state",
      "ready": true,
      "expected": "trend",
      "actual": "trend",
      "matched": true
    },
    {
      "guard_index": 1,
      "type": "metric_match",
      "output_ref": "ind_volatility.score",
      "field": "atr_zscore",
      "operator": ">",
      "expected": 0.5,
      "actual": 0.81,
      "ready": true,
      "matched": true
    }
  ],
  "evaluation_result": "matched_selected",
  "emitted_intent": "enter_long",
  "suppression_reason": null
}
```

Artifact rules:
- `bar_time` is the canonical strategy evaluation time,
- `decision_time` equals `bar_time` in v1,
- `decision_id` is deterministic from strategy/instrument/bar/rule identity,
- `trigger` records the referenced signal output and whether it matched,
- `guard_results` record only the guard inputs actually used by the rule,
- temporal guards include per-bar evaluation detail inside `guard_results`,
- temporal guard per-bar detail is bounded to the guard lookback window and remains deterministic for the same runtime timeline,
- `evaluation_result` is one of:
  - `not_matched`
  - `matched_selected`
  - `matched_suppressed`
- `emitted_intent` is populated only when the rule matched,
- `suppression_reason` is populated only for `matched_suppressed`.

Confidence:
- v1 does not include `confidence`, probability, or model-score fields in the canonical decision artifact,
- if research surfaces compute auxiliary scores, they are not part of the canonical decision contract and are not decision inputs.

Decision artifacts are suitable for:
- logging,
- preview read models,
- playback inspection,
- downstream rejection correlation,
- later AI analysis over deterministic, structured records.

## 6.4 RuleResolutionSpec

V1 allows all enabled rules to be evaluated, but it allows at most one selected decision candidate per instrument/bar.

Rule priority:
- each rule may define integer `priority`,
- default `priority` is `0`,
- higher integer wins.

Compile-time ambiguity rule:
- if two enabled rules in the same strategy emit different intents and have the same priority, compile fails,
- same-priority rules emitting different intents are forbidden by design in v1, even if they appear logically mutually exclusive,
- v1 does not attempt to prove mutual exclusivity because determinism and auditability take precedence over expressive rule packing.

Runtime resolution:
- evaluate all enabled rules,
- build one decision artifact per evaluated rule,
- collect matched rules,
- sort matched rules by:
  1. `priority` descending
  2. `rule_id` ascending
- select the first matched rule as the canonical decision candidate for that instrument/bar,
- mark remaining matched rules as `matched_suppressed` with `suppression_reason="higher_priority_rule_selected"`.

This keeps v1 deterministic without introducing a broad policy engine.

## 6.5 DecisionIntent

V1 decision layer emits entry intents only:
- `enter_long`
- `enter_short`

Exit management in v1 remains outside the decision layer and stays owned by execution policy / ATM behavior.

That keeps the strategy layer aligned with the current ladder/ATM runtime model instead of introducing speculative abstraction.

The selected decision candidate exposes the emitted intent plus the originating `decision_id`.

## 6.6 PositionPolicySpec

Position policy is explicit and separate from rule logic.

V1 policy:

```json
{
  "mode": "single_active_trade_per_symbol"
}
```

Meaning:
- if a trade is already active for a symbol, new entry intents for that symbol are rejected downstream with an explicit reason.

This is a deliberate contract choice for v1.
It is not left as an accidental runtime behavior.

## 6.7 RiskPolicySpec

Risk policy remains separate from decision rules.

V1 risk policy should keep using the current canonical fields already supported by runtime:
- `base_risk_per_trade`
- `global_risk_multiplier`
- instrument-scoped overrides only where a canonical runtime field already exists

Risk policy does not decide whether a rule matched.
It decides how much can be sized once an intent exists.

## 6.8 ExecutionPolicySpec

Execution policy remains separate from decision rules and position policy.

For v1, the existing ATM template contract remains the execution-policy surface.

That means:
- ATM templates are kept,
- ATM normalization is kept,
- trade lifecycle management remains in bot runtime,
- decision rules do not embed stop/target/execution details.

## 6.9 RejectionArtifact

`RejectionArtifact` is the canonical downstream rejection read model for a selected decision candidate that was not accepted by the next layer.

Rejections are downstream from decision evaluation.
They are not emitted for rules that simply did not match.

Canonical shape:

```json
{
  "rejection_id": "strategy-1:instrument-es:1712083200:rule-1:position_policy",
  "decision_id": "strategy-1:instrument-es:1712083200:rule-1",
  "strategy_id": "strategy-1",
  "instrument_id": "instrument-es",
  "symbol": "ES",
  "bar_epoch": 1712083200,
  "bar_time": "2024-04-02T14:40:00Z",
  "intent": "enter_long",
  "rejection_stage": "position_policy",
  "rejection_code": "ACTIVE_TRADE_ALREADY_OPEN",
  "rejection_reason": "Active trade already open for symbol",
  "context": {
    "blocking_trade_id": "trade-123"
  }
}
```

Allowed `rejection_stage` values in v1:
- `position_policy`
- `risk_policy`
- `execution_policy`

Artifact rules:
- every rejection must reference a prior `decision_id`,
- `rejection_id` must be deterministic from `decision_id` and `rejection_stage`,
- `rejection_code` is machine-oriented,
- `rejection_reason` is human-oriented,
- `context` is optional but must remain structured and bounded,
- `context` must contain only minimal, stage-specific diagnostic fields and must not duplicate the full decision artifact or indicator state.

Examples:
- position policy:
  - active trade already open for symbol
- risk policy:
  - base risk missing
  - quantity below minimum
  - notional below minimum
- execution policy/runtime:
  - limit maker params invalid
  - order intent rejected by adapter/runtime

Rejection artifacts exist to answer:
- what decision was rejected,
- where it was rejected,
- why it was rejected,
- what structured context explained the rejection.

## 7) What stays, what changes

### 7.1 Contracts retained

These are kept and reused:
- typed indicator output contract,
- `IndicatorExecutionEngine`,
- canonical instrument/runtime profile compiler,
- ATM template normalization contract,
- bot runtime ownership of fills, costs, and lifecycle outcomes.

### 7.2 Contracts replaced or removed

These should be converged or retired:
- preview-only decision path implementations,
- legacy rule storage semantics (`match`, generic `conditions` blobs as the real contract),
- evaluator features not exposed by authored strategy specs,
- strategy/runtime dual paths that rebuild the same decision artifact differently.

### 7.3 QuantLab boundary

QuantLab remains research-only.

QuantLab should:
- share indicator walk-forward runtime semantics,
- share the typed indicator output contract,
- not become a bot runtime or strategy execution surface.

QuantLab is allowed to remain a distinct research executor.
The canonical convergence target is between:
- strategy preview,
- bot runtime decision evaluation.

## 8) Why this architecture

- It preserves the existing strong parts of the stack.
- It removes semantic drift between preview and runtime.
- It keeps decision logic auditable.
- It avoids speculative abstractions around portfolio logic and multi-position management.
- It gives AI agents and UI code one stable contract to author against.

## 9) Tradeoffs

- Narrow v1 rule grammar reduces expressiveness in exchange for reliability.
- Bounded temporal guards increase rule usefulness without introducing a general history DSL.
- Single-active-trade-per-symbol blocks stacked same-symbol entries in v1.
- Compiler-first execution adds upfront validation work before runtime.
- Preview/runtime convergence requires deleting working but redundant code paths.
- QuantLab remains a separate research surface instead of becoming a universal execution simulator.

## 10) Risks accepted

- Existing implementation paths may remain partially unconverged until the canonical evaluator is wired end-to-end.
- UI affordances may temporarily lag the canonical backend contract during implementation.
- Deleting legacy paths can surface hidden dependencies that were previously masked.
- Restricting v1 to single active trade per symbol may defer some valid strategy patterns.

## 11) Strict contract

- Strategies consume typed indicator outputs only.
- Overlays are not decision inputs.
- Raw `StrategySpec` is not executed directly; compile first.
- Strategy preview and bot runtime must use the same compiled decision evaluator.
- Decision evaluation is readonly and side-effect free.
- Decision evaluation produces one canonical `DecisionArtifact` per evaluated enabled rule.
- Decision artifacts are deterministic and keyed by strategy/instrument/bar/rule identity.
- Decision layer v1 selects at most one decision candidate per instrument/bar after deterministic rule resolution.
- Decision layer v1 emits entry intents only (`enter_long`, `enter_short`).
- Position policy is explicit and enforced downstream from decision evaluation.
- V1 position policy is `single_active_trade_per_symbol`.
- Metric guards are numeric-only and use explicit comparison operators.
- Temporal guards are limited to `holds_for_bars`, `signal_seen_within_bars`, and `signal_absent_within_bars`.
- V1 does not include confidence scores in the canonical decision artifact.
- Risk policy and execution policy are separate concerns and must not be embedded inside decision rules.
- QuantLab shares indicator runtime semantics, not bot execution semantics.
- Multiple matched rules must resolve by `priority desc, rule_id asc`, with same-priority opposite-intent rules rejected at compile time.
- Missing output refs, type mismatches, or invalid rule specs fail loud with actionable context.

Failure behavior:
- invalid strategy spec: fail at compile time,
- invalid output reference/type/operator/value/temporal shape: fail at compile time when possible, otherwise fail loud at evaluation,
- metric output `ready=false` on a required bar: guard evaluates `false`,
- temporal guards do not trigger implicit history extension; insufficient history evaluates `false` according to the guard contract,
- preview/runtime divergence: treat as a contract violation,
- downstream entry rejection because of active trade/risk/execution constraints: emit an explicit `RejectionArtifact`.

## 12) Validation hooks

- code:
  - `StrategyCompiler` compile-time validation,
  - one canonical decision evaluator shared by preview/runtime,
  - deterministic rule resolution,
  - explicit policy enforcement boundaries.
- logs:
  - decision artifacts logged with `decision_id`, `rule_id`, symbol, timeframe, instrument ID, bar time,
  - rejection artifacts logged with `decision_id`, `rejection_stage`, `rejection_code`,
  - compile failures with actionable field references,
  - preview/runtime parity mismatch as a contract violation.
- storage:
  - strategy spec version,
  - decision artifacts and downstream rejection artifacts where required by consumers.
- tests:
  - preview == runtime decision parity on the same candles,
  - compile failure coverage for invalid output refs/types/operators/priority conflicts,
  - metric guard evaluation coverage,
  - temporal guard evaluation coverage,
  - single-active-trade-per-symbol rejection coverage,
  - decision artifact determinism coverage,
  - rejection artifact correlation coverage.

## 13) Versioning

- `StrategySpec` carries an explicit v1 version.
- This document defines the v1 contract only.
