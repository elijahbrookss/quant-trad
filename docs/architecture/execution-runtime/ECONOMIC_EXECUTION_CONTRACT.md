---
component: economic-execution-contract
subsystem: execution-runtime
layer: boundary
doc_type: architecture
status: active
tags:
  - execution
  - economics
  - research
  - reproducibility
  - quality
  - autonomy
code_paths:
  - src/engines/bot_runtime/core/execution_assumptions.py
  - src/engines/bot_runtime/core/execution_runtime.py
  - src/engines/bot_runtime/core/execution.py
  - src/engines/bot_runtime/core/execution_profile.py
  - src/engines/bot_runtime/core/domain/engine.py
  - src/engines/bot_runtime/core/domain/position.py
  - src/engines/bot_runtime/strategy/series_builder_parts/series_construction.py
  - portal/backend/service/bots/runtime_control_service.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/reports/run_research_dataset.py
  - portal/backend/service/reports/comparison.py
  - cli/experiments/contracts.py
  - cli/experiments/runner.py
  - cli/experiments/pass_gates.py
  - tests/integration/runtime/test_execution_assumptions.py
---
# Bar-Execution Economic Contract

## Scope

This is the implemented economic-truth floor for bar execution. It does not
claim venue-book realism, partial-fill realism, queue position, latency,
capacity, shadow calibration, or live calibration. Those remain separate
follow-on capabilities and do not expand the claims of this contract.

The contract preserves the existing canonical order-plan, fill, position,
wallet, event-ledger, replay, and reporting owners. It adds a run-scoped
economic interpretation and a versioned assumption bundle; it does not create a
second execution or accounting ledger.

## Ratified semantics

The human decisions governing this capability are:

1. `golden` remains a deterministic reproducibility and reconciliation
   certificate. It does not certify economic realism, scientific validity,
   product-economic completeness, promotion eligibility, or deployment safety.
2. Reports expose those dimensions separately as `reproducibility_status`,
   `execution_quality_class`, `scientific_quality_class`,
   `instrument_economics_class`, and `promotion_eligibility`.
3. X2 passive limits use strict penetration. A buy limit requires
   `bar.low < limit_price`; a sell limit requires `bar.high > limit_price`.
   Touch-only behavior is retained only by a pinned older model and cannot earn
   X2.
4. `economic_claim_intent` is required and immutable for each newly started
   run. A completed exploratory run cannot later become selection evidence; it
   must be rerun under the stricter intent and manifest.
5. L2 collection and replay remain independent. The bar-execution model
   consumes bars only and does not wire provider-specific book events into
   generic execution code.

## Run-start contract

`economic_claim_intent` accepts exactly:

| Intent | Meaning for bar-execution economics |
| --- | --- |
| `exploration` | Signal or compatibility research. It may resolve to X0 and cannot nominate or promote a candidate. |
| `economic` | Requires complete bar-execution economic assumptions and evidence. |
| `selection` | Adds non-empty experiment gates, a baseline comparison, golden reproducibility evidence, and a minimum X2 comparison class. |
| `promotion` | Uses the same fail-closed research gates but remains ineligible for actual promotion until the required governance capabilities exist. |

The HTTP run-start contracts require the intent in the request. Internal
service construction may read an explicitly configured bot intent, but no
boundary silently invents one. Start resolution writes the intent and the
resolved `execution_assumptions.v1` manifest into the immutable run snapshot;
both therefore participate in the run configuration hash.

The manifest records:

- schema and execution-model version;
- intent and resolution source;
- market-order and stop-order adverse BPS;
- passive-fill policy;
- fee policy;
- the explicit full-fill limitation;
- deterministic cost-stress scenarios;
- the maximum X class the assumptions can support; and
- a SHA-256 hash over the normalized material fields.

Booleans and finite non-negative numbers are type-checked rather than coerced.
Stress scenarios cannot reduce base fee costs, and an economic intent requires
at least one genuinely adverse scenario.

## Versioned behavior

| Model version | Behavior | Maximum class |
| --- | --- | --- |
| `legacy_bar_touch.v1` | Historical touch/full-fill compatibility with explicit-zero exploratory economics. | X0 |
| `costed_bar.v1` | Explicit fees and adverse market/stop BPS; passive touch behavior remains. | X1 |
| `conservative_bar.v1` | X1 economics plus strict passive penetration and disclosed full fills. | X2 |

Version pinning is the compatibility mechanism. Historical behavior is not
changed in place. Direct legacy constructors receive
`legacy_internal_default`; production run starts resolve and persist their own
manifest.

## Execution and fee evidence

One resolved assumption object is supplied to entry execution, exit execution,
backtest/paper adapters, and position target checks. It applies adverse BPS in
side-aware direction:

- market entry uses `market_slippage_bps`;
- triggered stop exit uses `stop_slippage_bps`;
- passive maker fills receive no synthetic BPS but must satisfy their pinned
  touch or penetration rule;
- requested price, fill price, applied BPS, model version, manifest hash, claim
  intent, fee policy, full-fill limitation, and quality ceiling are emitted as
  fill metadata.

The instrument execution profile distinguishes an absent fee schedule from an
explicit or verified-zero schedule and versions the resolved fee contract.
Economic runs reject unresolved schedules and unverified zero schedules before
risk acceptance. Fee role, source, version, rate, and paid amount continue into
canonical fill, wallet, position, trade, and report paths.

## X0-X2 assessment

The manifest sets only a ceiling. `RunResearchDataset` grants the attained class
after revalidating the pinned manifest and requiring matching evidence:

- valid manifest and hash;
- configured fee evidence with no `default_zero` source;
- per-fill fee and slippage facts when trades exist; and
- deterministic cost-stress evidence for X1 or X2.

Any missing or contradictory item deterministically downgrades the report to
X0 and records blocking reasons. Economic, selection, and promotion intent then
surface `economic_execution_quality_unqualified`; exploration may remain X0
without being misrepresented as economic evidence.

Comparisons accept `minimum_execution_quality_class`. Experiment selection and
promotion plans fix that minimum at X2, require golden evidence, a baseline
comparison, and non-empty pass gates. The bar-execution report sets
`scientific_quality_class` to S0 and `promotion_eligibility` to `ineligible`, so
X2 never implies autonomous promotion.

## Cost-stress evidence

`execution_cost_stress.v1` is a deterministic fixed-fill turnover
counterfactual. For each pinned scenario it applies additional adverse BPS to
entry-plus-exit turnover and increases positive fee costs. If the canonical fee
total is a net rebate, stress discounts that rebate toward zero instead of
making it more favorable. The report then emits stressed net PnL and an evidence
hash.

This is deliberately narrower than re-simulating decisions and fills. It can
show sensitivity of realized bar-level turnover to higher costs; it cannot show
order disappearance, different stops/targets, depth effects, queue effects, or
capacity. Later execution models must add new versioned evidence rather than
silently expanding this method's claims.

## Rollout, compatibility, and rollback

- Newly started API runs must state their intent. CLI, MCP, frontend, and
  reporting scripts send it explicitly.
- Old snapshots with no manifest remain readable but classify X0. No migration
  upgrades their historical economic claims.
- Existing golden evidence remains usable for its narrow reproducibility
  purpose; `research_valid` is no longer derived from golden certification.
- A rollback pins the prior model version for deterministic replay but forces
  X0 when bar-execution economic evidence is absent. Rollback may reduce
  permissions; it must never preserve an unsupported higher class.
- No database migration is required because intent and assumptions live in the
  immutable JSON run snapshot and existing event/report contracts.

## Capability boundary

Under this contract, agents may execute approved costed/stressed bar experiments,
compare only compatible declared X classes, and automatically reject failed
economic gates. They still may not create unrestricted strategy code, certify
their own evidence, mutate promotion state, submit venue orders, or deploy
capital.

The venue-neutral execution-context capability adds rule, fee, instrument, and
model contracts and pins their resolved bundle per run. The canonical order
lifecycle supplies durable order state. Replay-certified book execution may
consume L2 only through those implemented boundaries and must use atomic
per-fill entry settlement for book-driven partial entries. None of these
contracts may reinterpret X2 as book-level execution evidence.
