# Runtime Contract

## Canonical Runtime Flow

All derived outputs should follow one timeline:

1. initialize runtime components
2. process one bar in dependency order
3. mutate indicator-owned state via `apply_bar(bar, inputs)`
4. publish typed outputs via `snapshot()`
5. publish optional canonical overlays via `overlay_snapshot()`
6. publish optional runtime details via `detail_snapshot()`
7. evaluate strategies from published typed outputs only
8. publish canonical decision artifacts from the same bar result
9. build downstream read models and downstream rejection artifacts from the same bar result

## Artifact Contract

Indicators are computation units with internal state.

Public runtime surfaces are:
- typed outputs for strategy/runtime truth,
- optional canonical overlays for chart rendering,
- optional runtime details for operator/debug inspection.

Rules:
- outputs are the only strategy-visible indicator interface,
- overlays are not strategy inputs,
- runtime details are not strategy inputs,
- decision artifacts must derive from the published typed outputs for the same bar,
- indicator overlays represent the full current visual state for the bar,
- indicator details are non-core inspection artifacts and must stay separate from render overlays,
- indicators may prepare immutable source facts before walk-forward execution when those facts are true source inputs rather than reconstructed chart history,
- indicators must not prebuild full chart-history overlays before walk-forward execution starts,
- `apply_bar()` advances indicator-owned state only; it must not rebuild full-history overlay payloads on every bar,
- `overlay_snapshot()` is a read of current indicator state and may be requested selectively by consumers,
- `detail_snapshot()` is a read of current indicator state and may be transported independently from overlays,
- chart readouts that depend on the same live timeline should prefer canonical overlay payloads over a parallel detail refetch path,
- runtime transport may diff those full overlay snapshots and stream only deltas downstream,
- every declared output must be returned every bar,
- every declared overlay must be returned every bar,
- every declared detail must be returned every bar,
- `ready=False` means unusable now, not pending,
- runtime never waits, retries, or substitutes missing values,
- runtime and preview consumers must not fetch overlays through a parallel overlay service path.
- runtime and preview consumers must not reconstruct decision artifacts through a parallel rule-evaluation path.

## Cache Contract

Caching is valid only when it preserves runtime semantics:
- key includes semantic inputs
- outputs match non-cached walk-forward execution
- output readiness and overlay visibility semantics are unchanged

## Single-Path Rule

Do not add alternate reconstruction paths for the same artifact class.

Rules:
- strategies must not inspect indicator internals,
- downstream overlay consumers must not reinterpret indicator-local overlay blobs,
- if a surface needs overlay history, it must assemble that history from the runtime timeline instead of asking indicators to rebuild it inside `apply_bar()`,
- if required data is missing from the public runtime surface, extend the contract instead of reading hidden state,
- request/read paths must consume already-available projector/runtime snapshots or return an explicit unavailable state; they must not trigger ledger replay as an alternate reconstruction path.

## BotLens Selected-Symbol Reads

Normal BotLens selected-symbol switching is a projector-backed read model.

Rules:
- the standard selected-symbol route reads canonical `RunProjectionSnapshot` and `SymbolProjectionSnapshot` state,
- projector/bootstrap infrastructure may lazily ensure a missing projector once, but the selected-symbol read itself is not a replay boundary,
- the standard selected-symbol response must carry the normal BotLens symbol view state from projector snapshots in one contract,
- if projected symbol state is unavailable, the response must say so explicitly instead of fabricating an empty symbol base,
- selected-symbol websocket subscription updates must carry the selected-symbol snapshot resume cursor (`resume_from_seq` / `base_seq`) and stream session when available,
- the server must replay selected-symbol deltas with `stream_seq > resume_from_seq` after the subscription changes, or emit `botlens_live_reset_required` if the replay window cannot prove continuity,
- clients must still reject stale selected-symbol snapshots and must not apply symbol deltas before the symbol has an initialized base state,
- debugger, history, and forensics flows may use explicit reconstruction/query paths, but those paths must remain separate from the normal interaction path.

## BotLens Projection Failure Semantics

Projector rebuild failure is not an empty valid projection.

Rules:

- run projector rebuild failures must surface `health.status=projection_error`, readiness false, and a bounded fault explaining the failed rebuild,
- symbol projector rebuild failures must surface a `projection_error` diagnostic and `snapshot_ready=false`,
- selected-symbol reads over a failed symbol projection must return an explicit unavailable/projection-error state,
- error details must be bounded and operationally useful, not raw unbounded persisted payloads,
- downstream UI/service paths must distinguish "empty but valid" from "projection unavailable".

## BotLens Readiness Semantics

BotLens readiness is split into one explicit contract vocabulary:

- `catalog_discovered`: the symbol exists in run navigation/catalog state
- `snapshot_ready`: the selected-symbol snapshot exists with usable base state
- `symbol_live`: the symbol projector has observed first live runtime state for that symbol
- `run_live`: run-level live criteria are satisfied

Rules:

- these states must be exposed explicitly on BotLens run/selected-symbol contracts instead of being inferred from cache presence or a vague `ready` boolean,
- `live_transport.eligible` is transport eligibility only; it is not `run_live`,
- `catalog_discovered` must not imply `snapshot_ready`,
- `snapshot_ready` must not imply `symbol_live`,
- unavailable selected-symbol snapshots must remain honest and return `snapshot_ready=false` instead of fabricated empty state,
- `contract_state` may distinguish bootstrap/snapshot contract phases for consumers, but the readiness booleans remain the semantic source of truth.

## BotLens Candle Continuity Audit Surface

BotLens candle-gap auditing uses the existing observability substrate, not a parallel tracing path.

Rules:

- continuity signals are emitted as compact seam summaries, not per-candle logs,
- the minimum audit boundaries are source fetch/admission, selected-symbol snapshot assembly, and final full-run per-series summary,
- every detected gap must be classified as `expected_session_gap`, `provider_missing_data`, `ingestion_failure`, or `unknown_gap`,
- classification is deterministic and conservative; unavailable calendar/session proof means `unknown_gap`,
- expected session gaps do not count as defects, while provider, ingestion, and unknown gaps remain visible as defects/investigation items,
- summaries carry `candle_count`, `first_ts`, `last_ts`, expected interval, duplicate/out-of-order/missing-OHLCV counts, classified gap counts, largest/max gap severity, continuity ratio, and `final_status`,
- final run summaries must be emitted per run/series so a dashboard cannot look healthy while full-run persistence contains cross-batch gaps,
- continuity summaries must stay scoped by run and series so the next fresh-run audit can identify the first broken boundary directly.
