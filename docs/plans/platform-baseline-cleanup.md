# Platform Baseline Cleanup

## Baseline

- Integration branch: `feat/platform-baseline-cleanup`
- Starting commit: `1724240aa33950b6d941fabe467822b431f8010d`
- Starting state: clean and published to `origin/feat/platform-baseline-cleanup`
- Final target: one reviewable PR from `feat/platform-baseline-cleanup` to `develop`
- Scope exclusions: frontend redesign, L2/order flow, market-state expansion,
  options, live trading, and new strategy features

This is the persistent execution ledger for the cleanup. `AGENTS.md`, the
platform contracts, and component architecture documents remain the normative
engineering guidance; this file records only cleanup-specific state.

## Workstreams

| Workstream | Child branch | Status | Depends on |
| --- | --- | --- | --- |
| Candle-continuity extraction | integration baseline | Complete | — |
| Canonical lifecycle ledger | `feats/lifecycle-canonical-ledger` | In progress | baseline |
| Strict execution contracts | `feats/execution-contract-strictness` | Pending | lifecycle integration |
| Compatibility/dead-path removal | `feats/compatibility-dead-path-removal` | Pending | strict execution contract ownership |
| Temporal/config ownership | `feats/config-temporal-ownership` | Pending | compatibility caller migration |
| Baseline hygiene | `feats/baseline-hygiene` | Pending | structural ownership settled |
| Correctness evidence campaign | `feats/correctness-evidence-campaign` | Pending | all structural cleanup |

## Canonical Decisions

- `portal_bot_run_events` / `BotRunEventRecord` is the lifecycle event ledger.
- `portal_bot_runs` is a rebuildable current-run summary, not lifecycle history.
- Raw execution configuration must compile once into a strict execution plan;
  runtime code must not reinterpret or silently repair it.
- Backtest evaluation, indicator warmup, runtime recovery, overlay retention,
  strategy lookback, research range, and transport replay remain distinct.
- Existing continuity, provenance, readiness, diagnostics, caveat, and trust
  contracts remain canonical until tests prove a structural gap.
- `qt` is the canonical operator/agent workflow. MCP remains a thin optional
  adapter over shared CLI/API/domain contracts.
- Frontend validation is optional for this campaign; tracked Vite cache is not
  source.

## Schema Strategy

Lifecycle migration artifacts must be classified before editing as clean
bootstrap input, active upgrade path, or historical record. The lifecycle
workstream will choose and document one coherent hard-cutover strategy. Runtime
bootstrap changes require a demonstrated clean-start or idempotency defect.

## Progress and Validation

| Date | Commit/workstream | Evidence |
| --- | --- | --- |
| 2026-07-24 | `1724240` candle-continuity extraction | focused continuity/reporting: 54 passed; reporting: 37 passed; PR profile: 851 passed, 286 deselected; docs: 2 passed |
| 2026-07-24 | integration branch publication | pushed without history rewrite; upstream configured |

Each child branch must record its focused tests, broader regression profile,
documentation validation, diff review, and remaining-reference search before
integration.

## Discovered Risks

- Lifecycle truth is duplicated across the canonical event ledger and two
  legacy mirror tables; fleet/latest reads still depend on a mirror.
- ATM and execution-plan normalization can silently skip or weaken malformed
  rules.
- Backtest warmup shortfalls and some malformed/empty provider data are not
  always surfaced in quality evidence.
- Report observability reads can truncate at 2,000 events without a truncation
  caveat.
- Broad storage facades and compatibility aliases obscure canonical ownership.
- Similar temporal names currently hide distinct semantics and duplicated
  dispatch ownership.

## Blockers and Deviations

- Blockers: none.
- Deviations from the initial inventory: none yet.
- Database clean-start and repeated-bootstrap validation is intentionally
  deferred until structural schema cleanup is integrated.

## Final Acceptance

- [ ] One canonical lifecycle ledger; no mirrors or fallback reads
- [ ] Explicit, reconstructable run summary projection
- [ ] Malformed execution configuration fails before runtime
- [ ] Proven dead and compatibility-only production paths removed
- [ ] Explicit storage ownership and nonduplicated temporal dispatch
- [ ] Accurate backend CI with optional frontend checks
- [ ] Deterministic reference and repeated backtests
- [ ] No-lookahead checks
- [ ] Backtest and paper/runtime replay agreement under equal assumptions
- [ ] Order, fill, position, lifecycle, wallet, fee, P&L, and equity reconciliation
- [ ] Quality/provenance/readiness/trust evidence preserved end to end
- [ ] Clean and repeated database bootstrap validation
- [ ] Architecture and operator documentation aligned
- [ ] Integration branch clean, pushed, and ready for review
