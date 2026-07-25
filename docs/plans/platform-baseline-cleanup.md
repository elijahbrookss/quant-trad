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
| Canonical lifecycle ledger | `feats/lifecycle-canonical-ledger` | Complete | baseline |
| Strict execution contracts | `feats/execution-contract-strictness` | Complete | lifecycle integration |
| Compatibility/dead-path removal | `feats/compatibility-dead-path-removal` | Complete | strict execution contract ownership |
| Canonical ATM input schema | `feats/atm-canonical-contract` | Complete | compatibility inventory |
| Storage repository ownership | `feats/storage-repository-ownership` | Complete | compatibility caller migration |
| Temporal/config ownership | `feats/config-temporal-ownership` | Complete | storage ownership |
| Baseline hygiene | `feats/baseline-hygiene` | Pending | structural ownership settled |
| Correctness evidence campaign | `feats/correctness-evidence-campaign` | Pending | all structural cleanup |

## Canonical Decisions

- `portal_bot_run_events` / `BotRunEventRecord` is the lifecycle event ledger.
- `portal_bot_runs` is a rebuildable current-run summary, not lifecycle history.
- Raw execution configuration must compile once into a strict execution plan;
  runtime code must not reinterpret or silently repair it.
- Backtest evaluation, indicator warmup, runtime recovery, overlay retention,
  strategy lookback, research range, and transport replay remain distinct.
- Indicator overlay history is render-only and configured through one
  fail-loud dispatcher. Execution mode accepts only `fast`/`full`, defaults to
  `fast`, and is never inferred from playback.
- Existing continuity, provenance, readiness, diagnostics, caveat, and trust
  contracts remain canonical until tests prove a structural gap.
- Persistence responsibilities are owned by named repository modules. The bot
  orchestration boundary uses one explicit injectable gateway; storage packages
  do not aggregate or re-export repository functions.
- `qt` is the canonical operator/agent workflow. MCP remains a thin optional
  adapter over shared CLI/API/domain contracts.
- Frontend validation is optional for this campaign; tracked Vite cache is not
  source.

## Schema Strategy

Fresh bootstrap creates only the canonical runtime-event ledger and run summary.
The retired lifecycle bootstrap script was deleted. Existing databases use
`manual_migration_canonical_lifecycle_ledger_v1.sql`, which refuses deletion
unless legacy rows have field-equivalent canonical events. Startup rejects
retired tables until that explicit hard cutover is complete.


## Evidence Inventory

| Finding | Classification | Evidence / action |
| --- | --- | --- |
| Lifecycle models, schema, readers, writers, and tests | KEEP | `portal_bot_run_events` is the sole event ledger; `portal_bot_runs` is only the rebuildable summary. Legacy tables, fallback reads, mirrors, and synchronization code are absent after the validated hard cutover. |
| Risk service re-exports, `template_metrics`, strategy `evaluate`, provider cache alias | DELETE | Internal callers either use the canonical package already or can import it directly; wrapper-specific tests are not behavioral coverage. |
| Runtime streaming aggregate, one-thread-per-series runner, Python-version fallback, local `dotenv` shadow | DELETE | No production owners or callers; Python 3.12 and the declared `python-dotenv` dependency are canonical. |
| BotLens mailbox aliases and `bot_projection_refresh` | DELETE | No producer or useful caller remains; unknown messages already fail into bounded observability. |
| Broad `storage.storage` wildcard facade | CONSOLIDATE | Completed in `383d75b`: callers import named repository owners, bot orchestration uses one explicit gateway, package aggregates are empty, and the wildcard facade is deleted. Required gateway operations fail loudly rather than falling back through capability probes. |
| ATM aliases and nested/flattened stop-adjustment shapes | CONSOLIDATE | Completed in `7bb68f4`: schema v2 snake-case policy is the sole input contract; explicit target IDs/fractions and flattened stable-ID stop rules are required, while wrappers, aliases, implicit allocation, and instrument economics are rejected. |
| Playback `mode` fallback into `execution_mode` | CONSOLIDATE | Completed in `a5ef7de`: execution mode defaults at its own owner, accepts only `fast`/`full`, and rejects playback values. |
| Indicator overlay-history interface and Candle Stats/Regime implementations | KEEP | Renamed in `a5ef7de` to the explicit `configure_overlay_history` contract. All three definitions mean render-only retention, not research range, evaluation, recovery, or warmup. |
| Four replay-window dispatch helpers | CONSOLIDATE | Completed in `a5ef7de`: indicator preview, runtime validation, strategy preview, and bot setup use one fail-loud `configure_indicator_overlay_history` owner. |
| Research range, backtest evaluation, indicator warmup, runtime recovery, transport replay | KEEP | These are distinct windows with different clocks, failure policy, and consumers. Similar names are not evidence of duplication. |
| Continuity, candle catalog, readiness, provenance, diagnostics, confidence, and caveats | KEEP | Created in dataset/report builders, finalized in readiness, persisted with report fingerprints/materializations, exported in report bundles, and displayed through CLI/MCP report resources and research-check results. |
| Warmup/provider/truncation quality omissions | CONSOLIDATE | Surface warmup shortfall and malformed/empty provider evidence; add an explicit caveat when the 2,000-event observability read truncates. Do not replace the envelope. |
| Tracked `portal/frontend/.vite/deps` | DELETE | Generated dependency cache, not source; remove tracked files and ignore the directory. |
| Stale ignored report test and commented changelog workflow experiment | DELETE | The referenced test no longer exists; the commented OpenAI workflow has no executable path. The remote `test` branch still exists, so its CI trigger remains. |
| CLI operations | KEEP | `qt` is the canonical operator and agent contract. |
| MCP operations | VERIFY USAGE | All 42 registrations have handlers and no orphan definitions were found. External invocation is not visible in the internal call graph; keep the thin adapter until usage evidence supports tool-level deletion. |
| Missing bridge-session fallback to `"legacy"` | VERIFY USAGE | Verify every producer supplies a session identity before changing ingestion behavior. |
## Progress and Validation

| Date | Commit/workstream | Evidence |
| --- | --- | --- |
| 2026-07-24 | `1724240` candle-continuity extraction | focused continuity/reporting: 54 passed; reporting: 37 passed; PR profile: 851 passed, 286 deselected; docs: 2 passed |
| 2026-07-24 | integration branch publication | pushed without history rewrite; upstream configured |
| 2026-07-24 | `94a84a9..617f3ea` canonical lifecycle ledger | focused lifecycle/runtime/bootstrap: 59 passed; PR profile: 856 passed, 287 deselected; docs: 2 passed; isolated TimescaleDB 15 clean/repeated bootstrap, field-equivalent hard cutover, and event/summary rollback all passed |
| 2026-07-25 | `4ec2e45` strict contracts / merge `9ad7c5c` | focused execution/ATM/persistence: 57 passed; runtime profile: 371 passed, 798 deselected; PR profile: 882 passed, 287 deselected; docs: 2 passed; normalized-template idempotence, pre-persistence compilation, dormant-rule validation, and deterministic target allocation covered |
| 2026-07-25 | `7338f56` compatibility/dead-path removal / merge `ea4b63d` | focused canonical-import/runtime/BotLens/provider checks: 65 passed; child and integration PR profiles: 880 passed, 284 deselected; docs: 2 passed; backend compileall passed; deleted 75,184 lines including tracked Vite cache, unused wrappers/shims, deprecated routing, and stale CI references |
| 2026-07-25 | `7bb68f4` canonical ATM execution policy | focused ATM/runtime/strategy/reporting: 139 passed; runtime profile: 371 passed, 806 deselected; PR profile: 893 passed, 284 deselected; docs: 2 passed; backend compileall and remaining-reference audit passed; removed multi-template composition, alternative field shapes, implicit target allocation, and ATM-owned instrument economics |
| 2026-07-25 | `383d75b` storage repository ownership | focused bot/storage/reporting: 139 passed; required-gateway follow-up: 25 passed; PR profile: 893 passed, 284 deselected; docs: 2 passed; backend compileall and remaining-reference audit passed; deleted the wildcard storage facade, emptied aggregate package exports, centralized the bot gateway, and removed optional storage capability fallbacks |
| 2026-07-25 | `a5ef7de` temporal/config ownership | focused indicator/runtime/config: 105 passed; PR profile: 902 passed, 284 deselected; docs: 2 passed; affected compileall and remaining-reference audit passed; renamed visual replay hints to render-only overlay history, centralized four dispatchers, made malformed bounds fail loudly, and removed playback-to-execution inference |

Each child branch must record its focused tests, broader regression profile,
documentation validation, diff review, and remaining-reference search before
integration.

## Discovered Risks

- Backtest warmup shortfalls and some malformed/empty provider data are not
  always surfaced in quality evidence.
- Report observability reads can truncate at 2,000 events without a truncation
  caveat.

## Blockers and Deviations

- Blockers: none.
- Deviations from the initial inventory: none yet.
- The configured developer database on port 15432 rejected local credentials;
  database validation used an isolated repository-defined TimescaleDB project.
- Strict execution scope expanded to compile ATM templates at strategy and
  standalone-template persistence boundaries and to include execution-contract
  tests in the PR profile; this closes an admission-timing gap found in review.
- Independent review expanded the strict boundary to reject legacy flat stop
  input, conflicting target aliases, fractional integer shorthand, and dormant
  invalid trailing rules, and to honor target fractions during deterministic
  quantity-step allocation.
- Frontend checks remain intentionally skipped because frontend is outside the
  cleanup critical path.

## Final Acceptance

- [x] One canonical lifecycle ledger; no mirrors or fallback reads
- [x] Explicit, reconstructable, transactionally updated run summary projection
- [x] Malformed canonical execution configuration fails before runtime
- [x] Proven dead and compatibility-only production paths removed
- [x] Explicit storage ownership and nonduplicated temporal dispatch
- [ ] Accurate backend CI with optional frontend checks
- [ ] Deterministic reference and repeated backtests
- [ ] No-lookahead checks
- [ ] Backtest and paper/runtime replay agreement under equal assumptions
- [ ] Order, fill, position, lifecycle, wallet, fee, P&L, and equity reconciliation
- [ ] Quality/provenance/readiness/trust evidence preserved end to end
- [x] Clean and repeated database bootstrap validation
- [ ] Architecture and operator documentation aligned
- [ ] Integration branch clean, pushed, and ready for review
