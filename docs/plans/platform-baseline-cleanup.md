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
| Baseline hygiene | `feats/baseline-hygiene` | Complete | structural ownership settled |
| Correctness evidence campaign | `feats/correctness-evidence-campaign` | Complete | all structural cleanup |

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
| Implicit 20-tick breakeven movement | DELETE | Completed in `66aac0b` and `c5d3c76`: omitted breakeven and stop-adjustment policy compiles to disabled behavior, and the domain position default is zero. Breakeven movement now requires explicit configuration. |
| Playback `mode` fallback into `execution_mode` | CONSOLIDATE | Completed in `a5ef7de`: execution mode defaults at its own owner, accepts only `fast`/`full`, and rejects playback values. |
| Indicator overlay-history interface and Candle Stats/Regime implementations | KEEP | Renamed in `a5ef7de` to the explicit `configure_overlay_history` contract. All three definitions mean render-only retention, not research range, evaluation, recovery, or warmup. |
| Four replay-window dispatch helpers | CONSOLIDATE | Completed in `a5ef7de`: indicator preview, runtime validation, strategy preview, and bot setup use one fail-loud `configure_indicator_overlay_history` owner. |
| Research range, backtest evaluation, indicator warmup, runtime recovery, transport replay | KEEP | These are distinct windows with different clocks, failure policy, and consumers. Similar names are not evidence of duplication. |
| Continuity, candle catalog, readiness, provenance, diagnostics, confidence, and caveats | KEEP | Created in dataset/report builders, finalized in readiness, persisted with report fingerprints/materializations, exported in report bundles, and displayed through CLI/MCP report resources and research-check results. |
| Warmup/provider/truncation quality omissions | CONSOLIDATE | Completed in `38cf782`, `84dbf93`, and `0d8118f`: backtest warmup evidence is explicit, malformed candle frames fail before evaluation, and bounded observability reads expose coverage/truncation caveats. The existing envelope remains canonical. |
| Tracked `portal/frontend/.vite/deps` | DELETE | Completed in `7338f56`: the generated cache was removed, ignored, and a repository-wide tracked-artifact audit is clean. |
| Filename-routed PR profile and stale controller test seam | DELETE | Completed in `35949fe`: the full non-database backend suite replaces the 70-line filename allowlist, and the overlay logging test now fails through the current metadata-service boundary. |
| Commented changelog workflow experiment | DELETE | Completed in `7338f56`: the workflow had no executable path. The remote `test` branch still exists, so its active CI trigger remains. |
| CLI operations | KEEP | `qt` is the canonical operator contract. Every invocation writes a redacted structured audit by default and API calls record method, URL, status, duration, and byte counts. Direct mutation commands do not consistently require plan/apply/confirm, and `--no-audit-log` can disable the record, so unrestricted CLI access is not yet an agent-safe boundary. |
| MCP operations | VERIFY USAGE | All 42 registrations have handlers and no orphan definitions were found. Mutating tools require confirmation and usually plan by default; paper/live starts require an additional opt-in. External invocation is not visible in the internal call graph, so retain the thin optional adapter until usage evidence supports tool-level deletion. |
| Missing bridge-session fallback to `"legacy"` | VERIFY USAGE | Verify every producer supplies a session identity before changing ingestion behavior. |

## Scale and Agent-Safety Audit

| Area | Current protection | Remaining gap |
| --- | --- | --- |
| Report materialization | Input fingerprints, cached materializations, and a single-worker executor prevent duplicate concurrent builds. | `run_research_dataset.py` remains a 5,638-line concentration point mixing projection, quality, readiness, metrics, and presentation assembly. |
| Runtime projection | Bounded transport queues, revision cursors, batched persistence, and explicit overflow/degradation signals protect runtime truth. | `runtime_push_stream.py` is 3,037 lines and mixes event translation, persistence, diagnostics, transport, and lifecycle projection. |
| Runtime domain | Deterministic policies and reference scenario tests protect entry, exit, fill, lifecycle, and accounting behavior. | `core/domain/engine.py` is 2,028 lines and still combines sizing, admission, order/fill orchestration, position creation, and summary projection. |
| Setup and research | Async jobs are fingerprinted, partitioned, retryable, and workers use bounded configured pools. | `setup_prepare.py` is 1,913 lines, `research/checks.py` is 2,023 lines, and series-link loading sizes a thread pool directly from eligible-link count. |
| CLI and MCP | CLI audits by default; MCP delegates to shared CLI/API contracts and guards mutation. | `cli/main.py` is 3,351 lines; CLI audits do not persist response payloads or a uniform validation/caveat envelope, and direct CLI mutations lack uniform confirmation gates. |

## Unsupported or Deferred Workflows

- No standalone `qt backtest` command or credential-free canonical seed workflow
  exists. End-to-end CLI backtests require persisted instruments, candles,
  strategies, indicators, and bots.
- Paper observe-only intake currently supports bounded Coinbase streams. A
  credential-free paper/runtime replay over a persisted reference dataset is not
  exposed as an operator workflow.
- The generic `/api/health` route is an uptime probe, not a database-readiness
  check; database-backed reads or startup contract logs are required.
- Margin selection is conservative when session state is unavailable; full
  session-aware execution is not implemented.
- Golden disagreement traces expose absent order/fill/accounting evidence as
  unavailable; they cannot reconstruct facts that a run did not persist.
- MCP host registration is optional and was not configured in the validated
  local Codex environment.
- Frontend modernization, L2/order flow, market-state expansion, options, and
  live trading remain outside this campaign.

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
| 2026-07-25 | `35949fe` baseline hygiene | formerly hidden controller test: 1 passed; full non-database backend gate: 1,186 passed in 19.61s; docs: 2 passed; runner shell syntax, Make target expansion, whitespace, tracked-artifact, workflow, and retired-profile reference audits passed; frontend checks remain explicit opt-in |
| 2026-07-25 | `81498a9..84dbf93` deterministic evidence and quality | hand-verifiable long/short, stop/target/same-bar, gap-fill, repeatability, backtest/paper parity, wallet/accounting/lifecycle reconciliation, full disagreement traces, auditable warmup, and observability coverage added; malformed execution and candle inputs fail loudly |
| 2026-07-25 | `66aac0b..2e80c2f` exit and causal boundaries | implicit breakeven removed; staged/terminal exits reconcile; every strategy input must be a same-bar `RuntimeOutput`; future market-profile facts, research events, and report projections cannot alter prior decisions |
| 2026-07-25 | `6fb995a` database ownership fixture | opt-in PostgreSQL profile: 3 passed; full suite against TimescaleDB: 1,260 passed, 47 pre-existing dependency/deprecation warnings, 22.55s |
| 2026-07-25 | clean/repeated TimescaleDB bootstrap | isolated canonical startup and restart both produced 24 tables, 64 indexes, required extensions, and schema fingerprint `3ca3bf80c5b92e7883ecc066c5327495f234ff9eb047fb562a3d95d859544482`; normal empty legacy lifecycle tables were removed through the guarded migration |
| 2026-07-25 | `c5d3c76` domain breakeven default | affected execution profile: 47 passed; direct `LadderPosition` construction now defaults to disabled breakeven; final full suite with PostgreSQL enabled: 1,261 passed, 47 pre-existing dependency/deprecation warnings, 25.67s |

Each child branch must record its focused tests, broader regression profile,
documentation validation, diff review, and remaining-reference search before
integration.

## Discovered Risks

- There is no repository-defined, credential-free, persisted end-to-end
  backtest/paper fixture for exercising the complete CLI/API/job path.
- Direct CLI mutation is audited but is not uniformly plan/apply/confirm gated;
  agents should use approved wrappers until that boundary is tightened.
- API uptime health can succeed while database readiness is unavailable.
- The largest orchestration modules remain costly to reason about and review;
  extraction should continue only behind the established regression evidence.
- Session-aware margin behavior and reconstruction of evidence never persisted
  by a run remain unsupported and must stay explicit in reports.

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
- A representative persisted real-strategy run was not fabricated: canonical
  stores are empty and no approved credential-free seed/ingest command exists.

## Final Acceptance

- [x] One canonical lifecycle ledger; no mirrors or fallback reads
- [x] Explicit, reconstructable, transactionally updated run summary projection
- [x] Malformed canonical execution configuration fails before runtime
- [x] Proven dead and compatibility-only production paths removed
- [x] Explicit storage ownership and nonduplicated temporal dispatch
- [x] Accurate backend CI with optional frontend checks
- [x] Deterministic reference and repeated backtests
- [x] No-lookahead checks
- [x] Backtest and paper/runtime replay agreement under equal assumptions
- [x] Order, fill, position, lifecycle, wallet, fee, P&L, and equity reconciliation
- [x] Quality/provenance/readiness/trust evidence preserved end to end
- [x] Clean and repeated database bootstrap validation
- [x] Architecture and operator documentation aligned
- [ ] Integration branch clean, pushed, and ready for review
