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
| Architecture decision records | integration baseline | Complete | durable cleanup decisions and evidence |
| Async job ownership fencing | integration baseline | Complete | shared queue and research persistence ownership |

## Canonical Decisions

- `portal_bot_run_events` / `BotRunEventRecord` is the lifecycle event ledger.
- `portal_bot_runs` is the current-run summary, not lifecycle history. Its
  lifecycle status/timestamps must be ledger-projected; configuration and
  provenance fields remain independently persisted run identity.
- Raw execution configuration must compile once into a strict execution plan;
  runtime code must not reinterpret or silently repair it.
- Backtest evaluation, indicator warmup, runtime recovery, overlay retention,
  strategy lookback, research range, and transport replay remain distinct.
- Indicator overlay history is render-only and configured through one
  fail-loud dispatcher. Execution mode accepts only `fast`/`full`, defaults to
  `fast`, and is never inferred from playback.
- Existing continuity, provenance, readiness, diagnostics, caveat, and trust
  contracts remain canonical until tests prove a structural gap.
- Existing `indicator_source_candle_continuity.v1` evidence is collected by
  runtime composition, persisted with canonical series identity, and displayed
  through report context/readiness/diagnostics. It remains operational quality
  evidence, not semantic strategy identity.
- Complete-series candle continuity is producer-owned and persisted at the
  terminal run boundary independently of UI subscribers. Transport-observer
  continuity remains diagnostic and cannot certify or block material data
  quality.
- Canonical fill events are execution and accounting evidence. Spot wallet
  replay consumes those fills directly; margin wallet replay continues to use
  the existing derived margin ledger until that path is consolidated.
- Persistence responsibilities are owned by named repository modules. The bot
  orchestration boundary uses one explicit injectable gateway; storage packages
  do not aggregate or re-export repository functions.
- `qt` is the canonical operator/agent workflow. MCP remains a thin optional
  adapter over shared CLI/API/domain contracts.
- Durable lifecycle, accounting, known-at, execution/exit, dataset
  identity/quality, async fencing, agent mutation/promotion, and live-trading
  decisions require indexed ADRs. Incomplete enforcement remains proposed.
  Routine file movement and mechanical extraction do not require ADRs.
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
| Lifecycle models, schema, readers, writers, and tests | KEEP | Completed in `00440b2`: `portal_bot_run_events` is the sole lifecycle owner; direct run status/timestamp writers and fallback reads are gone; non-lifecycle run upserts reject lifecycle fields; phase/status/owner, chronology, terminality, and event-ID equivalence fail loudly; and lifecycle status/timestamps have an explicit ledger rebuild operation. |
| Risk service re-exports, `template_metrics`, strategy `evaluate`, provider cache alias | DELETE | Internal callers either use the canonical package already or can import it directly; wrapper-specific tests are not behavioral coverage. |
| Runtime streaming aggregate, one-thread-per-series runner, Python-version fallback, local `dotenv` shadow | DELETE | No production owners or callers; Python 3.12 and the declared `python-dotenv` dependency are canonical. |
| BotLens mailbox aliases and `bot_projection_refresh` | DELETE | No producer or useful caller remains; unknown messages already fail into bounded observability. |
| Broad `storage.storage` wildcard facade | CONSOLIDATE | Completed in `383d75b`: callers import named repository owners, bot orchestration uses one explicit gateway, package aggregates are empty, and the wildcard facade is deleted. Required gateway operations fail loudly rather than falling back through capability probes. |
| ATM aliases and nested/flattened stop-adjustment shapes | CONSOLIDATE | Completed in `7bb68f4`: schema v2 snake-case policy is the sole input contract; explicit target IDs/fractions and flattened stable-ID stop rules are required, while wrappers, aliases, implicit allocation, and instrument economics are rejected. |
| Implicit 20-tick breakeven movement | DELETE | Completed in `66aac0b` and `c5d3c76`: omitted breakeven and stop-adjustment policy compiles to disabled behavior, and the domain position default is zero. Breakeven movement now requires explicit configuration. |
| Fill-adapter fallback, fabricated bridge sessions/raw fanout, duplicate material fingerprint | DELETE | Completed in `6ed8366`: typed `execute_order(FillOrder)`, explicit bridge identity, typed fanout envelopes, and `semantic_fingerprint` are the sole contracts. |
| Silent internal execution coercions and missing ATM references | DELETE | Completed in `484ad56`: unknown same-bar, exit-event, terminal-reason, and liquidity values fail loudly; referenced ATM records must exist; entry order semantics come only from the compiled plan. |
| Same-batch runtime-event ID ordering | DELETE | Completed in `58ea831`: event IDs are idempotency keys only; stable producer order determines dense canonical `run_seq`, and non-monotonic position clocks block golden certification. |
| Playback `mode` fallback into `execution_mode` | CONSOLIDATE | Completed in `a5ef7de`: execution mode defaults at its own owner, accepts only `fast`/`full`, and rejects playback values. |
| Indicator overlay-history interface and Candle Stats/Regime implementations | KEEP | Renamed in `a5ef7de` to the explicit `configure_overlay_history` contract. All three definitions mean render-only retention, not research range, evaluation, recovery, or warmup. |
| Four replay-window dispatch helpers | CONSOLIDATE | Completed in `a5ef7de`: indicator preview, runtime validation, strategy preview, and bot setup use one fail-loud `configure_indicator_overlay_history` owner. |
| Research range, backtest evaluation, indicator warmup, runtime recovery, transport replay | KEEP | These are distinct windows with different clocks, failure policy, and consumers. Similar names are not evidence of duplication. |
| Continuity, candle catalog, readiness, provenance, diagnostics, confidence, and caveats | KEEP | Created by existing candle/indicator/runtime/report owners, finalized in readiness, persisted with report fingerprints/materializations, exported in report bundles, and displayed through CLI/MCP report resources and research-check results. Indicator source continuity now survives runtime metadata and both standalone/worker artifacts without introducing another quality envelope. |
| Compact report and experiment evidence projection | CONSOLIDATE | The canonical dataset already owned identity, repeatability, quality, blockers, fingerprints, and caveats, but `run_research_summary.v1` and persisted experiment summaries dropped part of that evidence. They now project those existing fields unchanged without creating another quality model or expanding the command/MCP surface. |
| Warmup/provider/truncation quality omissions | CONSOLIDATE | Completed in `38cf782`, `84dbf93`, and `0d8118f`: backtest warmup evidence is explicit, malformed candle frames fail before evaluation, and bounded observability reads expose coverage/truncation caveats. The existing envelope remains canonical. |
| Viewer-dependent runtime facts and transport-derived continuity | CONSOLIDATE | Completed in `a0e196e`: canonical decision/fill facts are produced without subscribers, complete-series continuity is persisted by the runtime at `run_final`, and sampled transport continuity is explicitly diagnostic-only. |
| Spot fill/accounting truth | CONSOLIDATE | Completed in `a0e196e`: entry/exit fills persist strict causal, fee, currency, accounting-mode, wallet-before/delta, and commit-clock evidence. Spot replay consumes 22 canonical fills without derived margin events and fails loudly on malformed or inconsistent state. |
| Margin fill plus derived-ledger dual representation | CONSOLIDATE | Raw margin fills are retained for execution evidence while the existing derived margin ledger remains wallet truth. Report replay excludes raw margin fills to prevent double application; one canonical margin accounting representation remains deferred. |
| Tracked `portal/frontend/.vite/deps` | DELETE | Completed in `7338f56`: the generated cache was removed, ignored, and a repository-wide tracked-artifact audit is clean. |
| Filename-routed PR profile and stale controller test seam | DELETE | Completed in `35949fe`: the full non-database backend suite replaces the 70-line filename allowlist, and the overlay logging test now fails through the current metadata-service boundary. |
| Deprecated GET report-build flags | DELETE | Removed from the read-only `GET /run-report` contract; report materialization is owned only by `POST /run-report/build`, and the stale architecture route description was corrected. |
| Duplicate indicator-type routes and removed ATM owner-field shim | DELETE | `/api/indicators/types` and `/api/indicators/types/{type_id}` are the only routes used by the CLI, MCP adapter, tests, and architecture docs. The uncalled `/api/indicators-types` pair and the unreachable `owner_id` payload deletion were removed. |
| Commented changelog workflow experiment and stale `test` CI targeting | DELETE | The workflow experiment was deleted in `7338f56`. `origin/test` has no commit absent from `origin/develop` (`origin/develop...origin/test = 1178/0`), so the active backend workflow now targets only `develop` and `main`. |
| CLI operations | KEEP | `qt` is the canonical operator contract. Every invocation writes a redacted structured audit by default and API calls record method, URL, status, duration, and byte counts. Direct mutation commands do not consistently require plan/apply/confirm, and `--no-audit-log` can disable the record, so unrestricted CLI access is not yet an agent-safe boundary. |
| MCP operations | VERIFY USAGE | All 44 registrations have handlers and no orphan definitions were found. Mutating tools require confirmation and usually plan by default; paper/live starts require an additional opt-in. External invocation is not visible in the internal call graph, so retain the thin optional adapter until usage evidence supports tool-level deletion. |
| Async queue ownership and research side effects | CONSOLIDATE | Opaque per-claim tokens, monotonic generations, PostgreSQL-clock bounded heartbeats, heartbeat-based reclaim, and conditional terminal writes are canonical. Queued research-check artifacts and success commit atomically; stale, failed, and duplicate owned effects cannot persist. Atomic request-fingerprint uniqueness suppresses concurrent duplicate dispatch, retry budgets bound timeout reclaim, and the exclusive-access migration requeues pre-fencing running rows only on first installation. |
| Missing bridge-session fallback to `"legacy"` | DELETE | Completed in `6ed8366` after all production producers were verified to supply an explicit bridge session. |
| Typed strategy model dictionary compatibility | DELETE | `StrategyLoader` already returns the typed runtime model. The generic backward-compatible `to_dict`, unused link `from_dict` constructors, and startup dictionary re-parsing were removed; one explicit `to_series_metadata` projection remains for runtime series. |
| Strategy market-identity aliases | DELETE | Strategy persistence, runtime, CLI, and experiment tooling use `datasource`/`exchange` as strategy defaults. Strategy-level `provider_id`/`venue_id` translation and response emission were removed, and strict request models reject those aliases. Provider/venue IDs remain at their actual owner: provider selection, credentials, and instrument admission. |
| Generated container run-ID fallback | DELETE | Backend startup and the runner already own run identity. Container startup now requires the exact `QT_BOT_RUNTIME_RUN_ID` and fails before config, lifecycle, lease, wallet, or report mutation instead of inventing a disconnected UUID. |

## Scale and Agent-Safety Audit

| Area | Current protection | Remaining gap |
| --- | --- | --- |
| Report materialization | Input fingerprints, cached materializations, and a single-worker executor prevent duplicate concurrent builds. | `run_research_dataset.py` remains a 6,041-line concentration point mixing projection, quality, readiness, metrics, and presentation assembly. |
| Runtime projection | Bounded transport queues, revision cursors, batched persistence, and explicit overflow/degradation signals protect runtime truth. | `runtime_push_stream.py` is 3,015 lines and mixes event translation, persistence, diagnostics, transport, and lifecycle projection. |
| Runtime domain | Deterministic policies and reference scenario tests protect entry, exit, fill, lifecycle, and accounting behavior. | `core/domain/engine.py` is 2,027 lines and still combines sizing, admission, order/fill orchestration, position creation, and summary projection. |
| Setup and research | Async jobs are fingerprinted, partitioned, retryable, fenced by heartbeat/token/generation, and workers use bounded configured pools. Research-check effects commit atomically with terminal success. | Sweeps restart from immutable input after retry and expose neither partial-progress checkpoints nor mid-job cancellation. `setup_prepare.py` is 1,931 lines, `research/checks.py` is 2,023 lines, and series-link loading sizes a thread pool directly from eligible-link count. |
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
- Golden candidacy remains blocked when market-state capture is unavailable,
  even when deterministic semantic comparison succeeds. Market-state expansion
  remains intentionally out of scope.
- The local `LOCAL_PG_ENV` loader prefers a quoted/stale `PG_DSN` from
  `secrets.env`; repository forensic targets fail in this environment unless
  the DSN is constructed from the working PostgreSQL fields.
- MCP host registration is optional and was not configured in the validated
  local Codex environment.
- Async research sweeps support bounded restart-only retry, not partial-progress
  checkpoints or mid-job cancellation. No status may imply those unsupported
  capabilities.
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
| 2026-07-25 | merge `074bc8d` correctness evidence campaign | child branch integrated into `feat/platform-baseline-cleanup` without history rewrite; later independent audit reopened the incomplete structural and correctness acceptance items recorded below |
| 2026-07-25 | `a0e196e` canonical persisted runtime evidence | affected runtime/BotLens/reporting/wallet: 209 passed; PR profile: 1,268 passed; full local PostgreSQL profile: 1,268 passed, 3 gated tests skipped; gated DB profile: 3 passed. Repeated persisted runs `31a41c9a-d60c-4e0b-9ae9-9d8b367a4aaa` and `f0703de6-721b-47b2-a11f-f094f21745cc` each produced 12 decisions, 11 closed trades, 22 fills, 92 gap-free runtime-sequenced events, and ending equity 100,432.6918. Strategy, material config, data snapshot, material, and semantic fingerprints matched; wallet replay was consistent with zero drift or missing trace. Canonical continuity covered 269 candles with zero gaps. Golden policy remained blocked only by deferred `market_state_unavailable`; operational fingerprints differed as expected. |
| 2026-07-25 | `6ed8366` compatibility hard cutover | affected runtime/BotLens/reporting: 275 passed; complete non-database backend gate after two fixture-only bridge-session corrections: 1,269 passed; docs: 2 passed; removed 219 lines of adapter, fanout, bridge, fingerprint, and dead-helper fallback behavior |
| 2026-07-25 | `484ad56` execution invariant hardening | affected execution/strategy loader: 137 passed; complete non-database backend gate: 1,281 passed; docs: 2 passed; unknown policies/roles/events/reasons and orphan ATM references now fail before fills |
| 2026-07-25 | `58ea831` canonical fact ordering | persistence/report/runtime-fact profile: 148 passed; complete non-database backend gate: 1,283 passed; docs: 2 passed; same-batch producer order is durable and non-monotonic trade clocks block golden certification |
| 2026-07-25 | `00440b2` canonical lifecycle ownership | lifecycle/event/docs profile: 64 passed; complete non-database backend gate: 1,291 passed; clean isolated PostgreSQL profile: 3 passed; repeated Timescale startup and repeated PostgreSQL profile: 3 passed. Direct summary writers, status fallback reads, divergent event-ID retries, backdated/post-terminal checkpoints, and ignored decision-ledger run payloads were removed or rejected. |
| 2026-07-25 | `3b9e17b` exact runtime candle identity and ADR delivery | exact snapshot/runtime/report/docs profile: 191 passed; complete non-database backend gate with capture disabled: 1,296 passed, 49 pre-existing dependency/deprecation warnings, 24.23s; docs: 2 passed. The repository `backend-check` wrapper hit a pytest temporary-capture `FileNotFoundError` before collection; the identical `QT_OMIT_DB_TESTS=1` scope passed with `-s`. Exact candle values now have fail-loud runtime-produced identity, while quality remains in the existing continuity/readiness contract. ADRs 0042-0049 record accepted and proposed cleanup boundaries without treating incomplete enforcement as complete. |
| 2026-07-25 | known-at and terminal lifecycle proof | runtime/domain profile: 128 passed with 14 pre-existing dependency warnings; complete non-database backend gate: 1,303 passed with 49 pre-existing dependency/deprecation warnings in 25.33s; docs: 2 passed; affected compileall and whitespace checks passed. Appending adversarial future candles cannot alter the consumed-prefix fingerprint, indicator truth/projections, decisions, orders, fills, lifecycle, or wallet accounting under either adapter. Position state now owns terminal reason and weighted exit price; incomplete closed facts and rejected-exit false closure fail loudly. Persisted CLI/job/report truncation and a credential-free paper runner remain open. |
| 2026-07-25 | configured-series completeness and exact snapshot coverage | focused series/container/artifact/report profile: 120 passed; complete non-database backend gate: 1,308 passed with 49 pre-existing dependency/deprecation warnings in 22.30s; isolated PostgreSQL profile: 3 passed; docs: 2 passed. Any eligible series-build failure aborts the run, backend-planned expected series survives worker aggregation, and report hashes require exact planned/terminal snapshot-set equality. The shared local database credentials have drifted from its initialized volume, so database evidence used an isolated repository TimescaleDB image. |
| 2026-07-25 | async job ownership fencing | final non-DB backend profile: 1,323 passed; full PostgreSQL-backed profile: 1,333 passed; docs contract: 2 passed; 49 pre-existing dependency/deprecation warnings in each full profile. Fresh and explicitly migrated PostgreSQL ownership profiles: 7 passed each. The exclusive migration was applied repeatedly without changing migrated job state and rejected a concurrent client. Claims now use hidden tokens, generations, PostgreSQL-clock bounded heartbeats, max-attempt-aware reclaim, stale-owner rejection, atomic research-check side effects, race-safe in-flight request idempotency, and literal-preserving exact fencing schema definition checks. |
| 2026-07-25 | canonical report instrument semantics and read-only GET hard cutover | affected report profile: 77 passed; complete reporting service profile: 103 passed; report API profile: 8 passed and 3 opt-in DB tests skipped; complete non-database backend gate: 1,334 passed with 49 pre-existing dependency/deprecation warnings in 31.88s; docs: 2 passed; affected compileall, whitespace, generated architecture-index, and retired-query reference audits passed. Independent diff review found and the final implementation closed three edge cases: untyped fill execution semantics cannot change report identity, configured contradictions remain fail-loud even with zero fills, and the ADR states the exact spot-versus-margin rule. Canonical spot fills now complete missing report accounting/execution semantics in deterministic identity order; margin fills do not invent derivative semantics; conflicting or ambiguous evidence fails loudly. Deprecated GET build flags are absent from OpenAPI, unsupported query parameters are rejected generically, and materialization remains POST-owned. This slice changed no schema; the environment denied reading the running container password for a redundant DB-endpoint rerun, so the preceding 1,333-test PostgreSQL baseline remains the database evidence. |
| 2026-07-25 | dead API compatibility route removal | affected indicator-route and execution-template profile: 16 passed; complete non-database backend gate: 1,334 passed with 49 pre-existing dependency/deprecation warnings in 31.13s; docs: 2 passed; affected compileall, whitespace, and remaining-reference audits passed. The canonical CLI, MCP adapter, tests, and docs all use `/api/indicators/types`; the uncalled `/api/indicators-types` route pair and an unreachable removed-field payload shim were deleted. |
| 2026-07-25 | typed strategy startup and series-metadata hard cutover | affected strategy/startup/series profile: 47 passed; complete non-database backend gate: 1,335 passed with 49 pre-existing dependency/deprecation warnings in 27.97s; docs: 2 passed; affected compileall, whitespace, and retired-method reference audits passed. Startup now requires the `StrategyLoader` typed domain model and reads its canonical fields directly. Only the detached `to_series_metadata` projection crosses into per-series runtime metadata; unused link constructors and generic dictionary compatibility were removed. |
| 2026-07-25 | canonical strategy market identity | affected strategy API, compile, variant, persistence, and ATM profile: 40 passed; complete non-database backend gate: 1,340 passed with 49 pre-existing dependency/deprecation warnings in 29.05s; docs: 2 passed; affected compileall, whitespace, and retired-helper reference audits passed. Strategy persistence and APIs now expose only `datasource`/`exchange` defaults, strict writes reject strategy-level provider/venue aliases, and provider/venue ownership remains with provider selection, credentials, and instrument admission. |
| 2026-07-25 | backend-owned container run identity | affected container identity, runner, startup, transport, observe-only, and runtime-composition profile: 52 passed; complete non-database backend gate: 1,342 passed with 49 pre-existing dependency/deprecation warnings in 28.80s; docs: 2 passed; affected compileall, whitespace, and generated-fallback reference audits passed. Missing `QT_BOT_RUNTIME_RUN_ID` now fails before container startup instead of creating a disconnected lifecycle, lease, wallet, and reporting identity; ADR 0042 records the invariant and enforcing test. |
| 2026-07-25 | compact dataset-quality evidence propagation | focused report/experiment profile: 17 passed; MCP passthrough profile: 16 passed; reporting profile: 136 passed; CLI profile: 64 passed; complete non-database backend gate: 1,343 passed with 49 pre-existing dependency/deprecation warnings in 30.42s; docs: 2 passed; affected compileall and whitespace checks passed. `run_research_summary.v1` and persisted experiment summaries now retain canonical dataset/config/strategy hashes, semantic and operational fingerprints, repeatability, data/execution quality, blockers, degraded/unavailable sections, and caveats. The existing CLI commands and thin MCP adapter remain unchanged, and no agent-policy documentation or second quality envelope was added. |
| 2026-07-25 | persisted causal harness and source-diagnostics closure | focused runtime/artifact/report/persisted harness: 87 passed; runtime profile: 462 passed, 887 deselected; reporting profile: 141 passed, 1,208 deselected; complete non-database backend gate: 1,349 passed with 49 dependency/deprecation warnings in 44.66s; clean/repeated isolated Timescale bootstrap and complete PostgreSQL-enabled gate: 1,359 passed with 49 warnings in 45.21s; docs: 2 passed. Repeated backtests, prefix truncation, and bounded paper replay use the production Strategy/SeriesBuilder/BotRuntime/compiler/risk/adapters/artifact/persistence path and prove exact semantic repeatability, signal-close causality, adapter agreement, lifecycle/accounting reconciliation, and explicit no-breakeven behavior. Existing indicator source continuity now persists and reaches readiness, caveats, diagnostics, golden gating, and operational identity; malformed evidence fails loudly. |

Each child branch must record its focused tests, broader regression profile,
documentation validation, diff review, and remaining-reference search before
integration.

## Discovered Risks

- A repository-defined credential-free persisted runtime harness now exercises
  the production strategy, series-builder, runtime, adapter, artifact, and
  persistence boundaries. It is test infrastructure, not a standalone
  operator-facing `qt backtest` command.
- Direct CLI mutation is audited but is not uniformly plan/apply/confirm gated;
  agents should use approved wrappers until that boundary is tightened.
- API uptime health can succeed while database readiness is unavailable.
- The largest orchestration modules remain costly to reason about and review;
  extraction should continue only behind the established regression evidence.
- Session-aware margin behavior and reconstruction of evidence never persisted
  by a run remain unsupported and must stay explicit in reports.
- Margin accounting still has two persisted representations: raw fills for
  execution evidence and derived ledger events for wallet truth.
- Persisted semantic repeatability, prefix invariance, paper replay, and
  accounting are proven by the credential-free harness. Operational artifacts
  intentionally retain run-instance IDs and wall-clock timing, so byte identity
  is not claimed.
- Exact runtime candle values now feed `data_snapshot_hash`; older runs without
  terminal snapshot or indicator-source diagnostic evidence remain explicitly
  unavailable rather than being upgraded by reconstruction.
- Canonical spot fill accounting now completes missing report instrument
  accounting/execution semantics. Conflicting configured and fill evidence
  blocks report construction rather than silently selecting one source.
- Async research sweeps still restart from the beginning after retry and expose
  no partial-progress checkpoint or mid-job cancellation contract. Abandoned
  report materializations still have no stale-build recovery lease.
- Sampled transport continuity can report apparent gaps even when producer-owned
  complete-series continuity is clean; the material/diagnostic distinction must
  remain visible to operators.

## Blockers and Deviations

- Cleanup merge blockers: none after the final regression and database gates.
  Golden-candidate promotion remains policy-blocked by deferred market-state
  capture, which is explicit and outside this campaign rather than represented
  as completed.
- Deviations from the initial inventory: none yet.
- The local TimescaleDB service on port 15432 passed full and gated database
  validation when the DSN was constructed from `POSTGRES_*`. The Make forensic
  environment preferred a quoted/stale `PG_DSN` and required an explicit local
  environment workaround.
- A later report-only slice did not repeat its three opt-in DB endpoint tests
  because access to the running container password was denied by the local
  credential safeguard. The service remained healthy, the slice changed no
  schema or repository code, and the immediately preceding complete
  PostgreSQL-backed baseline remained green.
- Strict execution scope expanded to compile ATM templates at strategy and
  standalone-template persistence boundaries and to include execution-contract
  tests in the PR profile; this closes an admission-timing gap found in review.
- Independent review expanded the strict boundary to reject legacy flat stop
  input, conflicting target aliases, fractional integer shorthand, and dormant
  invalid trailing rules, and to honor target fractions during deterministic
  quantity-step allocation.
- Frontend checks remain intentionally skipped because frontend is outside the
  cleanup critical path.
- Two representative persisted real-strategy runs completed from the existing
  local BTC/USD fixture without live orders or credentialed/paid market-data
  calls. Failed strict-contract discovery runs remain in the developer database
  as auditable degraded-terminal evidence.

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
- [x] Durable cleanup decisions captured in an ADR index with invariants,
  rejected alternatives, and enforcing evidence
- [x] Architecture and operator documentation aligned
- [ ] Integration branch clean, pushed, and ready for review
