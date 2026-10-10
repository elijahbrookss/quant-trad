# Developer Workflow

This workflow is for Codex, other agents, and local developer operations. It
standardizes the command surfaces for workflows, visualization, Docker,
database, reporting, logs, validation, and commits without changing
product/runtime behavior.

## Operational Surfaces

Use the surfaces by role:

| Surface | Role |
| --- | --- |
| `qt setup` | Canonical local readiness and provider-onboarding surface. Use it for env setup, setup doctor checks, and provider onboarding flows. |
| `qt` CLI | Primary agent/tool workflow and operation entrypoint. Use it for bot runs, experiments, provider checks, report summaries, report exports, and comparisons. |
| `qt mcp serve` | MCP protocol adapter for agent hosts. Use it when the host expects MCP resources/tools instead of direct shell commands. |
| UI | Human visualization and inspection surface. Use it to inspect charts, BotLens, fleets, strategies, reports, and playback. Do not treat UI state as workflow truth. |
| Makefile | Local development and forensic support index. Use it for Docker, DB, validation, tests, logs, git helpers, and direct local diagnostics. |

Start with `make deps` for local Python dependencies and `qt setup` for
readiness/provider onboarding. Start with `qt` when a task asks an agent to
operate the system through normal backend contracts. Use Make when the task is
about the local stack, direct DB inspection, tests, or forensic diagnostics.

Use `qt mcp serve` only as the MCP transport for agent hosts. It should expose
the same workflow boundary as `qt`, not a separate source of runtime, report, or
experiment truth.

Common agent/tool workflow commands:

- `qt setup doctor`
- `qt setup provider coinbase`
- `qt bots list`
- `qt bots get <bot_id>`
- `qt bots start <bot_id> --request-id <request_id>`
- `qt runs wait <bot_id> <run_id>`
- `qt reports summary <run_id>`
- `qt reports instruments <run_id>`
- `qt reports symbol-summary <run_id>`
- `qt reports trades <run_id> --symbol <symbol>`
- `qt reports decisions <run_id> --state rejected`
- `qt reports export <run_id>`
- `qt reports compare <baseline_run_id> <variant_run_id>`
- `qt indicators types`
- `qt indicators validate-config --type <type> --params-json '<json>'`
- `qt indicators validate-runtime <indicator_id> --instrument-id <instrument_id> --start <iso> --end <iso> --interval <timeframe>`
- `qt data derive-candles --dataset-id <frozen_id> --source-series-id <id> --start <iso> --end <iso> --timeframe <coarser_interval>`
- `qt data coverage --instrument-id <instrument_id> --start <iso> --end <iso> --timeframe <timeframe>`
- `qt research check requirements --request-json <request.json>`
- `qt research check preview --request-json <request.json>`
- `qt research check prepare --request-json <request.json> --freeze --created-by <actor> --dataset-name <name>`
- `qt research check run --request-json <request.json> --dataset-id <dataset_id> [--dispatch]`
- `qt research check replay <check_id>`
- `qt research observe-from-check <check_id> --title <title> --body <body>`
- `qt research trail <observation_or_check_id>`
- `qt research check sweep --check-family <family> --indicator-id <indicator_id> --instrument-id <instrument_id> --start <iso> --end <iso> --timeframe <timeframe> --detector-json '<json>' --variant <id[:key=value]> --rank-by <metric.path> --rank-direction <asc|desc>`
- `qt research check sweep ... --dispatch`
- `qt research jobs status <job_id>`
- `qt research jobs cancel <job_id>`
- `qt research jobs result <job_id> --format table`
- `qt instruments list`
- `qt instruments profile <instrument_id> --execution-semantics proxy_derivative`
- `qt experiments validate-plan <plan>`
- `qt experiments run-plan <plan> --experiment-id <experiment_id>`
- `qt experiments resume <experiment_id>`
- `qt experiments status <experiment_id>`
- `qt experiments collect <experiment_id> --wait --export`

For research calculations, use the sequence above. Preview is ephemeral and
cannot support an evidence-bearing Observation. Preparation may acquire only
when an explicitly authorized lower-level acquisition operation is requested;
Check execution never acquires. Dataset freeze records known facts and gaps,
while Indicator, Check, and Strategy/Backtest each own their readiness decision.
Do not calculate features, joins, outcomes, statistics, pass gates, or evidence
hashes in shell scripts, experiment orchestration, dossiers, or MCP handlers.

MCP host command:

- `qt mcp serve`
- `make mcp-ready`
- `make mcp-smoke`
- `make mcp-register-codex`

`make up` prints the MCP adapter command and whether the Codex MCP alias is
already configured. It does not daemonize `qt mcp serve`; the MCP host must
launch the stdio server so stdin/stdout are connected to that host.

Use the root `Makefile` as the support command index:

- `make deps` owns local Python venv creation and editable install.
- `make help` lists repo-native commands.
- `make status` shows compose service status.
- `make logs` tails the selected compose stack.
- `make logs-backend` tails backend logs.
- `make logs-bots` tails the isolated bot runtime logs, or use `bot=<container>`
  for spawned runtime containers.
- `make backend-shell` opens the backend container shell.
- `make bot-shell` opens the isolated bot runtime shell, or use
  `bot=<container>` for a spawned runtime container.
- `make dbshell` opens `psql` in the TimescaleDB container.
- `make db-query sql="select 1"` runs a one-line SQL statement.
- `make db-file file=scripts/db/example.sql` runs a SQL file. Add
  `statement_timeout=0` only for a reviewed long-running migration that
  explicitly requires it; the runner preserves other PostgreSQL startup
  options and applies that override before connecting.

Normal bot/run/report operations do not belong in Make. Use `qt bots`,
`qt runs`, `qt reports`, and `qt experiments` for those workflows.

The root `Makefile` remains the development and forensic support index. Do not
add new root-level workflow folders unless the file becomes difficult to
navigate. If it does, split by current sections into included files such as
`make/docker.mk`, `make/db.mk`, `make/reporting.mk`, `make/test.mk`, and
`make/docs.mk`.

## Reporting Diagnostics

For normal report workflows, prefer `qt reports ...` because it goes through
the backend API contract and returns machine-readable workflow output.

Make forensic helpers are for direct local audit and diagnostics. They use
existing backend report/runtime contracts and the single `PG_DSN`. Local make
targets source `secrets.env` only to construct `PG_DSN` when it is not already
exported.

Direct forensic targets are explicitly prefixed:

- `make forensic-run-ordering run=<run_id>`
- `make forensic-run-throughput run=<run_id>`
- `make forensic-run-event-summary run=<run_id>`
- `make forensic-run-storage-budget run=<optional_run_id>`
- `make forensic-run-seq-gaps run=<run_id>`
- `make forensic-run-write-latency run=<run_id>`
- `make forensic-observability-storage-budget run=<optional_run_id>`
- `make forensic-botlens-check run=<run_id>`
- `make forensic-botlens-check run=<run_id> symbol="<instrument_id|timeframe>"`
- `make forensic-wallet-diagnostics run=<run_id>`
- `make forensic-wallet-diagnostics run=<run_id> compare=<prior_run_id>`
- `make forensic-golden-compare left=<run_id> right=<run_id>`
- `make forensic-golden-compare left=<run_id> right=<run_id> check_prior=1`

Report export output defaults to `logs/reports/`, which is ignored and suitable
for local audit artifacts.

CLI experiment records default to `logs/experiments/`, which is also ignored.
Use these records to resume long-running research operations after a terminal
disconnect or context reset.

`forensic-golden-compare` builds and saves both `RunResearchDataset` payloads,
compares material hashes, report fingerprints, decision ids/verdicts, wallet
trace coverage, trade lifecycle, summary metrics, diagnostics, runtime
ordering, and golden candidate status, then writes `comparison_summary.json`.

## Validation

Use focused checks first, then broaden only when the change warrants it:

- `make test-reporting`
- `make test-reporting-api`
- `make test-botlens`
- `make validate-docs`
- `make git-check`
- `make check`
- `make frontend-check` when the change affects the supported frontend or its adapters

`make test-reporting-api` is intentionally separate from `make test-reporting`
because it starts FastAPI route tests and may expose backend lifespan, DB, or
watchdog readiness issues. It is bounded by `REPORT_API_TEST_TIMEOUT`.

`make check` is the backend baseline: repository hygiene, architecture-doc
contracts, and ordinary non-database backend tests. `make check-all` adds the
supported frontend tests and production build.

The database suite (`./scripts/ci/run_test_suite.sh db`) creates a disposable
Docker project with generated credentials and an internal network. Its
TimescaleDB service disables extension telemetry: the isolated network cannot
deliver those reports, and a surviving Telemetry Reporter was observed blocking
fixture cleanup. Ordinary database workers remain enabled. Migration fixtures
fence new connections to their own generated database, then use PostgreSQL's
forced drop; failures report remaining session types and waits without query
text or credentials. These settings apply only to the test stack.


### Exact-source CI equivalence for broad DB/recovery handoff

The broad PostgreSQL contract step explicitly enables `QT_SCHEMA_TEMPLATE_PILOT=1`
with `SOURCE_REVISION` from its actual checkout and `SOURCE_TREE_HASH` derived from
that commit. Reuse is restricted to the 13 reviewed exact node IDs in
`migration_test_support._PILOT_CASES`; every other case retains a fresh database.
Separate clean-bootstrap, namespace and recovery steps remain fresh. This does
not expand eligibility or turn the paired timing comparison into a whole-CI
performance guarantee. Diagnose the same cases with fresh setup using
`QT_SCHEMA_TEMPLATE_PILOT=0 ./scripts/ci/run_test_suite.sh db <exact-node-IDs>`,
or omit node IDs for the ordinary broad fresh-mode suite. The runner retains
source identity and isolation checks in both modes.


Run focused local checks while iterating. Once the final integrated commit has
one successful complete CI attempt, use that evidence for the broad DB and
prescribed recovery handoff row instead of repeating the same whole suite
locally. Until that receipt exists, report broad qualification pending; a
focused/component pass or a predecessor's green run cannot substitute.

For a pushed work branch, save the helper's JSON receipt:

```bash
python scripts/automation/check_release_ci.py <final-full-commit> --handoff-branch feature/<branch>
```

The caller must compare the receipt revision with the clean final tree being
handed off. The helper requires the latest exact-commit push, all seven expanded
job labels in the same workflow attempt, and both unexpired recovery artifacts
with SHA-256 digests and creation times bound to their producing jobs. Each
recovery bundle includes `qualification-source.json`, binding the full workflow
commit, run ID, attempt, runtime variant and verified runtime-image attestation.
A runtime source hash alone excludes tests/workflow files and cannot certify
this handoff. Archive the JSON receipt, job logs and source-attested bundles
before their seven-day retention expires; retain artifact IDs/digests as well as
URLs. The helper verifies successful source-attestation steps and artifact
metadata; it does not independently download/revalidate the bundle contents.

The coverage map is explicit:

| CI proof | Scope and exclusions |
| --- | --- |
| `clean-database-bootstrap` | Broad `pytest -m db` with isolated DB inputs; clean-bootstrap and header-namespace files are excluded there and executed separately in this same job. `QT_CLEAN_BOOTSTRAP_TEST_DSN` enables the clean-install case. |
| `deployment-contract` | `incremental-recovery` enables `QT_STORAGE_DEMO=1`, `QT_INCREMENTAL_APPLICATION_TEST=1`, production-derived test images, UID/PID ownership and private storage/restore volumes. Runs the application restore and retained-raw-placement cases separately. Its `storage-demo` selection includes the default `test_storage_end_to_end_db.py` plus the explicitly listed online copy/switch cases, with owned storage topology. Other named worker/core/storage-layout rehearsals remain required. |
| `committed-recovery (current)` and `committed-recovery (24357ff387776822f676ae2a8b1cce7f209c313e)` | Both actual expiry/committed-switch recovery variants and their independently bound evidence bundles are mandatory. |
| `pr-suite`, `frontend`, `deployment-rehearsal` | Remain mandatory companions; they do not replace DB/recovery scope. |

This is equivalent to the ordinary broad DB row plus the prescribed CI recovery
proofs, not certification of every optional test that skips without a specialized
topology. The storage-demo command is an explicit selection. For changes touching
an omitted storage-worker, working-root, archive-reference/root-copy,
history/recovery-maintenance, header-supervision, or standalone storage-restore
case, run the relevant owned focused topology and retain its result; do not
infer coverage from a green broad job or add another whole-suite cycle by habit.

Keep inexpensive local documentation, shell/configuration, diff and clean-tree
checks. Record known failures with source, node ID, traceback and disposition.
Unresolved failures and local/host-specific issues require focused reproduction
or explicit reconciliation; CI success does not erase an earlier failure. The
helper receipt deliberately records local failure review as unassessed.

Deployment remains separate and stricter:

```bash
python scripts/automation/check_release_ci.py <final-full-commit>
```

Only an exact successful `develop` push can satisfy this mode; work-branch or PR
receipts cannot. Existing compatibility, deployment authorization, image checks,
post-deploy readiness and recovery requirements remain in force. No active run
is cancelled when this equivalence path is adopted.


For architecture-affecting changes, follow `AGENTS.md`: inspect
`docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md`, update targeted component
docs, refresh the index, and run `make sync-docs`.

## Local release-package storage

Keep bulky image exports outside the checkout on a deliberately selected
volume. Before exporting, check that volume's free space and the host drive
backing WSL; Linux's virtual free-space figure does not establish Windows
headroom. On the current workstation, release packages use
`D:\QuantTradReleaseStaging`; repositories remain on the internal SSD.

Retain the deployed package, a compatible rollback package, the active
candidate, and any explicitly pinned recovery image. Superseded test/build
exports need not accumulate: identify exact files and active dependencies
before authorized cleanup, preserve small receipts and source hashes, and
record removals. Do not use broad Docker pruning or delete archive data,
backups, research results, keys, or active jobs as package cleanup. After WSL
cleanup, verify actual host free space before resuming builds; disk compaction
is a separate controlled workstation operation.

## Hard-Shutdown And Desktop UI Recovery

After an unclean workstation shutdown, prove PostgreSQL readiness before
restarting backend and collector processes. The backend supervisor starts API,
indicator, and research workers together; starting that fanout while PostgreSQL
is still in crash recovery produces connection-reset noise and can exaggerate
cold-start latency. Use `pg_isready`, then restart the affected services once
the database accepts connections. Do not treat the initial failed schema checks
as application data corruption without independent evidence.

Codex Desktop browser QA from a WSL checkout requires the desktop browser
bridge to accept the workspace's WSL file URI. If the bridge rejects it with
`sandboxCwd is not a local file URI`, continue API, contract, compilation, and
live HTTP validation, but do not substitute an unapproved hidden browser and do
not claim pixel or click-path verification. Reopen the repository through a
supported Remote WSL/native path or repair the bridge before making an
interactive-browser claim.

## Codex Workflow Shape

Do not hide full audits behind one opaque target. Keep the pieces composable:

- Start runs explicitly with `qt bots start` or a checked-in/ignored
  experiment plan through `qt experiments run-plan`.
- Wait explicitly with `qt runs wait` or `qt experiments collect --wait`.
- Compare explicitly with `qt reports compare` for normal report comparisons.
- Use `make forensic-golden-compare` when you need the direct local forensic
  comparison helper.
- Use `make db-query`, `make logs-backend`, `make logs-bots`, and shell targets
  when the failure mode needs direct inspection.

This is the useful automation boundary: `qt` operates the system through the
backend API, Make supports local diagnostics, and Codex still chooses the next
diagnostic path instead of being funneled through a single rigid script.

## Branch Naming and Coordinated Releases

Every work branch uses `feature/<description>` or `hotfix/<description>`.
Use `feature/` for planned capabilities, documentation, integration and
qualification; use `hotfix/` for corrective fixes. Do not use agent names or
alternative prefixes such as `codex/`, `feat/`, `feats/`, `fix/`, `docs/`
or `verification/`. Long-lived `main`, `develop` and `test` are not work
branches and retain their names.

For a coordinated release:

1. Create one `feature/` integration branch from the intended `develop`
   revision. Component PRs target that branch in dependency order.
2. Preserve original commits through merge commits. Keep component branches and
   old PR discussions; leave automatic source-branch deletion disabled.
   Renaming a GitHub PR source closes that PR, so link its replacement instead
   of rewriting or discarding its evidence.
3. Reconcile overlaps before merging. A component already included by another
   branch is not another implementation to copy. Prior exact-source tests remain
   evidence within their tested scope; they do not certify the aggregate.
4. Validate the final integrated tree. The normal CI suite runs on pushes to
   `feature/**` and `hotfix/**`, including integration merges. Superseded work
   branch runs may be cancelled; the latest aggregate still needs its result.
5. Raise the final integration PR to `develop` only when authorized. For the
   current storage/research consolidation the user will raise that PR.
   Deployment approval and PR approval are separate decisions.

One coordinated deployment can still require ordered preparation, a bounded
schema cutover and application promotion. Merging code does not run those
operations, retire recovery material or restart research. Exact schema/runtime
compatibility, resource admission and recovery evidence remain release gates.

## Commit Helper

Use:

```bash
make commit msg="reporting: add wallet trace diagnostics"
```

The helper rejects empty or multiline messages, requires
`<area>: <core change>`, keeps the message at 72 characters or less, runs
`git diff --check`, stages all repo changes only because this target was
explicitly invoked, commits, and prints the resulting hash.

## Existing Target Notes

Keep these aliases unless all callers have moved:

- `up`, `down`, `restart`, `logs`, `build`, `rebuild`, and `ps` wrap the
  `stack-*` Docker targets.
- `bots-*` targets operate the isolated bot runtime compose file.

Review before cleanup:

- Keep audit helpers in existing locations such as `scripts/reporting/` and
  `docs/engineering/`; do not add root-level prompt or workflow files.

### Explicit Market Profile first-return research

Create a separate `market_profile` instance through `qt indicators create
--payload-json <file> --apply --confirm` with top-level `"version": "v2"`.
`qt indicators validate-config --payload-json <file>` validates the same request.
Omitting version retains v1. Do not edit a historical instance to change versions.

An event-fact request with `outcomes.first_return` pins definition 9 / evaluator
8. Declare `classification_lag_bars`, `sample_lag_bars`,
`readiness_contract: "market_profile.first_return_state.v2"` and
`dependence: "leave_one_original_profile_out.v1"`. Use the public
`balance_breakout` detector, bar horizons, no Fact inputs and `gap_policy: reject`.
The sample must follow classification and precede every original endpoint.
Keep classification within the Indicator's declared origin lifetime. Outcomes
are descriptive distance-to-original-POC changes, not trading returns.

### Fixed current-state risk comparison

An explicit `outcomes.forward_risk.schema_version: candle_risk_matched_state.v1`
and `matching_contract: crossing_state_matched_pairs.v1` select definition 11 /
evaluator 10. Retain the fixed baseline, readiness, no-tail and horizon fields
from the existing candle-risk configuration. Old definitions reject this mode.
Use an existing compatible frozen Dataset; no new acquisition or freeze is
needed merely to change the analytical question. Qualification must prove
original-Dataset reuse, public-state eligibility, outcome-blind matching,
whole-pair missingness/deletions, cross-arm nonoverlap, bounded cancellation,
Observation admission and provider-free replay before empirical admission.
