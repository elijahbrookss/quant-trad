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
- `make db-file file=scripts/db/example.sql` runs a SQL file.

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


For architecture-affecting changes, follow `AGENTS.md`: inspect
`docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md`, update targeted component
docs, refresh the index, and run `make sync-docs`.

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
