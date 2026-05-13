# Developer Audit Workflow

This workflow is for Codex and local developer operations only. It standardizes
repeat audit, Docker, database, reporting, log, validation, and commit commands
without changing product/runtime behavior.

## Command Structure

Use the root `Makefile` as the single entrypoint:

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
- `make bot-active bot=<bot_id>` prints backend active-run state.
- `make bot-start bot=<bot_id> request=<request_id>` starts a bot and prints
  the accepted run id.
- `make bot-stop bot=<bot_id> run=<run_id> preserve=1` stops a bot run while
  preserving the runtime container/artifacts when the backend supports it.
- `make run-status run=<run_id>` prints persisted DB run status.
- `make run-wait run=<run_id>` waits for a terminal DB run status.

The root `Makefile` remains the entrypoint. Do not add new root-level workflow
folders unless the file becomes difficult to navigate. If it does, split by
current sections into included files such as `make/docker.mk`, `make/db.mk`,
`make/reporting.mk`, `make/test.mk`, and `make/docs.mk`.

## Reporting Audit

Reporting helpers use existing backend report contracts and the single
`PG_DSN`. Local make targets source `secrets.env` only to construct `PG_DSN`
when it is not already exported.

- `make report-readiness run=<run_id>`
- `make report-dataset run=<run_id>`
- `make report-summary run=<run_id>`
- `make report-diagnostics run=<run_id>`
- `make report-manifest run=<run_id>`
- `make report-export run=<run_id>`
- `make run-ordering run=<run_id>`
- `make run-throughput run=<run_id>`
- `make run-event-summary run=<run_id>`
- `make run-seq-gaps run=<run_id>`
- `make run-write-latency run=<run_id>`
- `make botlens-check run=<run_id>`
- `make botlens-check run=<run_id> symbol="<instrument_id|timeframe>"`
- `make wallet-diagnostics run=<run_id>`
- `make wallet-diagnostics run=<run_id> compare=<prior_run_id>`
- `make golden-compare left=<run_id> right=<run_id>`
- `make golden-compare left=<run_id> right=<run_id> check_prior=1`

Report export output defaults to `logs/reports/`, which is ignored and suitable
for local audit artifacts.

`golden-compare` builds and saves both `RunResearchDataset` payloads, compares
material hashes, report fingerprints, decision ids/verdicts, wallet trace
coverage, trade lifecycle, summary metrics, diagnostics, runtime ordering, and
golden candidate status, then writes `comparison_summary.json`.

## Validation

Use focused checks first, then broaden only when the change warrants it:

- `make test-reporting`
- `make test-reporting-api`
- `make test-botlens`
- `make validate-docs`
- `make frontend-test`
- `make frontend-build`
- `make git-check`
- `make check`

`make test-reporting-api` is intentionally separate from `make test-reporting`
because it starts FastAPI route tests and may expose backend lifespan, DB, or
watchdog readiness issues. It is bounded by `REPORT_API_TEST_TIMEOUT`.

For architecture-affecting changes, follow `AGENTS.md`: inspect
`docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md`, update targeted component
docs, refresh the index, and run `make sync-docs`.

## Codex Audit Shape

Do not hide full audits behind one opaque target. Keep the pieces composable:

- Start runs explicitly with `make bot-start`.
- Wait explicitly with `make run-wait`.
- Compare explicitly with `make golden-compare`.
- Use `make db-query`, `make logs-backend`, `make logs-bots`, and shell targets
  when the failure mode needs direct inspection.

This is the useful automation boundary: commands remove repeated typing, but
Codex still chooses the next diagnostic path instead of being funneled through a
single rigid script.

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
