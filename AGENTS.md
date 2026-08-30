# Quant-Trad Agent Context

This file is the **entry point for all agents and contributors**.

It defines the expectations, principles, and engineering discipline required
to work safely inside the Quant-Trad codebase.

This file governs contributor and agent workflow. Product behavior is defined
by the platform contracts under `docs/contracts/`. This file may summarize
those rules, but it cannot override them. If workflow guidance and a platform
contract disagree about product behavior, the platform contract wins.

---

## Agent TL;DR (Read This First)

- QuantLab = research only
- Strategy = decision logic only
- Bot = execution + realism only
- UI = human visualization and inspection only
- `qt` CLI = agent/tool workflow and operation entrypoint
- `qt mcp serve` = MCP adapter over `qt`/backend contracts, not new truth
- Make = local stack, DB, tests, and forensic helpers
- All bot runs are walk-forward
- Derived artifacts must respect known-at timing
- Playback is a debugger, not a demo
- Fail loud; never swallow errors
- Prefer simple designs early; refactor when patterns are proven
- Abstractions belong in core components, not everywhere
- Prefer interfaces at real boundaries
- Duplicate logic is a refactor signal

> **Infrastructure Rule:** Only one DSN exists (`PG_DSN`). New persistence layers must use it directly—no additional DSN env vars or mapper layers.

---

## Repository Reading Path

Start with the product before opening deep architecture:

1. `README.md` explains what QT does and how to start it.
2. `docs/current-system.md` explains the current end-to-end system, its limits,
   and the six promises that guide high-consequence changes.
3. `docs/contracts/platform/04_glossary.md` standardizes QT vocabulary.
4. `docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md` maps code paths to the
   component documents that describe them.
5. `docs/contracts/README.md` leads to the authoritative platform contracts.
6. `docs/core-promises.md` shows which important system promises a change may
   affect and links to the contracts and decisions that define them.

For an ordinary change, this path is enough to find current meaning and normal
checks. Use historical plans, incident records, and research evidence only when
the change needs their context; they do not override current contracts.

Before changing behavior:

- use the component index to find the relevant component documents;
- use the glossary and contracts to confirm current meaning;
- check the six core promises for consequences beyond the local component;
- update a platform contract when platform-wide product meaning changes;
- add or revise an ADR when a durable architectural or safety tradeoff changes;
- run focused tests first, then the normal validation scope described in
  `docs/engineering/developer-workflow.md`.

A passing test supports the behavior it exercises; it does not override the
documented product contract.

---

## Canonical Context (Required Reading)

Agents MUST understand these documents before making architectural or behavioral changes:

- `docs/contracts/platform/00_system_contract.md`
- `docs/contracts/platform/01_runtime_contract.md`
- `docs/contracts/platform/02_execution_playback_contract.md`
- `docs/contracts/platform/03_engineering_contract.md`
- the platform glossary listed in the Repository Reading Path above

These define the system contract.

---

## System Philosophy (Quant-Trad Specific)

Quant-Trad models markets as **incrementally discovered systems**.

Indicators, regimes, and profiles:
- summarize observed behavior
- do not predict or assume future state
- become known at specific points in time

Nothing “snaps into existence” retroactively.

If an artifact would not exist yet in live trading, it must not exist yet in the system.

## Engine Consistency Rule

All derived outputs must come from one runtime state-engine timeline:

`initialize -> apply_bar -> snapshot`

This applies to indicators, overlays, signals, strategy previews, bot runtime, and playback views.

- Do not add alternate reconstruction paths for the same artifact.
- Do not read mutable engine internals from outside the engine.
- If required data is missing from `snapshot.payload`, add it to the engine contract.
- If a consumer cannot run from snapshots, fail loud with actionable context.

This rule exists to prevent semantic drift and preserve trust across the platform.

---

## Logging Is Part of the Product

Logging is not optional and not cosmetic.

Logs must make it possible to trace:
QuantLab → Strategy → Bot → Trades → Playback

### Logging Principles
- Prefer structured logs (key=value or JSON)
- Log lifecycle boundaries, not noise
- One event = one log line with full context
- Never swallow errors to “keep things running”

### Debugging Guidance
- If the root cause isn’t clear, add targeted, temporary logs to observe state transitions—do not ship workarounds that mask the issue.
- Prefer stabilizing dependencies (refs, memoized callbacks) before adding logs; throttle diagnostics and remove them once the fix is in.
- For container log inspection in this environment, prefer `docker logs --tail <N>`; `--since` is not reliable here.

### Required Correlation Fields (when applicable)
Include these whenever they exist:
- `run_id`, `bot_id`, `bot_mode`
- `strategy_id`
- `indicator_id`, `indicator_type`, `indicator_version`
- `provider`, `venue`, `exchange`
- `symbol`, `timeframe`
- `trade_id`
- `bar_time` / `playback_time`

### Log Levels
- **DEBUG** — internal mechanics, cache behavior, counters
- **INFO** — lifecycle events and stage transitions
- **WARN** — unexpected but recoverable states (always explain why)
- **ERROR** — failed actions or invalid results (never swallowed)

If a fallback is used, it must emit a WARN explaining why.

---

## Error Handling Rules

- Do not swallow exceptions
- Do not silently skip invalid states
- Prefer failing early over producing incorrect output
- Errors must include context (IDs, symbol, timeframe, lifecycle stage)

A system that hides errors cannot be trusted or improved.

---

## Engineering Discipline

### Prefer Simplicity Early
- Solve the current problem clearly
- Avoid speculative abstractions
- Refactor when duplication or pressure appears

### Abstractions Belong in Core
Use interfaces and abstractions when:
- multiple implementations already exist
- behavior varies by environment (providers, execution)
- testing requires substitution

Do not abstract leaf logic “just in case.”

### Prefer Interfaces at Boundaries
Good boundaries include:
- data providers
- execution adapters
- storage layers
- fee / margin models

Avoid switch statements in core services.
Use registries and explicit registration instead.

### Schema Expectations
- Runtime must not perform implicit migrations or data backfills.
- Current ORM/model definitions are the canonical clean-schema description.
  Startup bootstrap may create missing clean-model tables and enforce only the
  explicitly reviewed bootstrap clauses owned by the persistence boundary.
- If existing columns disagree with the current contract, fail loud with an
  actionable error; do not silently patch or alter them in place.
- Existing deployments change through explicit, reviewed operator cutovers
  outside runtime. Preserve their manual SQL and runbooks as historical and
  operational evidence; they do not outrank the current model contract.

---

## Refactor Signals

Refactor when:
- logic appears in 2+ places
- a class or function has multiple responsibilities
- adding a feature requires touching unrelated files
- conditionals become a pile of special cases

Do not refactor blindly.
Refactor with logs, tests, or concrete pressure.

---

## Non-Negotiable Rule

> If you must choose between convenience and correctness, choose correctness.

Quant-Trad is designed to be explainable first.
Performance, polish, and optimization come second.

---

## Docs Sync Workflow

- After updating files in this repo, run `make sync-docs`.
- `make up` and `make build` also trigger `sync-docs` automatically.
- Configure destination per machine using:
  - `SYNC_DOCS_DEST`
  - or `OBSIDIAN_SYNC_DOCS_DEST`
  - optional local override file: `.sync-docs.mk`

## Developer Workflow

- Use `qt` as the primary command surface for agent/tool workflows and
  operations: bot runs, experiments, provider checks, report summaries, report
  exports, and comparisons.
- Use the UI for human visualization and inspection. Do not use frontend state
  as workflow truth.
- Use `make help` as the repo-native support index for Docker, DB, validation,
  git, local stack control, and direct forensic helpers.
- Use `docs/engineering/developer-workflow.md` for the standard Codex
  and local development workflow before inventing new one-off commands.
- Keep local support and forensic helpers in existing locations such as the root
  `Makefile`, `scripts/reporting/`, and `docs/engineering/`; do not add new
  root-level workflow files or folders. Normal bot/run/report workflows belong
  in `qt`, not Make.

### Normal Validation Matrix

Run focused checks while working. Before handoff, run every applicable row
below; broad or cross-system changes should run the full matrix. All database
and configuration checks use disposable local inputs. Never point validation at
production or live systems, load real credentials, deploy a host, or enable
external-order submission.

| Area | Normal command or check | Required scope |
|---|---|---|
| Documentation and indexes | `make validate-docs` | Documentation, contracts, glossary, architecture metadata, or generated-index changes; also broad handoff validation. |
| Non-database Python | `make backend-check` | Backend, CLI, domain, service, configuration, or cross-system changes. |
| Disposable database | `./scripts/ci/run_test_suite.sh db` | Persistence, schema, repository, recovery-guard, or database-backed behavior; requires the isolated Docker test stack. |
| Frontend | `make frontend-check` | UI, frontend adapters, API-view contracts, or broad handoff validation. |
| Deployment/configuration without deployment | `bash -n scripts/automation/server_deploy.sh`, `bash -n scripts/automation/server_host_bootstrap.sh`, then `docker compose --env-file <disposable-env> -f docker/docker-compose.server.yml config --quiet` | Deployment scripts, Compose/configuration, or broad handoff validation. Render configuration only; do not run deploy, credential, or remote-host actions. |
| Diff and clean tree | `git diff --check`; after committing the intended work, require empty `git status --porcelain=v1` | Every handoff. Inspect failures and preserve unrelated user changes rather than staging them for cleanliness. |

Record unavailable prerequisites honestly. A skipped database, frontend, or
configuration row is an unavailable validation result, not a passing result.

## Commit Hygiene

- Commit coherent slices as they become reviewable instead of saving every
  change for branch closeout.
- Prefer small one-line messages in the existing `<area>: <core change>` style.
- Use `make commit msg="area: core change"` when the whole staged scope belongs
  together; otherwise stage explicit paths and run `git commit -m` directly.
- Never stage unrelated local changes just to make the tree clean.

## Architecture Docs Tagging + Index Workflow

When a change materially affects runtime/service/provider/storage/reporting architecture, docs updates are required in the same pass.

Required workflow:
1. Locate existing component docs via `docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md` before changing architecture.
2. Update/create relevant component docs under `docs/architecture/<subsystem>/`.
3. Follow `docs/engineering/documentation/component-documentation-standard.md`.
4. Refresh the architecture index with:
   - `python scripts/docs/build_architecture_index.py`
5. Run `make sync-docs` after doc updates.

Agent expectation:
- Prefer component-targeted doc updates over broad vague edits.
- Treat frontmatter `code_paths` as navigation and coverage, not exclusive file
  ownership.
- Platform contracts remain authoritative over narrower architecture notes.
- Runtime composition/wiring changes must keep docs and index in sync.
- Runtime composition changes should preserve mode-aware seams (`backtest`/`paper`/`live`) even when only backtest is implemented today.
- If you touch code paths listed in `code_paths`, verify corresponding docs remain accurate.
