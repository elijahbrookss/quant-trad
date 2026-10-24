# CI Test Topology

This document describes the GitHub Actions topology defined by
[`.github/workflows/test.yaml`](../../../.github/workflows/test.yaml). The
workflow runs for pushes to `develop`, `main`, `feature/**` and `hotfix/**`, and
for pull requests targeting `develop` or `main`.

## Goals

- Fail fast on backend, frontend, deployment-contract, and database regressions.
- Keep optional-provider and database boundaries explicit.
- Use pinned dependency inputs for Python and frontend jobs.
- Exercise database contracts only against disposable CI databases.
- Preserve the difference between a passing workflow and runtime, deployment,
  or production proof.

## Current Workflow Topology

The workflow defines fifteen jobs. `changes` computes requirements before the six
existing qualification jobs and the new `docs` check. Those checks can run
concurrently; `ci-required` always runs after their results. The numbering below
identifies the six existing qualification jobs for reproduction.

| # | Job ID | Primary boundary |
| ---: | --- | --- |
| 1 | `pr-suite` | Complete ordinary non-database backend pytest screen on the runner host |
| 2 | `frontend` | Current frontend test command plus production asset build |
| 3 | `deployment-contract` | Server shell/Compose validation and attested production-image builds |
| 4 | `clean-database-bootstrap` | Clean-schema bootstrap, private namespace verification, then PostgreSQL-marked contract tests |
| 5 | `deployment-rehearsal` | Real deployment controller with synthetic Docker services and injected rollout failure |
| 6 | `committed-recovery` | Native Linux missing-config recovery after a committed switch and expired final window, against current and first-release runtimes |

`committed-recovery` expands into two matrix runs: the current revision and the
pinned first-release revision `24357ff387776822f676ae2a8b1cce7f209c313e`. Each builds
its attested test runtime, uses synthetic guarded source peers and independent
disposable PostgreSQL/archive storage, reproduces the missing repository input,
and exercises the explicit continuation and completion observation. Neither run
uses production inputs or proves production throughput. The 25-minute job timeout
bounds fixture setup and teardown; the operation keeps its own smaller limits.
This runs on native Linux because Docker Desktop remaps bind paths and cannot
qualify the exact Linux host identity checks.

### 1. `pr-suite`

Runner: `ubuntu-latest` with Python 3.12 and pip caching keyed by
`requirements.lock`.

The workflow steps are:

1. `Checkout repository`;
2. `Set up Python`;
3. `Install dependencies` with `pip install --no-deps -r requirements.lock`
   followed by `python -m pip check`; and
4. `Run PR suite` with `./scripts/ci/run_test_suite.sh pr`.

The `pr` suite runs `QT_OMIT_DB_TESTS=1 pytest -q`. It is the complete ordinary
non-database backend screen, not the whole workflow gate. Missing locked Python
dependencies fail collection; they do not silently turn supported coverage into
skips.

### 2. `frontend`

Runner: `ubuntu-latest` with Node.js 20 and npm caching keyed by
`portal/frontend/package-lock.json`.

The workflow steps are:

1. `Checkout repository`;
2. `Set up Node.js`;
3. `Install pinned frontend dependencies` with
   `npm --prefix portal/frontend ci`; and
4. `Test and build production frontend` with `make frontend-check`.

`frontend-check` invokes the package's `npm test` command and the Vite
production build. `npm test` runs every `tests/**/*.test.js` file through the
shell-free Node-native suite and every `src/**/*.test.jsx` file through the
pinned Vitest/jsdom profile. It does not run lint, a real backend, a browser or cross-browser/E2E
suite, production deployment, live collector or order activity, or
accessibility conformance.

### 3. `deployment-contract`

The stable `deployment-contract` check aggregates six independent proof job
families. `deployment-proof-build` builds and source-attests production and test
images once, collects the original explicit online selection, and publishes an
immutable transport bound to source, run, attempt, archive SHA-256 and image IDs.
The transport is provisional; it is not a qualified release package.

Four separate runners own the unchanged alert, lifecycle/prepared-key recovery,
application/current-and-frozen-reader restore, and core/storage-layout groups.
`deployment-proof-online` assigns the complete collected node IDs to six shards,
including individual parameter cases within the long controller file. At most
four online shard runners execute concurrently. The scheduling seed comes from
run 38999753104; new collected cases are included automatically. It never filters
coverage. Each shard retains its own generated Docker project, credentials,
internal network, database and source/history volumes. No tests share a database
between shard runners. The real expiry and Timescale recovery waits are unchanged.

Every consumer checks the exact workflow checkout, source hash, run/attempt,
transport checksum and loaded image identities. Prebuilt Compose overrides may
only select these admitted image IDs in the two existing storage proof suites;
no consumer rebuilds or pulls a substitute test image. Each prebuilt proof also
renders the actual Compose model and refuses inherited build recipes, changed
image IDs, pulls, host ports or a non-internal network. Local/default runner
behavior remains unchanged.

The aggregate requires all independent jobs to succeed and reads their retained
receipts and diagnostic logs. It rejects missing/duplicate groups, missing or
duplicate online cases/phases, wrong source/image/attempt, checksum drift,
failures, cancellation and unexpected skips. Only the existing prepared-
transaction case may skip with its exact max_prepared_transactions reason.
It rechecks image identities before exporting the qualified routine package.
The external exact-source admission helper also requires every expanded proof
job and the successful aggregate attestation, rather than trusting a green
wrapper alone. Partial diagnostics are uploaded on failure. The existing current
and first-release committed recovery jobs remain separately mandatory.

The fifteen job definitions expand to twenty-one jobs, including both recovery
variants and all six online shards. `ci-required` keeps its stable name and
computed scope; develop/main and PR release qualification remain full. Superseded
feature/hotfix PR runs now cancel using `github.head_ref`, while protected-branch
push behavior is preserved. There is no permission, protected-rule or production
trigger change.

Timing is a measured-trial question. Shard projections from one historical run
exclude transport/build overhead and queue variance. The whole-workflow 20–25
minute goal is not a performance claim until a complete instrumented run passes.
`QT_CI_TEST_PHASES=1`, per-group wall time, setup/call/teardown outcomes and all
original logs support the comparison.

### 4. `clean-database-bootstrap`

Runner: `ubuntu-latest`, with a
`timescale/timescaledb:2.14.2-pg15` service exposed on localhost port 5432. The
service starts `quanttrad_bootstrap` for user `quanttrad` with trust
authentication.

The workflow steps are:

1. `Checkout repository`;
2. `Set up Python` 3.12;
3. `Install dependencies` from `requirements.lock` and run `pip check`;
4. `Prepare isolated database contracts` by disabling telemetry in the
   disposable service, creating `quanttrad_contracts`, and installing
   `timescaledb` and `pgcrypto` in both databases;
5. `Prove clean current-schema bootstrap` against `quanttrad_bootstrap`;
6. `Verify PostgreSQL filesystem namespace` through the Docker DB runner; and
7. `Run PostgreSQL-backed contract tests` against `quanttrad_contracts`, with
   the clean-bootstrap and namespace fixture files excluded from this invocation;
   and
8. `Prove frozen baseline refuses archive-generation reader floor` by starting the
   source-attested pre-generation application image against its own disposable
   layout, applying the current metadata-only migration, and requiring the same
   frozen image to refuse the changed reader contract.

Clean bootstrap, namespace verification, PostgreSQL-marked contracts and the
frozen-reader proof are four sequential steps in this fourth job. They are not
separate workflow jobs.
The two host invocations set `RUN_DB_TESTS=1`, `QT_DB_TEST_ISOLATED=1`, disable
Loki delivery, and use an explicit disposable DSN. The namespace step uses the
repository's isolated Docker runner because its fixture needs PostgreSQL 15
server utilities and a private process/filesystem environment. The fixture
starts a temporary Unix-socket-only cluster with no inherited database
credentials and a synthetic udev UUID entry; it does not exercise live disks.
The storage-demo proof collection also includes a named nondefault-tablespace
generation migration: the prepared alias and canonical-dependency heap, TOAST
relation and indexes must inherit their legacy catalog's owned history placement.
The reader-floor proof uses the immediate pre-generation application commit and
the localhost-only disposable PostgreSQL service. Neither proof deploys or
contacts production.

The disposable CI service disables extension telemetry and reloads its
configuration before database setup, matching the local isolated stack.
Telemetry jobs must not affect the database-cleanup qualification.

The clean-bootstrap database begins from the service image's empty application
schema. Most DB-marked tests share `quanttrad_contracts` within the job.
Migration tests that own historical schema ordering create and tear down a
unique child database from that disposable PostgreSQL service. The workflow
does not claim per-test database isolation for the rest of the DB corpus.

## What The Workflow Protects

The workflow catches:

- non-database backend import, logic, provider-wiring, runtime, accounting,
  reporting, BotLens, CLI, MCP, and service-contract regressions;
- failures discovered by the frontend package's current test command and Vite
  production build;
- server-script syntax errors, invalid server Compose configuration, source
  attestation/build failures, and failures to build the four production images;
- clean current-schema bootstrap failures on PostgreSQL 15 / TimescaleDB
  2.14.2; and
- tests currently marked `db` against the job's disposable contract database.

## Honest Ceilings

A passing workflow does not by itself prove:

- composed-service networking or runtime behavior after containers start;
- deployment, rollback, backup restoration, or server health on an actual host;
- full backtest, paper, or live runtime orchestration;
- browser-specific frontend behavior or frontend tests outside the current
  package test discovery;
- per-test database isolation within `quanttrad_contracts`;
- behavior against an existing migrated operator database; or
- external-provider availability, live order submission, production safety, or
  production readiness.

The workflow uses only its disposable GitHub-hosted environment. Shared
development, live, and production databases are outside this topology.

## Local Reproduction By Job

Use a clean checkout of the revision under review. Match Python 3.12, Node.js
20, the checked-in lockfiles, and Docker where the job requires it. Never point
database reproduction at a shared development, live, or production database.

### Reproduce `pr-suite`

```bash
python -m pip install --upgrade pip
pip install --no-deps -r requirements.lock
python -m pip check
./scripts/ci/run_test_suite.sh pr
```

The `backend` suite is an alias for the same non-database pytest boundary, but
the workflow calls `pr`.

### Reproduce `frontend`

```bash
npm --prefix portal/frontend ci
make frontend-check
```

### Reproduce `deployment-contract`

Run this only in a disposable checkout because `init-env` writes the named
operator-environment file.

```bash
qt_ci_private="$(mktemp -d "${TMPDIR:-/tmp}/qt-disposable-operator.XXXXXX")"
chmod 0700 "$qt_ci_private"
export QT_SINGLE_NODE_ENV_FILE="$qt_ci_private/qt-disposable.env"
export QT_SERVER_ENV_FILE="$QT_SINGLE_NODE_ENV_FILE"
bash scripts/automation/server_deploy.sh init-env
bash -n scripts/automation/server_deploy.sh
bash -n scripts/automation/server_host_bootstrap.sh

qt_ci_revision="$(git rev-parse HEAD)"
qt_ci_tree_hash="$(
  python3 scripts/provenance/source_tree_hash.py \
    --root . \
    --git-revision "$qt_ci_revision"
)"
export QT_RELEASE_REVISION="$qt_ci_revision"
export QT_SOURCE_TREE_HASH="$qt_ci_tree_hash"

docker compose \
  --env-file "$QT_SINGLE_NODE_ENV_FILE" \
  --file docker/docker-compose.server.yml \
  config --quiet
docker compose \
  --env-file "$QT_SINGLE_NODE_ENV_FILE" \
  --file docker/docker-compose.server.yml \
  build tsdb backend frontend frontend-v2
```

The workflow computes revision/tree-hash material separately in each of its two
validation/build steps; reusing the same values locally is equivalent only while
the checkout remains unchanged between commands.

### Reproduce `clean-database-bootstrap`

There is no single local wrapper that exactly reproduces all three CI database
steps. For exact topology, provision a fresh disposable
`timescale/timescaledb:2.14.2-pg15` service with user `quanttrad`, database
`quanttrad_bootstrap`, trust authentication, and localhost port 5432. Create a
second database named `quanttrad_contracts`, then install `timescaledb` and
`pgcrypto` in both databases. Before this setup, disable
`timescaledb.telemetry_level` in that disposable service and reload its
configuration, as the CI preparation step does. Do not apply these fixture
settings to a live database.

After installing the Python dependencies as in `pr-suite`, run the three CI test
steps separately:

```bash
QT_CI_BOOTSTRAP_DSN='postgresql+psycopg2://quanttrad@127.0.0.1:5432/quanttrad_bootstrap'
QT_CI_CONTRACT_DSN='postgresql+psycopg2://quanttrad@127.0.0.1:5432/quanttrad_contracts'

PG_DSN="$QT_CI_BOOTSTRAP_DSN" \
QT_CLEAN_BOOTSTRAP_TEST_DSN="$QT_CI_BOOTSTRAP_DSN" \
RUN_DB_TESTS=1 \
QT_DB_TEST_ISOLATED=1 \
QT_LOGGING_LOKI_URL='' \
QT_LOGGING_DEBUG='false' \
python -m pytest -q tests/test_portal/test_clean_database_bootstrap_db.py

./scripts/ci/run_test_suite.sh db tests/test_market_data/test_header_namespace_db.py

PG_DSN="$QT_CI_CONTRACT_DSN" \
RUN_DB_TESTS=1 \
QT_DB_TEST_ISOLATED=1 \
QT_LOGGING_LOKI_URL='' \
QT_LOGGING_DEBUG='false' \
python -m pytest -q -m db \
  --ignore=tests/test_portal/test_clean_database_bootstrap_db.py \
  --ignore=tests/test_market_data/test_header_namespace_db.py
```

`./scripts/ci/run_test_suite.sh db` remains the convenient repository-owned
local DB profile. It builds a source image that excludes local environment and
secret files, generates a unique synthetic database identity and Compose
project for every invocation, runs on an internal network without a host port,
and tears down only that project's containers, network, volumes, and locally
built test image. It is still not an exact
reproduction of the fourth workflow job's two-database layout and separate
clean-bootstrap invocation.

## Ownership And Maintenance

- [The workflow](../../../.github/workflows/test.yaml) owns job IDs, runner and
  service definitions, and the direct commands for frontend, deployment, and
  database jobs.
- [`scripts/ci/run_test_suite.sh`](../../../scripts/ci/run_test_suite.sh) owns
  the backend pytest suite commands called by `pr-suite` and local profiles.
- [`portal/frontend/package.json`](../../../portal/frontend/package.json) and
  [the Makefile](../../../Makefile) own `frontend-check` test/build composition.
- [`tests/conftest.py`](../../../tests/conftest.py) owns DB opt-in and isolated
  DSN guards, required-dependency checks, and suppression of implicit dotenv
  discovery; the workflow owns the two CI database invocations.
- Update this document whenever workflow jobs or their step boundaries change.

Do not infer full runtime, deployment, or production coverage from a job name. Read the commands and environment of the exact workflow step.

### 5. `deployment-rehearsal`

Runner: `ubuntu-latest`, with a 15-minute timeout. After checkout it runs
`python3 scripts/ci/test_server_promotion.py`. The production deployment helper
uses real Git, Docker Compose, images, health gates, locks, and isolated volumes
with tiny synthetic services and local GitHub API responses. It verifies CI
rejection, compatible promotion, failed rollout recovery, synthetic worker
drain/restart, and volume continuity. It does not replace the real-QT core smoke
or the PostgreSQL and collector contract suites. Reproduce it with the same
command on a Docker-enabled local machine; no provider credentials are needed.

## Local disposable storage demonstration

The storage-demo suite of scripts/ci/run_test_suite.sh exercises the storage
release on an isolated PostgreSQL instance with separate disposable
source/history filesystems. Its test service uses the backend Dockerfile's
storage-test target, which inherits production code, dependencies and PostgreSQL
tools and adds test inputs. Normal database tests retain their existing test
image. The final production target excludes these added storage-test inputs.

This local demonstration is not another GitHub Actions job or a deployment.
Its internal network, generated credentials, owned resources and cleanup remain
unchanged. A passed run supports only the exercised disposable layout; actual
HDD performance, preserving migration duration and server permissions require
their own evidence.

The committed-recovery matrix also exercises a retained database image missing
pgBackRest. It reproduces the historical missing-config failure, then completed
repository setup blocked on WAL archival, and waits for both actual attempt
expiries. The explicit v2 correction replaces only the database image while
preserving PGDATA, keys and completed preparation. The same current/frozen reads,
encrypted pair and read-only completion checks must pass afterward. This test
uses generated disposable inputs; it does not measure production outage or grant
production operation authority. Its 25-minute job bound includes image builds
and deliberate expiry waits, not a production completion estimate.


## Conservative work-branch selection

`scripts/ci/ci_impact.py` owns the small allowlist. Feature/hotfix pushes compare
all branch contributions to the merge base with the workflow's explicit
`QT_CI_INTEGRATION_REF` (default `origin/develop`). Earlier changes remain in
scope even if a predecessor push run was cancelled. A deleted path and its
renamed replacement both participate. For a separately coordinated feature
integration, review and set that baseline explicitly; do not infer it from the
last push or an unqualified predecessor. PRs into release branches, develop/main pushes,
and manual workflow dispatch retain complete qualification. Use Actions **Run
workflow** with **force_full** to request complete checks on a work branch.
Missing/zero bases, missing history, invalid comparisons, unknown paths and empty
diffs select all checks. Mixed changes take the union, and any high-risk path
selects the complete workflow. The plan records base/head, paths and requirements
in the selection log; it never treats unavailable coverage as a passing test.

| Scope | Work-branch requirement |
| --- | --- |
| Markdown and static documentation assets | `docs` |
| Exact reviewed presentation files in `ci_impact.PRESENTATION` | `docs`, `frontend` |
| Reviewed event_statistics, directional_model, metrics modules and test_event_fact_check | `docs`, complete ordinary `pr-suite` |
| Evaluators, registry/catalog, request/result serialization, frozen bindings, repositories, dependencies, configuration, storage, migrations, shared infrastructure or any unlisted path | Complete workflow |

The calculation allowlist is a test-environment boundary, not permission to
change historical scientific meanings or eligibility for automatic deployment.
The presentation inventory currently contains index.css, v2.css,
v2-operator.css, ActivityHeatmap.jsx, WarningPanel.jsx and BotLensHeader.jsx at
the exact paths in the selector. Credential dialogs, mixed chart containers,
application roots and new/unlisted frontend files select the full workflow.
Do not add evaluator or frontend globs. New eligible files require review of imports,
durable contract ownership and regression scope. Changes to the selector itself
receive full qualification.

`ci-required` checks every computed required job for success. Failure,
cancellation, a missing result or an unexpected skip fails the gate; an explicitly
unrequired skip remains a skip. The existing release receipt still requires all
seven expanded original jobs and both recovery bundles. A selected iteration run
cannot satisfy it. GitHub must require the stable `ci-required` check when this
policy is activated; changing repository rules needs separate approval. This
component does not change GitHub rules or any production trigger.

The deployment-contract job captures image IDs before runtime proofs, verifies
they are unchanged afterward, and exports backend/frontend/frontend-v2 in the
`routine-release-package` artifact with source SHA/hash, run/attempt, image IDs
and archive checksum. It adds artifact export time and storage proportional to
the images; seven-day retention matches recovery evidence. Infrastructure images
are not exported or upgraded by routine promotion.

The workflow gives its GitHub token read-only repository contents access and
disables checkout credential persistence. PR source still runs on disposable
runners without production secrets; PR evidence never qualifies a deployment.


### Actual routine-path disposable rehearsal

The existing `deployment-rehearsal` job now runs its original compatible operator
rehearsal and `test_server_promotion.py --routine`. The latter is admitted only
on a GitHub-hosted runner using its default local Docker socket; remote contexts
and local/production execution refuse before creating resources. It uses a unique
Compose project, private runner-temp fixture Git repo/environment, internal
networks, no host ports and owned volumes. Runtime applications are tiny synthetic
processes, not QT/provider clients; every enrollment flag is disabled. The actual
PostgreSQL container and psql/schema probe are real.

The routine mode invokes the actual `promote-routine` controller, package
capture/export/ZIP/load and immutable-ID pinning. It removes candidate image tags
before admission so a cached build cannot substitute for loading the package.
Local fake GitHub responses model a completed exact develop attempt and artifact
digest; they are explicitly synthetic admission metadata, not GitHub release
qualification. An actual tiny dump is restored on an independent owned PostgreSQL
volume before generating fixture-only recovery admission. The rehearsal checks
retained rows/schema/cluster, compatible success, active-hold/credential-scope/
changed-artifact/stale-restore refusal, unhealthy rollout recovery, interrupted
schema-drift hold and explicit reconciliation followed by offline recovery.
Docker-call traces reject build/pull during routine activation and recovery.

A ten-minute operation deadline, existing fifteen-minute job limit and bounded
owned cleanup constrain this test. Failure preserves a small `qualification.json`
with source, phases, outcome and cleanup errors. The `routine-controller-rehearsal`
artifact contains that receipt only; no environment, resolved recovery snapshot,
backup payload or credential is uploaded. Cleanup targets only the recorded
unique fixture resources, never a daemon-wide prune.

A passing non-Docker unit suite does not certify this path. The hosted run must
actually finish this routine step and its existing companions successfully.
After the source run completes, independently validate its real exported
`routine-release-package` download/digest/job/attempt binding. A work-branch
receipt remains handoff evidence only; production admission still requires an
exact develop push and the separately authorized installation/activation.

The routine fixture also proves that edits to its private operator file and
caller configuration overrides refuse before recovery-snapshot creation or
container replacement. Configuration fingerprints remain private to the fixture;
the uploaded receipt contains phase results only.
