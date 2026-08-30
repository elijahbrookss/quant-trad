# CI Test Topology

This document describes the GitHub Actions topology defined by
[`.github/workflows/test.yaml`](../../../.github/workflows/test.yaml). The
workflow runs for pushes to `develop` or `main` and for pull requests targeting
either branch.

## Goals

- Fail fast on backend, frontend, deployment-contract, and database regressions.
- Keep optional-provider and database boundaries explicit.
- Use pinned dependency inputs for Python and frontend jobs.
- Exercise database contracts only against disposable CI databases.
- Preserve the difference between a passing workflow and runtime, deployment,
  or production proof.

## Current Workflow Topology

The workflow defines exactly four jobs. None declares `needs`, so GitHub may run
them concurrently. The numbering below is for documentation only.

| # | Job ID | Primary boundary |
| ---: | --- | --- |
| 1 | `pr-suite` | Complete ordinary non-database backend pytest screen on the runner host |
| 2 | `frontend` | Current frontend test command plus production asset build |
| 3 | `deployment-contract` | Server shell/Compose validation and attested production-image builds |
| 4 | `clean-database-bootstrap` | Clean-schema bootstrap followed by PostgreSQL-marked contract tests |

### 1. `pr-suite`

Runner: `ubuntu-latest` with Python 3.12 and pip caching keyed by
`requirements.lock`.

The workflow steps are:

1. `Checkout repository` with full Git history;
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

Runner: `ubuntu-latest`, with a 45-minute timeout.

The workflow steps are:

1. `Checkout repository`;
2. `Generate disposable operator environment` by running
   `bash scripts/automation/server_deploy.sh init-env` with
   `QT_SINGLE_NODE_ENV_FILE` bound to the checkout's `secrets.env`;
3. `Validate shell and Compose contracts` by syntax-checking both server
   scripts, computing the Git revision and source-tree hash, and running
   `docker compose ... config --quiet`; and
4. `Build production images with source attestation` by recomputing the
   revision/tree hash and building `tsdb`, `backend`, `frontend`, and
   `frontend-v2` from `docker/docker-compose.server.yml`.

This job validates configuration and image construction. It does not start the
composed platform, deploy to a server, exercise health checks, or perform a
rollback.

### 4. `clean-database-bootstrap`

Runner: `ubuntu-latest`, with a
`timescale/timescaledb:2.14.2-pg15` service exposed on localhost port 5432. The
service starts `quanttrad_bootstrap` for user `quanttrad` with trust
authentication.

The workflow steps are:

1. `Checkout repository`;
2. `Set up Python` 3.12;
3. `Install dependencies` from `requirements.lock` and run `pip check`;
4. `Prepare isolated database contracts` by creating `quanttrad_contracts` and
   installing `timescaledb` and `pgcrypto` in both databases;
5. `Prove clean current-schema bootstrap` against `quanttrad_bootstrap`; and
6. `Run PostgreSQL-backed contract tests` against `quanttrad_contracts`, with
   the clean-bootstrap test excluded from this second invocation.

Clean bootstrap and PostgreSQL-marked verification are two sequential steps in
this fourth job. They are not separate workflow jobs. Both set
`RUN_DB_TESTS=1`, `QT_DB_TEST_ISOLATED=1`, disable Loki delivery, and use an
explicit disposable DSN.

The clean-bootstrap database begins from the service image's empty application
schema. Most DB-marked tests share `quanttrad_contracts` within the job.
Migration tests that own historical schema ordering create and tear down a
unique child database from that disposable PostgreSQL service. The workflow
does not claim per-test database isolation for the rest of the DB corpus.

## What The Workflow Protects

The four-job workflow catches:

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
export QT_SINGLE_NODE_ENV_FILE="$PWD/secrets.env"
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
  --env-file secrets.env \
  --file docker/docker-compose.server.yml \
  config --quiet
docker compose \
  --env-file secrets.env \
  --file docker/docker-compose.server.yml \
  build tsdb backend frontend frontend-v2
```

The workflow computes revision/tree-hash material separately in each of its two
validation/build steps; reusing the same values locally is equivalent only while
the checkout remains unchanged between commands.

### Reproduce `clean-database-bootstrap`

There is no single local wrapper that exactly reproduces both CI database
steps. For exact topology, provision a fresh disposable
`timescale/timescaledb:2.14.2-pg15` service with user `quanttrad`, database
`quanttrad_bootstrap`, trust authentication, and localhost port 5432. Create a
second database named `quanttrad_contracts`, then install `timescaledb` and
`pgcrypto` in both databases.

After installing the Python dependencies as in `pr-suite`, run the two CI test
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

PG_DSN="$QT_CI_CONTRACT_DSN" \
RUN_DB_TESTS=1 \
QT_DB_TEST_ISOLATED=1 \
QT_LOGGING_LOKI_URL='' \
QT_LOGGING_DEBUG='false' \
python -m pytest -q -m db \
  --ignore=tests/test_portal/test_clean_database_bootstrap_db.py
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
