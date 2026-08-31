# Operator Handbook

This is the operator landing page for a local, VPS, or on-prem Quant-Trad
installation. It points to the exact runbooks and commands that own each
operation so the deployment does not depend on chat history or one person's
memory.

## Source of truth

| Operator task | Canonical source |
| --- | --- |
| Install, deploy, update, rollback, SSH access, storage move | [Portable single-node deployment](../engineering/server-deployment.md) |
| Local workstation setup | [Getting started](../getting-started.md) |
| Provider credentials and provider implementation | [Adding a provider](../guides/adding-a-provider.md) |
| Inspect, diagnose, start, stop, pause, resume, or restart collectors | [Collector operations](../guides/collector-operations.md) |
| Configure recipients, validate, test, and operate alerts | [Operator email alerting](alerting.md) |
| Coinbase product metadata and stream proof | [Coinbase derivatives setup](../guides/coinbase-derivatives-paper-setup.md) |
| Chainlink scheduled collection | [Chainlink structured Facts](../guides/chainlink-structured-facts.md) |
| Runtime ownership and recovery semantics | [Continuous collector runtime](../architecture/data/CONTINUOUS_COLLECTOR_RUNTIME.md) |
| Secrets and trust boundaries | [Security layer](../architecture/security/SECURITY_LAYER.md) |

The deployed CLI is the executable reference for the running release:

```bash
bash scripts/automation/server_deploy.sh qt --help
bash scripts/automation/server_deploy.sh qt data collectors --help
```

Documentation describes the workflow; CLI help describes the exact arguments
accepted by that release.

## First install

The supported V1 target is one Ubuntu host running the complete stack under
Docker Compose. Follow the [single-node deployment runbook](../engineering/server-deployment.md)
in order:

1. run the one-time privileged host bootstrap;
2. create the private operator environment with `init-env`;
3. run `doctor`;
4. deploy one reviewed commit SHA;
5. verify `status`, `release`, and `fleet`; and
6. use SSH forwarding for private operator surfaces.

A new database initializes directly at the current schema. Historical upgrade
scripts are optional tools for preserving an older database; they are not part
of a clean install.

## Routine update

Promote a reviewed commit, not a moving branch name:

```bash
bash scripts/automation/server_deploy.sh doctor
bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>
bash scripts/automation/server_deploy.sh release
bash scripts/automation/server_deploy.sh status
bash scripts/automation/server_deploy.sh fleet
```

The deploy helper builds commit-tagged images, drains the collector, verifies
the initializer and service health, checks embedded source attestations, and
records the previous successful revision. Use `rollback` only when the older
application is compatible with the persisted data contract.

## Provider onboarding

Public Coinbase market products, public trades, and public Level 2 do not need
credentials. Add a Coinbase credential only for an authenticated stream or an
account-scoped workflow:

```bash
bash scripts/automation/server_deploy.sh credentials-coinbase
```

The credential is written through the backend into the encrypted credential
store. It does not belong in `secrets.env`, shell history, a collector
definition, or a Git repository.

Chainlink's default scheduled binding uses the endpoint referenced by
`CHAINLINK_ARBITRUM_RPC_URL` in the private operator environment. It does not
use Coinbase credentials.

## Add a product without deploying

An additional product does not require a new image when the running release
already contains its provider adapter and collector types. First run a bounded
public stream smoke check:

```bash
bash scripts/automation/server_deploy.sh qt providers stream-smoke \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --symbol <coinbase-product-id> \
  --product-id <coinbase-product-id> \
  --channel market_trades \
  --channel level2 \
  --auth-mode public \
  --duration 12
```

Then enroll the product through the deployed Coinbase futures collector pack:

```bash
bash scripts/automation/server_deploy.sh qt \
  data collector-definitions enroll-product \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --product-id <coinbase-product-id> \
  --actor-id <operator-id> \
  --reason "Approved market-data coverage" \
  --confirm
```

With no repeated `--collector` arguments, the command installs the supported
Coinbase futures pack: scheduled open interest, scheduled funding rate,
continuous trades, and continuous Level 2. Use repeated `--collector` values
to select a subset. New definitions start running. Reapplying the command is
idempotent and updates reviewed configuration without undoing a later audited
pause, stop, or restart intent.

The backend validates the live provider product, persists its canonical
instrument metadata, derives the exact contract translation, and admits only
collector adapters already registered by the deployed release. The collector
worker discovers the new definitions through the database; the application
stack is not rebuilt or restarted.

Verify the result through the same operational plane:

```bash
bash scripts/automation/server_deploy.sh qt instruments list \
  --datasource COINBASE \
  --exchange COINBASE_DIRECT \
  --symbol <coinbase-product-id>
bash scripts/automation/server_deploy.sh fleet
```

Inspect each returned collector until scheduled provider activity and
continuous stream ownership are healthy. A newly enabled latest-state poll has
history only from enrollment forward; QT does not invent earlier OI or funding
coverage.

## What requires a deployment

| Change | Deploy? | Reason |
| --- | --- | --- |
| Start, stop, pause, resume, restart, or probe an existing collector | No | Durable operator intent is control-plane state. |
| Store or rotate a provider credential | No | Credentials use the encrypted provider boundary. |
| Add another product supported by a deployed collector pack | No | Product metadata and definitions are validated at enrollment. |
| Change the poll interval while re-enrolling a supported product | No | The scheduled definition is durable configuration. |
| Change alert recipients or delivery credentials | Grafana only | Run `validate-alerts` then `apply-alerts`; collectors and other services are not restarted. |
| Add a provider transport, WebSocket channel, projection, Fact schema, or recovery behavior | Yes | Executable behavior and contracts must pass review and CI. |
| Change database or archive contracts | Yes | Persistence compatibility and clean-schema validation are release concerns. |
| Change the default clean-install fleet | Yes | Defaults are reviewed source-controlled release material. |

Product enrollment is a separate admin boundary. It does not make arbitrary
collector JSON editable from the browser and does not let an operator invent a
new collector type at runtime.

Chainlink uses the same scheduled collector lifecycle, but adding a feed is not
currently the same operation as adding a Coinbase product. A Chainlink binding
also fixes the network, feed contract address, dimensions, unit, endpoint
reference, and provenance contract. The V1 bindings therefore remain reviewed
manifests. Adding another binding needs a configuration release until a
similarly bounded Chainlink enrollment pack exists; it does not need a new
collector runtime.

## Daily collector operation

Use **Operations -> Market** in Frontend V2 or the exact CLI views:

```bash
bash scripts/automation/server_deploy.sh fleet
bash scripts/automation/server_deploy.sh qt data collectors plane
bash scripts/automation/server_deploy.sh qt \
  data collectors detail <scheduled_fact-or-continuous_stream> <collector-id>
bash scripts/automation/server_deploy.sh qt \
  data collectors diagnose <scheduled_fact-or-continuous_stream> <collector-id>
```

Diagnose before changing state. Preserve fleet/detail output, recent events,
gaps, provider success, accepted Fact time, worker/lease evidence, and the
operation request ID when recording an incident.

An isolated `provider_trade_side_unknown` event means the provider supplied a
trade outside the proven BUY/SELL maker-side contract. QT retains the exact raw
frame, quarantines only those trades, continues the collector, and marks the
affected live-flow coverage invalid rather than inventing a side. Inspect the
event's raw-record reference and rejected count; a repeating collector failure
or growing retained spool is not the expected state and should be treated as a
runtime incident.

## Backups, storage, and recovery

The database, market-structure archive, deployment state, and private operator
environment have different recovery roles:

- PostgreSQL holds canonical Facts, definitions, lifecycle intent, and audit
  evidence. Back it up and restore-test it.
- The market-structure root holds durable raw archive/checkpoint objects. Move
  it only with a drained collector and checksum verification.
- The deployment-state directory records current and previous release SHAs; it
  is not a database backup.
- `secrets.env` contains infrastructure secrets and the credential encryption
  key. Back it up securely or encrypted credential references may become
  unreadable.

Follow the storage move and rollback procedures in the deployment runbook;
never clear leases or rewrite evidence with ad hoc SQL during normal recovery.

## Adding another node

The V1 deployment contract is one active application/collector host. A second
machine is straightforward as an independent research node with its own
database and archive, or as a cold standby restored from verified backups.

Do not point a second full Compose stack at the same database while each host
uses local archive storage and call that horizontal scaling. A shared active
deployment needs an explicit design for shared object storage, database
connectivity, unique worker identity, lease/fencing behavior, ingress/TLS,
backups, failover, and capacity. The collectors already expose ownership and
fencing primitives, but the V1 server preset has not claimed the complete
multi-node operating contract.

## Documentation ownership

When an operator command, environment variable, default service, credential
flow, enrollment boundary, backup path, or recovery behavior changes, update
this handbook and the linked detailed runbook in the same pull request. Record
incident-specific evidence under `docs/incidents/`; keep durable architecture
decisions under `docs/architecture/decisions/`.
