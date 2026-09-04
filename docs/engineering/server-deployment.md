# Portable Single-Node Deployment

Quant-Trad's V1 deployment preset runs the complete application, market-data,
research, and observability stack on one Linux host. The host may be an on-prem
server or an Ubuntu VPS; the runtime contract is the same. Docker Compose owns
services, GitHub review and CI qualify a commit, and an operator promotes that
exact commit.

This is intentionally smaller than a platform deployment. Kubernetes,
self-hosted CI runners, automatic production promotion, and per-PR preview
stacks are not required for V1. Local development remains disposable, CI uses
an ephemeral database, and the single-node host is the durable environment.

## Release Story

One release moves through five visible boundaries:

1. A pull request runs the non-database suite, frontend tests/build, rendered
   Compose contract, production container builds, and PostgreSQL-backed suite.
   GitHub Actions starts a disposable TimescaleDB service container on the
   GitHub-hosted runner, proves an empty database initializes directly at the
   current schema, then runs all tests marked `db`.
2. `doctor` verifies Docker, Compose, the private operator environment, storage,
   source cleanliness, and the rendered Compose model on the target host.
3. `deploy` resolves one Git ref to a commit, installs the reviewed Python and
   frontend lockfiles, builds commit-tagged application images, and embeds both
   the revision and source-tree hash. The Python lock is part of that hash.
4. The backend initializes or validates the database. A one-shot initializer
   installs the reviewed code-owned instruments and market-data definitions,
   and only then may the durable collector worker start.
5. Health checks prove the API, frontends, Grafana, database, and collector
   heartbeat. The deploy helper verifies image attestations and records the
   current and previous successful revisions for rollback.

Deploy never executes historical upgrade scripts. An update reconciles services
only after the new images have built and the reviewed Compose model has rendered
successfully. Application containers are replaced for new commit-tagged images;
a service whose reviewed runtime configuration changed may also be recreated.
The collector gets a five-minute stop window to close its provider connections,
seal and finalize its WAL segments, and release fenced leases. Any real provider
downtime remains gap evidence.

## Full Service Surface

| Service | Purpose | Host access |
| --- | --- | --- |
| TimescaleDB | canonical runtime and research persistence | private Docker network only |
| backend | API, control plane, research operations, and bot launcher | `127.0.0.1:8000` |
| initialize | idempotent current-release definition enrollment | one-shot, no port |
| market-data-collector | scheduled and continuous acquisition | no published port |
| frontend | legacy operator UI served by Nginx | `127.0.0.1:5173` |
| frontend-v2 | V2 operator console served by Nginx | `127.0.0.1:5174` |
| pgAdmin | database operations | `127.0.0.1:8080` |
| Loki and Alloy | durable logs and Docker log ingress | Loki private; Alloy UI `127.0.0.1:12345` |
| Docker event and capacity samplers | lifecycle and storage evidence | no published port |
| Grafana | provisioned dashboards, alert rules, and optional email routing | `127.0.0.1:3000` |
| IBKR Gateway | optional paper/live broker transport and VNC | `broker` profile; loopback only |

All published ports are loopback-only by default. Use SSH forwarding. Do not
expose the V1 operator surfaces with a public firewall rule or reverse proxy.

The server preset gives the `tsdb` container an isolated 1 GiB `/dev/shm`
allocation. PostgreSQL parallel workers use this container-local filesystem for
dynamic shared-memory segments; the explicit size avoids relying on Docker's
small default. It does not change `shared_buffers`, durable database storage,
query admission, or the schema, and it is not a substitute for bounding
expensive status queries.

A pull-request merge does not change the running server. This capacity setting
becomes active only after an operator runs `deploy` for a reviewed commit and
Compose recreates `tsdb`. The recreate briefly restarts PostgreSQL but preserves
the `postgres-data` named volume. The deployment helper does not run a migration
before, during, or after that recreate; any separately required historical
schema transition remains an explicit operator-run procedure.

For the Level 2 bounded-status change, use this future rollout order after its
pull request is reviewed; merging alone performs none of these steps:

1. Take a database backup and prove it can be restored.
2. Stop the backend and collector writers, then wait for every stream lease to
   expire.
3. Against the still-running old database release, run
   `scripts/db/manual_migration_book_operational_rollups_v1.sql`. The seed is
   single-worker and disables both parallel query and parallel index-maintenance
   workers so the current 64 MiB shared-memory allocation is sufficient. It
   repairs and verifies the two exact bounded-read indexes and installs the
   always-on checkpoint counter trigger. Its database lock wait is bounded at
   30 seconds; a lock-timeout error means a writer is still active and must be
   investigated instead of retrying blindly.
4. Deploy the reviewed release. Compose recreates `tsdb`, preserves the named
   data volume, and activates the 1 GiB shared-memory allocation.
5. Reconcile every seeded rollup against canonical facts and checkpoints, run
   eight concurrent market-structure status reads, and then verify database,
   backend, and collector health before declaring the rollout complete.

If the migration, reconciliation, concurrency proof, or health gate fails,
stop and retain the failure evidence for RCA. Do not start the new application
code against an unseeded store.

Rolling back from this release to code that predates the bounded counters is
allowed only as an explicit compatibility window. The old code can write new
book Facts without advancing their rollup; the database trigger continues to
count checkpoint inserts while the migration remains installed. Before
promoting the new release again, stop writers and unconditionally rerun the
absolute seed migration. Startup and status verify the trigger plus bounded
Fact/checkpoint markers, but those guards do not replace the mandatory reseed;
do not assume a later collector cycle will repair an old-code interval.

The backend controls Docker to launch isolated bot runtimes, so its Docker
socket mount is a privileged trust boundary. Alloy and the Docker evidence
samplers use read-only socket mounts. Treat the backend image, API access, and
deployment account accordingly.

## Clean Database And Optional Legacy Upgrades

A new Quant-Trad database initializes directly at the current schema. The first
process to acquire the schema advisory lock:

1. proves no Quant-Trad model relation exists;
2. creates every current model-owned and canonical Fact relation;
3. seeds the exact code-owned Fact schema registry;
4. installs current validation and immutability functions, triggers, indexes,
   and Timescale layout; and
5. validates and commits the complete contract atomically.

Historical upgrade artifacts are not clean-install input. They remain optional,
reviewed tools for an operator preserving an older database. That operator must
back up and restore-test first, stop all writers, identify the exact starting
contract, and run only its reviewed upgrade sequence. The simpler alternative
is to export evidence worth retaining and initialize a new empty database.

Startup never guesses an upgrade sequence or executes historical migration
scripts. It may create compatible model-owned tables and indexes under the
existing bootstrap contract, but migration-owned, legacy, or incompatible
schema drift fails loudly with specific operator guidance.

## One-Time Ubuntu Host Bootstrap

The only privileged step installs Docker Engine and Compose, enables Docker at
boot, grants the deployment account Docker access, and creates stable storage
directories. Run it in your own SSH terminal so the sudo password is entered
only there:

```bash
bash scripts/automation/server_host_bootstrap.sh
```

The default root is `/srv/quanttrad`. A different on-prem or VPS layout may set
`QT_SINGLE_NODE_ROOT` when running the bootstrap. It creates:

```text
/srv/quanttrad/app
/srv/quanttrad/market-structure
/srv/quanttrad/deploy-state
/srv/quanttrad/backups
```

Log out and reconnect once so Docker-group access applies. No later release
operation requires sudo.

Keep the market-structure path stable. It may begin on NVMe and later become an
HDD mount without changing container paths or stored object keys. PostgreSQL
stays in a Docker named volume; database backups can move to separate storage.

Clone the repository into the application directory, then generate the private
operator environment:

```bash
bash scripts/automation/server_deploy.sh init-env
```

This creates `/srv/quanttrad/secrets.env` beside the `app/` checkout, exactly
once with mode `0600`, strong URL-safe database and UI passwords, and a valid
provider-credential encryption key. Keeping it outside Git makes exact-SHA
updates and rollback independent of ignored working-tree files. The command
refuses to overwrite an existing file. Review or change non-secret settings as
needed. Trading and market-data provider credentials remain in the encrypted
application credential store. The managed email relay's send-only credential is
an installation secret and does belong in this private file when alerting is
enabled.

## Validate And Deploy

Before every first deploy, update, or rollback:

```bash
bash scripts/automation/server_deploy.sh doctor
```

Promote an exact reviewed SHA whenever possible:

```bash
bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>
```

Using `origin/develop` is supported, but the helper resolves it once and records
the resulting SHA. It requires a clean checkout, fetches the requested ref,
renders Compose, pulls pinned third-party images, builds release images, waits
for health, verifies embedded source attestations, checks definition enrollment,
and writes release state outside the repository.

Operator email is optional and disabled by default. A managed transactional
relay owns outbound delivery; Grafana owns alert state, grouping, routing, and
resolved notifications. Configure the private environment once, then routine
recipient changes only edit the comma-separated `QT_ALERT_EMAILS` value:

```bash
bash scripts/automation/server_deploy.sh validate-alerts
bash scripts/automation/server_deploy.sh apply-alerts
```

`apply-alerts` requires the clean checkout to match the recorded deployed
revision and force-recreates only Grafana without starting its dependencies.
When `QT_ALERTS_ENABLED=true`, validation requires the managed relay host,
credential, verified sender, and at least one valid recipient. The deployment
helper adds `docker/docker-compose.alert-email.yml`; when false, the overlay,
SMTP settings, contact point, and root email policy are absent. Provider
secrets are never printed. Follow the
[operator email alerting runbook](../operators/alerting.md) for setup, testing,
rule standards, recipient changes, blind spots, and rollback.

Before merge, an operator can test the exact candidate SHA from a detached
worktree without changing the recorded production release:

```bash
bash scripts/automation/server_deploy.sh validate-alerts
QT_ALERT_PREVIEW_BASE_ROOT=/srv/quanttrad/app \
  bash scripts/automation/server_deploy.sh preview-alerts
```

The preview recreates only Grafana and records enough state to restore it from
the production checkout. Full deploys and routine alert applies are refused
while that preview marker exists. After the real-provider contact-point test,
restore before removing the candidate worktree:

```bash
bash scripts/automation/server_deploy.sh restore-alerts
```

If the production revision predates native email alerting, restoration runs an
explicit Grafana provisioning cleanup before recreating the production
configuration. See the alerting runbook for the complete worktree, verification,
and cleanup sequence.

Useful operations are:

```bash
bash scripts/automation/server_deploy.sh release
bash scripts/automation/server_deploy.sh status
bash scripts/automation/server_deploy.sh fleet
bash scripts/automation/server_deploy.sh qt <qt-arguments...>
bash scripts/automation/server_deploy.sh logs market-data-collector
bash scripts/automation/server_deploy.sh validate-alerts
bash scripts/automation/server_deploy.sh apply-alerts
bash scripts/automation/server_deploy.sh preview-alerts
bash scripts/automation/server_deploy.sh restore-alerts
bash scripts/automation/server_deploy.sh stop
```

Rollback uses the same verified promotion path:

```bash
bash scripts/automation/server_deploy.sh rollback
```

Without an argument it promotes the previously successful revision. An explicit
compatible SHA may be supplied. Do not roll application code across an
incompatible persisted contract; restore a verified backup or rebuild a clean
database instead.

## Provider Credentials And Immediate Collection

The first release enrolls and starts the reviewed market-data fleet by default:

- scheduled open-interest and funding collectors for the three reviewed
  futures products;
- continuous trade collectors for those products; and
- continuous L2 collectors for those products; and
- the hourly Chainlink nxtAssets BTC ETP proof-of-reserves collector from its
  reviewed structured Fact manifest.

Enrollment owns initial state only. Reapplying a release updates reviewed
configuration without undoing a later audited stop, pause, or safety halt.

The default `market_trades`, `level2`, and `heartbeats` subscriptions are
public Coinbase channels, so a clean single-node deployment starts those six
continuous streams without credentials. Authentication remains an explicit
per-enrollment option; the transport creates JWTs only for definitions whose
reviewed manifest selects `auth_mode: authenticated`.

When authenticated subscriptions or account-scoped provider workflows are
needed, load Coinbase credentials once through the encrypted provider
credential store. Coinbase recommends Ed25519 keys, and the deployment lock
supports raw 32-byte or 64-byte base64 Ed25519 private-key material as issued
by the CDP portal:

```bash
bash scripts/automation/server_deploy.sh credentials-coinbase
```

The command is interactive and does not echo credentials into release state or
the operator environment file. To import the downloaded CDP JSON directly from
a trusted client without copying it onto the server or transforming it with
`jq`, pipe it over SSH. Set the two client-local values for the installation:

```bash
QT_SERVER_HOST=your-server-host
CDP_KEY_FILE=/path/to/cdp_api_key.json
ssh "$QT_SERVER_HOST" \
  'cd /srv/quanttrad/app && bash scripts/automation/server_deploy.sh credentials-coinbase --cdp-key-file - --no-input' \
  < "$CDP_KEY_FILE"
```

The importer accepts Coinbase's `name` or `id` key identifier and its
`privateKey`, proves locally that the material can sign a WebSocket JWT, and
reports only `EdDSA` or `ES256`. It never returns the key or signed token.
Credential-store validation proves encrypted storage and required-field
presence; use a bounded authenticated stream smoke check when provider-side
acceptance must also be proven. Public definitions remain eligible whether or
not a credential is stored.

The default Chainlink binding reads a public Arbitrum JSON-RPC endpoint from
`CHAINLINK_ARBITRUM_RPC_URL`; `init-env` supplies the public mainnet endpoint and
an operator may replace it with another compatible endpoint. It does not use
the Coinbase credential store.

`QT_SINGLE_NODE_STRUCTURED_FACT_MANIFESTS` is an OS-path-separated list of
checked-in manifests. The initializer enrolls every manifest and binding marked
enabled; `QT_SINGLE_NODE_ENABLE_STRUCTURED_FACTS=false` disables that group.
This keeps provider selection in reviewed manifests while the worker continues
to use the shared scheduled-collector lifecycle.

## Additional Products Without A Release

The three-product Coinbase fleet is the clean-install baseline, not a runtime
symbol whitelist. Once a release contains the Coinbase futures collector pack,
an operator can enroll another validated product without rebuilding images or
restarting the stack:

```bash
bash scripts/automation/server_deploy.sh qt \
  data collector-definitions enroll-product \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --product-id LNP-20DEC30-CDE \
  --actor-id <operator-id> \
  --reason "Add LINK market-data coverage" \
  --confirm
```

Coinbase's [public product page](https://www.coinbase.com/futures/lnp-20dec30-cde/)
and product API identify `LNP-20DEC30-CDE` as a future with one-contract
quantity increments, a `0.001` price increment, and 50 LINK per contract.
Enrollment refreshes that metadata from Coinbase instead of trusting those
prose values, creates the canonical instrument, and installs the existing
scheduled OI/funding and continuous trade/L2 definitions. The worker discovers
them from durable configuration.

The command is idempotent. Initial enrollment starts the definitions, while a
later reapplication does not override audited lifecycle intent. Use repeated
`--collector` arguments to select only part of the registered pack. A new
provider, transport, channel, projection, Fact contract, or recovery behavior
still requires code, CI, and a release.

See the [operator handbook](../operators/README.md) for the preflight,
verification, backup, and multi-node boundaries.

## Collector Architecture And Soak

L2 is not a special lifecycle or daemon. Scheduled collectors use the shared
scheduler contract. Every continuous collector uses the same supervisor and
provider-neutral runtime for registration, fenced ownership, connection
lifecycle, fsynced WAL, bounded queues, archive publication, reconnect, graceful
stop, recovery, and gap evidence.

Provider transport and domain projection are separate adapters. Coinbase
WebSocket/JWT behavior belongs to its transport adapter. Each projection creates
its own epoch analyzer: trade translation belongs to the trade projection, while
book reconstruction, validity, and checkpoints belong to the L2 projection. A
future provider or channel registers adapters;
it does not add a provider/channel switch to the runtime or a whitelist to
persistence.

The single-node preset is intentionally opinionated about its initial fleet,
not about collector ownership: it enrolls the reviewed Coinbase OI, funding,
trade, and L2 definitions plus every enabled binding in the configured
structured Fact manifest list. The enrollment controls default to enabled; the
L2 control is not a canary gate. Another deployment can select different
reviewed stream and structured Fact manifests while retaining the same
scheduler, continuous supervisor, storage, health, and release machinery.

L2 has additional projection evidence because an order book is stateful: after
a discontinuity it must restore a verified checkpoint plus durable deltas or
wait for a fresh provider snapshot. That is correctness for the L2 data product,
not different collector ownership.

Collection starts so the host can produce evidence. During the initial soak,
inspect rather than pre-emptively disabling L2:

1. confirm the worker heartbeat and every enabled definition in `fleet`;
2. confirm scheduled attempts and canonical OI, funding, and Chainlink reserve
   Fact growth, including explicit stale/error gap evidence;
3. confirm continuous leases, raw manifests, trades, book checkpoints, validity
   intervals, derived features, and coverage revisions;
4. measure spool, archive, PostgreSQL, Docker, CPU, and memory growth;
5. restart the collector container and prove lease fencing, spool recovery,
   checkpoint reconciliation, a fresh post-gap snapshot, and no false coverage;
6. retain one-hour and 24-hour evidence as the initial capacity baseline.

Safety policies continue collection through warning thresholds and fail closed
at critical disk/spool thresholds. On the reference server, the configured
reserves are small relative to the available NVMe; projected exhaustion and
actual growth remain the meaningful soak signals.

## Private Operator Access

Forward the main operator surfaces from a trusted client:

```bash
QT_SERVER_HOST=your-server-host
ssh \
  -L 5174:127.0.0.1:5174 \
  -L 5173:127.0.0.1:5173 \
  -L 8000:127.0.0.1:8000 \
  -L 3000:127.0.0.1:3000 \
  -L 8080:127.0.0.1:8080 \
  "$QT_SERVER_HOST"
```

Frontend V2 is then at `http://127.0.0.1:5174`, Grafana at
`http://127.0.0.1:3000`, and pgAdmin at `http://127.0.0.1:8080`.

## Optional IBKR Gateway

IBKR remains outside the default because it adds broker credentials and trading
surfaces, not because it needs another deployment system. Keep paper mode and
read-only API during admission:

```bash
QT_SINGLE_NODE_PROFILES=broker \
  bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>
```

Never enable live broker mode as a side effect of an application release.

## Storage Move And Recovery

For a dedicated HDD, configure both values in the private operator environment:

```dotenv
QT_MARKET_DATA_ROOT=/mnt/quanttrad-archive/market-structure
QT_MARKET_DATA_EXPECTED_UUID=<filesystem-uuid-from-findmnt-or-blkid>
```

The second value is the filesystem UUID, not the drive serial or partition UUID.
An empty UUID preserves the initial directory-backed mode; it does not provide
missing-disk protection. Host admission reads `/run/udev/data`; the native Linux
host must run udev and expose current filesystem metadata there. Containers
receive a read-only metadata bind and need no block-device privileges. Do not
use a copied marker file as proof that the HDD is mounted.

Before deployment, run the non-mutating probe:

```bash
bash scripts/automation/server_deploy.sh validate-storage
```

It reports filesystem identity and capacity or fails with the exact path and
reason. `doctor` and `deploy` use the same probe; deploy repeats it after image
builds. All archive-writing process entrypoints recheck after container restart.
Compose refuses to create missing host bind directories. Never clear the UUID
or create a fallback directory just to bypass a failed check.

Mount the filesystem by UUID using the host's normal mount configuration and
ensure it is mounted before Docker starts these containers after reboot. Disk
formatting, fstab/systemd changes, and data moves are separate operator actions;
the deploy helper performs none of them. Do not combine an unverified archive
move with enabling destructive retention.

Schedule database backups outside application containers and restore-test them.
Loki retains seven days in its single-host filesystem store. Docker JSON logs
rotate independently so a failed log pipeline cannot consume the host without
bound. PostgreSQL emits to container stderr in this server preset, so Alloy
captures it through the same bounded path instead of accumulating a separate
unmanaged database-log volume.

Moving archives to an HDD is an explicit maintenance event:

1. identify the intended disk by model/serial and filesystem UUID, prepare and
   mount it, and keep PostgreSQL on its existing NVMe-backed named volume;
2. stop and drain the collector and backend writers, then verify no active
   stream lease remains;
3. copy and checksum the archive tree to the already-mounted HDD;
4. set the host root and expected UUID, then run `validate-storage` and `doctor`;
5. deploy the reviewed storage-aware release and restart the stack; and
6. verify existing archive reads/checksums, frozen dataset reads, worker health,
   new writes on the HDD, and reboot recovery before retiring the old copy.

Do not rewrite stored archive keys or bypass checksum verification during the
move.

If admission or verification fails, leave writers stopped and retain both
copies. A rollback to the old path requires checking its UUID and reconciling
all writes made after the move before restoring configuration; merely pointing
at an old copy would lose acknowledged objects. A code rollback while dedicated
storage is configured must retain the filesystem guard. Releases predating it
are not a safe automatic rollback target. Formatting, deleting either copy,
or clearing the expected UUID is not a recovery procedure.

### Canonical Retention Rollout Gate

The generalized retention executor is implemented but defaults off. Its current
whole-day dependency gate still excludes L2/derived families whose complete
closure is not yet admitted. Do not enable unattended retention until those
proofs and representative disposable stress checks are complete.

After that gate is cleared, use this separate, reviewed rollout order:

1. Finish and validate the HDD move above; retain database backups and a tested
   restore path. Do not combine disk cutover with deletion activation.
2. If the deployment still uses inline canonical JSON, stop/drain all writers
   and run the explicit
   [canonical storage-tier cutover](../architecture/data/GENERALIZED_FACT_DATA_PLANE.md#explicit-storage-cutover).
   Runtime startup must not migrate existing columns in place.
3. Review `market_data_lifecycle.canonical_retention` hot windows, byte targets,
   page/verification limits and run bounds against measured host capacity.
   Keep both lifecycle execution flags false while inspecting `qt data
   market-structure lifecycle-plan` and dry `lifecycle-run` output. Resolve all
   missing-mount, incomplete-proof, receipt and headroom blockers.
4. With automatic scheduling disabled for the maintenance window, explicitly
   enable the outer and canonical execution flags and run one bounded manual
   `lifecycle-run --execute`. Inspect outcomes, committed page/proof progress,
   physical bytes, frozen known-at reads, collector lag, and independent NVMe/HDD
   alerts. A plan or verified page alone is not deletion authority.
5. Only after repeated bounded runs and a restart/resume check are accepted,
   enable the recurring lifecycle schedule. Retain the configured filesystem
   identity and the fail-closed archive/pin checks.

Server Compose reads these operator settings without overriding them with
hard-coded values: `QT_MARKET_DATA_LIFECYCLE_ENABLED` (schedule, default true),
`QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED` (outer mutation gate, default false),
and `QT_MARKET_DATA_LIFECYCLE_CANONICAL_EXECUTION_ENABLED` (canonical mutation
gate, default false). Keeping the schedule off does not prohibit an explicitly
requested, otherwise enabled manual run. Setting a hot window alone never
enables mutation.

This release starts honoring outer flag values that the earlier server preset
hard-coded. Before deploying it, inspect the private operator file and rendered
Compose configuration and explicitly set **both execution flags false** for the
cutover. Do not assume an old, previously ignored `true` value is harmless.

Rollback begins by disabling canonical execution and stopping/draining the
lifecycle worker. Already committed cold movement is not undone by that flag:
retain every acknowledged archive object, database catalog and dependency hold.
Keep a release that can read both hot and cold payloads. Do not roll back to an
inline-only reader after reclaiming a hot partition, drop the cold catalogs, or
point readers at an older archive copy. Restoration to the old layout requires
an explicit offline restore/rehydration plan and verification, not a config flip.
