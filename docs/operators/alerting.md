# Operator Email Alerting

Quant-Trad uses Grafana's native alerting system. Application code produces
signals; Grafana evaluates rules, keeps alert state, groups notifications, and
routes email through one installation-owned contact point. Quant-Trad does not
implement a second alert dispatcher, email queue, or incident platform.

The concrete flow is:

```text
logs / database / metrics
          |
          v
Normal -> Pending -> Firing -> Resolved
          |
          v
notification policy -> qt-operator-email -> managed relay -> recipients
```

The formal primitive is an Alertmanager-style stateful alert lifecycle. A log
line or database row is an event. A rule turns sustained bad evidence into an
alert state. A notification is one delivery attempt for that state. An incident
is the operator's response when the impact warrants one. Keeping those apart
prevents reconnect rows or repeated errors from becoming one page per event.

## V1 boundary

This slice provides:

- one optional, provisioned Grafana email contact point;
- one root notification policy for every reviewed alert rule;
- one or more comma-separated recipients in `QT_ALERT_EMAILS`;
- firing and resolved email notifications;
- 30-second grouping, five-minute regrouping, and four-hour repeats;
- strict configuration validation without printing provider secrets;
- a CI proof that a real Grafana rule reaches two captured recipients;
- the existing collector safety rules, a sustained per-stream reconnect-storm
  rule, a two-minute database-unavailable rule, and native-server storage rules.

### Storage rules

`Storage safety` tracks `docker-engine-storage` (the Docker/PostgreSQL disk)
and `market-archive-storage` (the configured archive disk) separately. The
capacity dashboard labels both resources; directory-mode installations may
initially put both on the same physical disk.

- 70% to below 85%: warning after five minutes.
- 85% to below 92%: critical after two minutes.
- 92% or above: emergency after one minute.
- Missing or failed physical filesystem evidence: critical per resource.

Usage rules use the peak of the last two minutes. Their severity bands do not
overlap, so an escalation does not create three simultaneous usage alerts.
Availability rules use that same two-minute window and a two-minute hold:
missing telemetry is not zero usage or a healthy disk. Query failures alert too.
Warnings apply to available application capacity, excluding filesystem-reserved
space. Check free bytes and growth as well as percentages.

Use `docker-stats` logs filtered to `capacity_unavailable` to diagnose missing
mounts, UUID mismatches, read-only filesystems, Docker-root mismatch, or probe
failure. Never clear the expected UUID or create a fallback directory to make
an alert disappear. At critical/emergency pressure, inspect archival backlog
and a retention dry run. Delete only through the verified retention contract;
if safe headroom cannot be restored, explicitly stop collectors. Alerts do not
automatically stop ingestion, migrate data, or delete anything.

The isolated validation command is:

```bash
python3 scripts/ci/test_storage_alerts.py
```

It requires local Docker, creates only a uniquely named disposable Loki/Grafana
project with loopback ports and SMTP disabled, loads the production storage
rules, verifies their queries/math, and removes only its own containers/volumes.

It does not provide SMS, escalation schedules, acknowledgements, an incident
record, or an external monitor for the host itself. If the host, Grafana, its
network path, or the managed relay is unavailable, this installation cannot
email about its own failure. Add one external heartbeat only when that blind
spot is important enough to operate.

## What an installation owner supplies

The repository owns the rules, routing shape, validation, and deployment
workflow. Each installation owner supplies only the identity and authorization
needed to send mail:

| Input | Why it exists | Where it lives |
| --- | --- | --- |
| Managed email-provider account | Operates delivery, TLS, reputation, and retries | Provider account |
| Authorized sender address or domain | Prevents arbitrary sender impersonation | Provider account plus `QT_ALERT_EMAIL_FROM` |
| One send-only credential | Authorizes this installation to send | Private `QT_ALERT_SMTP_PASSWORD` |
| One or more recipients | Defines who owns this installation's alerts | Private `QT_ALERT_EMAILS` |
| Grafana administrator access | Runs the contact-point test and inspects rule state | Existing installation access |

There are two different setup costs. The installation owner performs the
provider and sender setup once. After that, adding or removing Quant-Trad users
is only a comma-separated `QT_ALERT_EMAILS` edit, followed by validation and a
monitoring-only apply. Application users do not need provider accounts or email
credentials.

## One-time installation setup

Grafana OSS needs an outbound email transport. The supported choice is a
managed transactional-email relay: the provider operates the server, TLS,
delivery reputation, and retries; the Quant-Trad installation holds one scoped
credential. Do not use a personal Gmail password, disable account security, or
operate an SMTP server for this feature.

There cannot be safe global email delivery with only a recipient address:
someone must authorize and pay for the sender. That is a one-time installation
concern. After it is configured, adding or removing people changes only
`QT_ALERT_EMAILS`.

Create a sender/domain and a send-only relay credential with the selected
managed provider. A provider-supplied test sender is sufficient for a first
delivery proof when the provider restricts it to the account owner's inbox; a
verified project domain is needed before routing to arbitrary users. Store the
following in the private `secrets.env` beside the server checkout, never in Git
or chat:

```dotenv
QT_ALERTS_ENABLED=true
QT_ALERT_EMAILS=owner@example.com,backup@example.com
QT_ALERT_SMTP_HOST=smtp.provider.example:587
QT_ALERT_SMTP_USER=provider-user-or-token
QT_ALERT_SMTP_PASSWORD=provider-secret
QT_ALERT_EMAIL_FROM=alerts@example.com
QT_ALERT_EMAIL_FROM_NAME=Quant-Trad Alerts
```

### Resend reference path

Resend is the reference managed provider for the first Quant-Trad installation.
It is not a hard dependency: another authenticated STARTTLS relay can supply the
same `QT_ALERT_SMTP_*` contract.

1. Create the installation's Resend account.
2. Create a send-only API key; Grafana uses that value as the relay password.
3. For an initial proof to the same address that owns the Resend account, use
   Resend's `onboarding@resend.dev` test sender. It cannot send to arbitrary
   recipients.
4. Before adding other owners, verify a project domain in Resend and replace the
   sender with an address on that domain.

The corresponding first-proof values are:

```dotenv
QT_ALERTS_ENABLED=true
QT_ALERT_EMAILS=account-owner@example.com
QT_ALERT_SMTP_HOST=smtp.resend.com:587
QT_ALERT_SMTP_USER=resend
QT_ALERT_SMTP_PASSWORD=<private-resend-api-key>
QT_ALERT_EMAIL_FROM=onboarding@resend.dev
QT_ALERT_EMAIL_FROM_NAME=Quant-Trad Alerts
```

Provider instructions:

- [Send with Resend SMTP](https://resend.com/docs/send-with-smtp)
- [Resend test-sender restriction](https://resend.com/docs/knowledge-base/403-error-resend-dev-domain)

Use the installation's private infrastructure runbook to reach the host and edit
its protected `secrets.env`. Do not put the credential in a command, shell
history, Git, or chat. Quant-Trad deliberately does not prescribe a workstation,
SSH alias, host name, or secret-editing tool.

The production transport is locked to mandatory STARTTLS with certificate
verification. Unencrypted SMTP exists only inside the isolated capture test. Keep
`secrets.env` mode `0600` and include it in the installation's secure backup and
credential-rotation procedure.

Validate without starting or restarting containers:

```bash
bash scripts/automation/server_deploy.sh validate-alerts
```

Validation requires every delivery field only when `QT_ALERTS_ENABLED=true`,
rejects malformed or duplicate recipients, validates the relay host/port, and
never prints the password.

## Prove the exact change before merge

An alerting change does not need to become the recorded production release
before it can be tested. Use a detached worktree at the exact candidate commit.
Set `CANDIDATE_REF` to the branch or commit being reviewed:

```bash
CANDIDATE_REF=feature/example-alert-change
git -C /srv/quanttrad/app fetch origin "$CANDIDATE_REF"
CANDIDATE_SHA="$(git -C /srv/quanttrad/app rev-parse FETCH_HEAD)"
git -C /srv/quanttrad/app worktree add \
  --detach /srv/quanttrad/alert-preview \
  "$CANDIDATE_SHA"
cd /srv/quanttrad/alert-preview
bash scripts/automation/server_deploy.sh validate-alerts
QT_ALERT_PREVIEW_BASE_ROOT=/srv/quanttrad/app \
  bash scripts/automation/server_deploy.sh preview-alerts
```

`preview-alerts` records the candidate SHA and the existing production SHA,
then force-recreates only Grafana. It does not rebuild or restart the database,
backend, frontends, Alloy, Loki, or collectors. The production release record
does not change, and normal apply/deploy commands fail closed until the preview
is restored.

While the preview is active:

1. Confirm the candidate SHA printed by the command matches the PR head.
2. Confirm `qt-operator-email`, the root notification policy, and the reviewed
   rules are provisioned in Grafana.
3. Use the contact-point **Test** action and confirm delivery to every address.
4. Confirm the database rule evaluates `Normal`; do not stop the production
   database merely to manufacture a firing alert.
5. Check Grafana logs and the provider delivery record for errors.

The disposable integration proof separately exercises a real firing rule and
two-recipient routing without touching production:

```bash
bash scripts/ci/test_grafana_email_alerting.sh
```

Restore before merging or deploying anything else:

```bash
cd /srv/quanttrad/alert-preview
bash scripts/automation/server_deploy.sh restore-alerts
cd /srv/quanttrad/app
git worktree remove /srv/quanttrad/alert-preview
```

Restoration first provisions explicit deletions for preview-only rules and the
email contact point, resets the policy tree when the production revision did
not yet contain this feature, and then recreates Grafana from the recorded
production checkout. This matters because removing a provisioning file alone
does not remove resources already stored in Grafana's database.

## Deploy after acceptance

Alert rules and routing structure are reviewed release material. Promote their
accepted commit through the normal release path:

```bash
bash scripts/automation/server_deploy.sh doctor
bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>
bash scripts/automation/server_deploy.sh status
```

For first-time enablement or later recipient/provider changes on that already
deployed revision, apply only the private configuration to Grafana:

```bash
bash scripts/automation/server_deploy.sh validate-alerts
bash scripts/automation/server_deploy.sh apply-alerts
```

`apply-alerts` refuses a dirty checkout or a revision different from the
recorded deployment. It force-recreates Alloy and Grafana with `--no-deps`; it does
not restart the database, backend, frontends or collectors.

Then open Grafana through the private SSH tunnel and check **Alerting**:

1. `qt-operator-email` is the contact point.
2. The default notification policy routes to `qt-operator-email`.
3. The provisioned rules appear under the **Quant Trad** folder.
4. Use Grafana's contact-point **Test** action and confirm the message arrives
   at every address in `QT_ALERT_EMAILS`.
5. Confirm the received sender, subject, TLS/provider delivery record, and
   resolved-message behavior.

The automated transport proof uses a disposable Grafana and capture-only
Mailpit container. It never sends mail to the internet and is not a production
relay. The pre-merge contact-point test is what proves the managed provider and
real inbox boundary.

## Change recipients

Edit only the private recipient value for routine ownership changes:

```dotenv
QT_ALERT_EMAILS=first@example.com,second@example.com
```

Run `validate-alerts`, run `apply-alerts` from the currently deployed clean
checkout, and use the Grafana test
action. Removing the last recipient requires setting `QT_ALERTS_ENABLED=false`;
a blank enabled list is rejected.

All V1 recipients receive the same installation-wide alert set. Per-team
routes become useful only when there are genuinely different owners or
escalation policies; until then they are machinery that exists to support
itself.

## Rule standard

A reviewed rule must answer these questions before it is enabled:

1. What bad state, not merely what event, does it represent?
2. How long must that state persist before anyone should act?
3. Who owns it, and what is the first action?
4. What evidence proves recovery?
5. What should happen on query error or missing data?
6. How will the firing and resolved paths be tested?

Every provisioned rule must therefore include:

- a stable data source and bounded query;
- an explicit condition and `for` duration;
- bounded `severity`, `owner`, and `component` labels;
- `summary`, `first_action`, and `recovery` annotations;
- explicit `noDataState` and `execErrState` choices;
- a test that proves its expected state transition.

Use `warning` when timely investigation prevents impact and `critical` when the
service is unavailable, correctness is threatened, or a safety latch requires
action. Do not alert on lifetime disconnect totals, individual retry log lines,
or undifferentiated event-table row volume. A bounded count of distinct
connection failures can represent a bad state when it is evaluated per stream,
has a sustained threshold, and has a concrete operator action. Prefer direct
signals such as sustained coverage loss, stale valid data, exhaustion risk, or
an active safety halt whenever those signals capture the failure sooner.

The initial rules are intentionally small:

- database query unavailable for two minutes: critical;
- collector safety latch active: critical;
- collector storage safety warning observed: warning;
- at least six unique disconnect epochs for one stream within 15 minutes,
  sustained for five minutes: warning.

A future rule belongs in the reviewed provisioning directory, not only in the
Grafana UI. UI edits to provisioned resources are not the durable source.

### Collector reconnect-storm response

`qt-collector-reconnect-storm` evaluates the canonical
`market.stream_session_events` history. It counts distinct
`(session_id, connection_epoch)` pairs with a `provider_disconnected` event,
grouped by `definition_id`. Grafana creates one alert instance per affected
stream, while the notification policy groups instances from the same rule and
severity into one email.

On a firing alert:

1. Inspect the recent disconnect reasons for the affected stream.
2. Check valid-data freshness and accepted-record throughput; a healthy current
   snapshot does not erase reconnect churn.
3. Check collector CPU and the cost of any synchronous work in the receive
   path, including spool traversal.
4. Restart only if continuity is currently lost and a restart is the safest
   containment. A restart erases useful live evidence and is not the default
   first action.

Treat the alert as recovered only after the stream remains below six unique
disconnect epochs per rolling 15-minute window for at least five minutes and
continues producing fresh, valid data. A resolved email means the rule recovered;
it does not by itself prove the underlying defect was corrected.

## History and retention

No new event store is needed for this slice. Source logs already flow through
Docker stdout/stderr and Alloy into Loki, with the server's bounded seven-day
retention. Canonical collector safety events remain in PostgreSQL. Grafana's
alerting state and notification configuration live in the existing
`grafana-data` volume.

That is enough for current investigation and later dashboards, but it is not a
claim that notification history is canonical or retained forever. Add a
durable incident/event model only when a real query, reporting, or audit need
outlives those existing stores.

## Disable or roll back

Set `QT_ALERTS_ENABLED=false`, run `validate-alerts`, and run `apply-alerts`.
The email overlay is then absent, Grafana receives no SMTP settings, and the
email contact point/policy file is not mounted. Alert rules may still
evaluate in Grafana, but this installation has no external email route.

A code rollback uses the normal deployment rollback command. Do not delete the
Grafana volume or broad-restart the stack to change alerting.

## Optional native-host clock-health alert

`qt-host-clock-unhealthy` uses the existing Alloy -> Loki -> Grafana route and
existing operator-email contact point. It does not add recipients, credentials,
a telemetry database, a research procedure, or an admission gate. Historical
research results, evidence and timestamps remain unchanged.

The host-native `scripts/reporting/server_clock_health.py` performs only
`/usr/bin/chronyc tracking`, `activity`, and `-n sources`, with a three-second
limit each. The installed timer samples every 30 seconds. A selected reachable
remote source and at least one online source are required. Invalid/missing
status, unsynchronised leap status, a reference more than 1 second in the
future or older than 1,800 seconds, or uncertainty above 0.100 seconds emits
`healthy=0` with bounded reason codes. The conditional uncertainty estimate is
`abs(system time offset) + root delay / 2 + root dispersion`; it assumes the
upstream clock is correct. These are operational warning thresholds, not a
scientific timing tolerance or proof of historical clock accuracy.

The probe exports no source addresses or raw command output. It appends to
`clock-health.jsonl`, rotating at 1 MiB to one retained `.1` file, in a directory
owned by the sampling user and not writable by others. Symlink/non-regular or
multiply linked output files are refused. Alloy reads only that inert directory
through a read-only bind; it has no chrony control socket or private log mount.
Alloy validates and retains the sample's UTC timestamp. Missing, malformed,
non-string or unparseable timestamps are discarded before timestamp assignment;
replayed valid files retain their original age and cannot appear fresh.

The rule uses the worst health sample in a two-minute window and a two-minute
hold. Recovery requires the bad samples to leave that window. Stopped sampling
becomes no data after the window expires; missing or malformed evidence and
query errors alert, rather than imply health. Detection therefore normally takes
roughly two to five minutes including the one-minute evaluation cadence. Existing
notification grouping and delivery add their own delay. The same-host monitor
cannot independently diagnose its own host/network outage or a clock sufficiently
wrong to disrupt Loki ingestion; missing-data alerting provides bounded coverage
while Grafana remains operational.

### Activation requirements (not executed by implementation)

Activation is a separately coordinated monitoring change after review and
qualification of the exact source, including the disposable Alloy configuration
and Loki/Grafana rule proof in `scripts/ci/test_storage_alerts.py`. Do not copy candidate files into an older installed release or restart
application/collector services as a shortcut to monitoring activation.

A host owner must approve/install the two supplied systemd units and prepare
only the new non-secret export directory. With the reviewed source at
`/srv/quanttrad/app`, the installation commands are:

```sh
sudo install -d -o qtadmin -g qtadmin -m 0755 /srv/quanttrad/deploy-state/clock-health
sudo install -o root -g root -m 0644 scripts/reporting/qt-clock-health.service /etc/systemd/system/
sudo install -o root -g root -m 0644 scripts/reporting/qt-clock-health.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now qt-clock-health.timer
```

These commands require host-owner installation permission; ordinary read-only
chronyc status already works as `qtadmin`. No access to `/var/log/chrony`, group
membership changes, clock correction, source/network configuration, or changes
to chrony itself are required. The unit's Python/script paths and existing
`qtadmin` identity must be verified for the installation. An unhealthy probe
exits 1 after exporting its record; the timer continues sampling, and systemd
also makes this failure visible.

The narrowest supported activation uses the existing `apply-alerts` operation,
which now recreates only Alloy and Grafana. It does not restart the backend,
collectors, initialize job, database or frontends. No new execution command or
notification route is introduced. Keep all existing private credentials,
recipients, SMTP/TLS settings and source/network/chrony settings unchanged.

There are two phases, and neither was executed by this implementation:

1. Install the exact qualified combined release with `QT_CLOCK_HEALTH_ENABLED`
   absent or `false`, using the already established reviewed full-release runner.
   This is the normal release cutover and may restart application/collector
   services, so coordinate it separately with research ownership. The default
   Grafana mount explicitly deletes only `qt-host-clock-unhealthy`, installing
   a rollback baseline that can remove a previously persisted clock rule.
2. After the release is recorded, the installation owner prepares the export
   directory/timer as above and adds `QT_CLOCK_HEALTH_ENABLED=true` to the existing
   private deployment environment. This is a non-secret opt-in, not a credential
   rotation. From that same clean, recorded source, run the existing commands:

```sh
# PHASE 1: through the existing approved full-release runner and its recorded
# mount/UID/group/environment profile, with the clock flag disabled:
bash /srv/quanttrad/app/scripts/automation/server_deploy.sh deploy <QUALIFIED_FULL_COMBINED_COMMIT>

# PHASE 2: same established operator execution environment, after timer setup
# and explicit QT_CLOCK_HEALTH_ENABLED=true in the existing private env file:
bash /srv/quanttrad/app/scripts/automation/server_deploy.sh validate-alerts
bash /srv/quanttrad/app/scripts/automation/server_deploy.sh apply-alerts
bash /srv/quanttrad/app/scripts/automation/server_deploy.sh status
```

`<QUALIFIED_FULL_COMBINED_COMMIT>` must be replaced by the final integrated,
qualified revision; a work-branch handoff receipt is not develop deployment
admission. These are the existing runner's inner commands, not an instruction
to bypass its verified operator container/profile. Preparing or starting that
operator remains a separately authorized activation step. Do not run an ad-hoc
Compose command or copy candidate files into an older deployed release.

Enablement refuses an absent baseline or a recorded baseline that lacks the
clock-rule cleanup file. This makes pinned rollback to a clock-disabled baseline
safe: Grafana persists rule definitions in its database, so merely removing a
rule mount would leave an unwanted missing-data alert. Default/disabled source
provisions an exact `deleteRules` entry; the enabled overlay replaces that same
mount target with the live rule. Existing contact points, policies and unrelated
rules are untouched. Frozen recovery uses its pinned rendered configuration
without appending an overlay. An enabled candidate missing overlay support
fails closed; disabling the flag is an explicit operator decision.

The directory bind never creates missing host paths. `apply-alerts` requires
the clean source to match the recorded deployed revision. Before declaring the
alert active, verify timer cadence, fresh JSONL timestamps, Alloy ingestion, the
provisioned rule's healthy state, and existing notification routing. The
extended disposable proof exercises actual file -> Alloy -> Loki ingestion,
malformed/old timestamps, file rotation, and enable/disable/re-enable against
retained Grafana SQLite. It sends no real email and does not certify production
delivery. The existing disposable email proof separately exercises native
notification routing with synthetic recipients; production recipients do not
change.

For a planned disable, explicitly set the flag to `false` and run the same
`validate-alerts` and `apply-alerts` commands from the recorded release. This
removes the persisted clock rule and the Alloy directory mount while preserving
all other rules/routing. A host owner may then run
`sudo systemctl disable --now qt-clock-health.timer`. Stopping the timer alone
while the alert remains enabled deliberately becomes missing evidence.
