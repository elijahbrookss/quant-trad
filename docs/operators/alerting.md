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
- the existing collector safety rules plus a two-minute database-unavailable
  rule.

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
Grafana-only apply. Application users do not need provider accounts or email
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

### Configure it from a Windows laptop

The configured SSH alias lives in Ubuntu WSL. Start an interactive server shell
from PowerShell:

```powershell
wsl -d Ubuntu -- ssh qt-server
```

Edit the private file on the server rather than putting the credential in a
PowerShell command, shell history, Git, or chat:

```bash
cd /srv/quanttrad
nano secrets.env
chmod 600 secrets.env
```

In `nano`, save with `Ctrl+O`, press `Enter`, then exit with `Ctrl+X`. Validate
from the deployed checkout, or from the detached candidate worktree during a
pre-merge proof:

```bash
cd /srv/quanttrad/app
bash scripts/automation/server_deploy.sh validate-alerts
```

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
before it can be tested. Use a detached worktree at the exact candidate commit:

```bash
git -C /srv/quanttrad/app fetch origin feat/grafana-email-alerting
git -C /srv/quanttrad/app worktree add \
  --detach /srv/quanttrad/alert-preview \
  origin/feat/grafana-email-alerting
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
recorded deployment. It force-recreates only Grafana with `--no-deps`; it does
not restart the database, backend, frontends, Alloy, or collectors.

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
action. Do not alert on raw disconnect counts, individual retry log lines, or
event-table row volume. Alert on sustained coverage loss, stale valid data,
exhaustion risk, an active safety halt, or another state with a concrete action.

The initial rules are intentionally small:

- database query unavailable for two minutes: critical;
- collector safety latch active: critical;
- collector storage safety warning observed: warning.

A future rule belongs in the reviewed provisioning directory, not only in the
Grafana UI. UI edits to provisioned resources are not the durable source.

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
