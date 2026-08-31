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
managed provider. Store the following in the private `secrets.env` beside the
server checkout, never in Git or chat:

```dotenv
QT_ALERTS_ENABLED=true
QT_ALERT_EMAILS=owner@example.com,backup@example.com
QT_ALERT_SMTP_HOST=smtp.provider.example:587
QT_ALERT_SMTP_USER=provider-user-or-token
QT_ALERT_SMTP_PASSWORD=provider-secret
QT_ALERT_EMAIL_FROM=alerts@example.com
QT_ALERT_EMAIL_FROM_NAME=Quant-Trad Alerts
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

## Deploy and prove delivery

Alert rules and routing structure are reviewed release material. Promote their
commit through the normal release path:

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

The repository's automated transport proof is:

```bash
bash scripts/ci/test_grafana_email_alerting.sh
```

It uses a disposable Grafana and capture-only Mailpit container. It never sends
mail to the internet and is not a production relay.

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
