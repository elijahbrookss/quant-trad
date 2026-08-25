# Grafana Dashboard Provisioning

The checked-in JSON files in this directory are Quant-Trad's supported Grafana
dashboard source for local development and the native-Linux server.

## Provisioning Contract

[`dashboard.yml`](dashboard.yml) configures Grafana's file provider:

- the source directory is `/etc/grafana/provisioning/dashboards`;
- Grafana polls for file changes every 10 seconds;
- `allowUiUpdates` is `false`, so a provisioned dashboard cannot acquire a
  competing durable definition through the UI; and
- the repository directory is mounted read-only into Grafana in both supported
  Compose topologies.

Edit or replace the checked-in JSON, review the diff, and deliver it through the
normal source workflow. A dashboard displayed from Grafana's data volume or
created only in the UI is not a replacement for the reviewed file source.

There are no supported `make grafana-backup` or `make grafana-restore` targets.
The repository also provides no Grafana backup hook, cron entry, systemd timer,
or other automatic export/restore scheduler.

## Export A Candidate Backup

The tracked
[`scripts/backup-grafana-dashboards.sh`](../../../../scripts/backup-grafana-dashboards.sh)
script exports every dashboard returned by Grafana's dashboard-search API. It
extracts each bare dashboard model from the API wrapper and writes a slug-named
JSON file to `OUTPUT_DIR`.

Prerequisites:

- Bash, `curl`, and `jq`;
- a running, reachable Grafana API;
- valid Grafana credentials; and
- a writable output directory.

From the project root, set the actual credentials for the target Grafana and
run:

```bash
export GRAFANA_URL='http://localhost:3000'
export GRAFANA_USER='admin'
export GRAFANA_PASSWORD='<current-admin-password>'
export OUTPUT_DIR='./docker/grafana/provisioning/dashboards'
bash scripts/backup-grafana-dashboards.sh
```

Do not commit credentials. The script's `admin`/`admin` defaults are only
fallback values and should not be assumed to match a configured stack.

The script overwrites matching output filenames but does not delete stale JSON,
identify which dashboards are provisioned, validate repository intent, or prove
that its output can restore a loss. After export:

```bash
jq empty docker/grafana/provisioning/dashboards/*.json
git diff -- docker/grafana/provisioning/dashboards
```

Review additions, replacements, and stale files explicitly before committing.
An export is a candidate source update, not an automatic backup acceptance or
restore attestation.

## Reapply Checked-In Dashboards Locally

The restore source is the reviewed JSON in this directory. With the local
observability profile already running, Grafana should reconcile a changed file
within one 10-second polling interval. Verify the expected dashboard UID and
content in Grafana after the poll.

If the observability profile is stopped, start it with the normal stack command.
If the provider needs a bounded reload, recreate the local observability profile:

```bash
# Start the profile if it is stopped:
make STACK_PROFILES=observability stack-up

# Or recreate it when a bounded reload is needed:
make STACK_PROFILES=observability stack-restart
```

The local composition uses the supported Promtail shipper. Do not start Alloy
against the same application containers during a dashboard reload; dashboard
provisioning never requires a second log shipper.

## Reapply Checked-In Dashboards On A Native Server

Commit dashboard JSON through normal review and deploy the exact reviewed
revision with the supported server helper:

```bash
bash scripts/automation/server_deploy.sh doctor
bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>
```

The server composition mounts the reviewed provisioning directory read-only and
uses Alloy as its only normal Docker-to-Loki shipper. Do not copy dashboard files
directly into a running server container, save a competing UI version, or start
Promtail beside Alloy.

Local polling, stack restart, and reviewed-commit deployment reapply the
checked-in source. They are not evidence that a destructive dashboard-loss and
restore rehearsal passed. No such restore proof or automatic recovery workflow
is claimed here; verify the expected UIDs and content after each operator action.

For the complete native-server promotion boundary, see
[Portable Single-Node Deployment](../../../../docs/engineering/server-deployment.md).

## Authoring And File Format

Each JSON file is a bare Grafana dashboard model, not the API response wrapper.
Keep its `uid` stable when updating an existing dashboard. Author changes in the
file directly, or export a candidate model from Grafana and review it before
replacement. Because `allowUiUpdates` is false, UI-only edits to a provisioned
dashboard are not the supported authoring path.

## Recommended Entry Dashboards

Primary operator dashboards:

- `runtime-hotpath-control.json` (`uid=qt-runtime-hotpath-control`) — per-bar
  runtime attribution and worst-bar context;
- `botlens-transport-control.json` (`uid=qt-botlens-transport-control`) —
  bounded BotLens transport, payload, replay, and queue pressure; and
- `observability-cost-control.json` (`uid=qt-observability-cost-control`) —
  exporter write cost, rollup reduction, and database pressure.

Focused supporting dashboards:

- `botlens-diagnostics-failure-analysis.json` (`uid=qt-botlens-diagnostics`) —
  candle continuity and projection failure inspection; and
- `runtime-process-control-tower.json` (`uid=qt-runtime-control-tower`) —
  process/thread health outside BotLens backend observability.

The older playground-style BotLens overview, queue, pipeline, per-run,
attribution, and I/O/database dashboards were removed in favor of dashboards
that map directly to runtime hot-path, transport-budget, and observability-cost
questions.
