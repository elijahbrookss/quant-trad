# Grafana Dashboard Provisioning

This directory contains Grafana dashboards that are automatically loaded when Grafana starts.

## How It Works

- Dashboard JSON files in this directory are automatically provisioned to Grafana
- Changes to files are detected and applied within 10 seconds (see `updateIntervalSeconds` in `dashboard.yml`)
- You can edit dashboards in the Grafana UI (changes are allowed with `allowUiUpdates: true`)

## Backing Up Dashboards

### Manual Backup
```bash
# From project root
make grafana-backup
```

### Automatic Backup Options

#### 1. Git Pre-Commit Hook (Recommended)
```bash
# Install the hook (one-time setup)
ln -sf ../../scripts/git-hooks/pre-commit-grafana .git/hooks/pre-commit

# Now dashboards are backed up automatically before each commit
```

#### 2. Cron Job (Every Hour)
```bash
# Edit your crontab
crontab -e

# Add this line (adjust path to your project):
0 * * * * cd /home/elijah/dev/quant-trad && make grafana-backup >/dev/null 2>&1
```

#### 3. Systemd Timer (Linux)
```bash
# Create ~/.config/systemd/user/grafana-backup.service
[Unit]
Description=Backup Grafana Dashboards

[Service]
Type=oneshot
WorkingDirectory=/home/elijah/dev/quant-trad
ExecStart=/usr/bin/make grafana-backup

# Create ~/.config/systemd/user/grafana-backup.timer
[Unit]
Description=Backup Grafana Dashboards Hourly

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target

# Enable and start
systemctl --user enable --now grafana-backup.timer
```

## Restoring Dashboards

Dashboards are automatically loaded from this directory when Grafana starts.

To reload without restarting:
```bash
make grafana-restore
```

## Exporting from Grafana UI

1. Go to Dashboard → Settings → JSON Model
2. Copy the JSON
3. Save to a file: `docker/grafana/provisioning/dashboards/my-dashboard.json`
4. Commit to git

Or use the backup script to export all dashboards at once.

## File Format

Each JSON file should contain a Grafana dashboard model. The filename will be used as the slug.

Example: `system-metrics.json` → Dashboard available at `/d/<uid>/system-metrics`

## Recommended Entry Dashboard

For bot runtime process/thread health, start with:

- `runtime-process-control-tower.json` (`uid=qt-runtime-control-tower`)

This dashboard links to profiler, attribution, workers, IO/DB, overlay optimization, and Loki error deep-dive dashboards.
