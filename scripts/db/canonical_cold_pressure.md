# Read-only cold migration pressure receipt

Run this manually before and during a bounded cold-catalog load. Obtain one fresh JSON Lines capture from `scripts.reporting.server_filesystem_capacity` on the database host. Qualify that the PostgreSQL WAL root is on the supplied SSD and the registered cold target is on the supplied HDD from the deployment's existing mount evidence. The filesystem capture and SQL connection are separate observations.

Use the approved operator `PG_DSN`, PostgreSQL 15 or later, and a role with `pg_monitor` access plus permission to execute `pg_control_system()`. Supply the audited database identity, device UUIDs, and the maximum sample age chosen for this observation.

```sh
PYTHONPATH=src:. python -m scripts.db.canonical_cold_pressure native-filesystems.jsonl \
  --database "$QT_PRESSURE_DATABASE" --database-oid "$QT_PRESSURE_DATABASE_OID" \
  --system-identifier "$QT_PRESSURE_SYSTEM_IDENTIFIER" --ssd-uuid "$QT_PRESSURE_SSD_UUID" \
  --hdd-uuid "$QT_PRESSURE_HDD_UUID" --max-sample-age-seconds "$QT_PRESSURE_SAMPLE_AGE_SECONDS"
```

`resident_wal` measures current files visible to `pg_ls_waldir()`. `wal_since_reset` and `database_temp_since_reset` measure cumulative writes. `resident_default_tablespace_temp` counts files visible in the default tablespace across databases. Custom temp tablespaces and files that finish between samples require separate evidence. The reported settings apply to the observer session and may differ for the loader or another role. The archive marker inventory includes both all `.ready` markers and the subset for WAL segments. The receipt records observed pressure and does not reserve storage. Check the current admission, backup, and restore requirements separately before running a load.
