# Rehearse the provider stop and restart boundary

This fixture uses the real continuous stream runtime, Coinbase parser, durable spool, archive finalizer, and PostgreSQL repositories. It substitutes a finite scripted transport for the provider connection. The tests exercise both stream types and one scheduled polling definition per case on a disposable database with temporary spool and archive roots. The fixture also runs the approved drain checker against the same shutdown. [Cutover runbook](../../docs/engineering/canonical-cold-cutover-runbook.md)

From a disposable DB test environment, set `QT_PROVIDER_BOUNDARY_FIXTURE_OUTPUT` to an existing directory to retain JSON receipts. Run the focused fixture using the standard database lane.

```sh
QT_PROVIDER_BOUNDARY_FIXTURE_OUTPUT=/review/provider-fixtures \
pytest -q tests/test_market_data/test_canonical_cold_provider_boundary_db.py
```

Each mode writes three hashed snapshots and one classified result. `pre_stop` pauses the scripted transport after the tail has been fsynced while the old lease is live. `drained` requires the old lease to be absent and verifies all original tail frame IDs, byte hashes, and receive ordinals against current Parquet bytes and database mappings. `resumed` requires a new session, an acknowledged subscription and a heartbeat in raw bytes, a newly opened trade coverage interval or Level 2 snapshot, and a later graceful terminal observation for this bounded fixture. Every snapshot records the accepted canonical commit watermark. All snapshots use read-only repeatable PostgreSQL snapshots and a paused or completed fixture transport for filesystem stability.

The trade schedule verifies archive-complete and canonicalization watermarks. The Level 2 schedule seeds a BIP-labeled prefix budget quality event before stopping, then injects a distinct provider sequence gap in the resumed session. The result keeps pre-existing, drain, and post-start quality evidence separate. The interruption between old and new proven coverage remains an unproven interval. The result reports no event count for that interval. Ordinals belong to each individual session and epoch, so subtracting old and new positions cannot quantify provider loss.

Recheck a retained set without a database or spool connection.

```sh
PYTHONPATH=src:. python -m scripts.db.canonical_cold_provider_boundary \
  --pre-stop /review/provider-fixtures/level2-pre_stop.json \
  --drained /review/provider-fixtures/level2-drained.json \
  --resumed /review/provider-fixtures/level2-resumed.json
```

The approved drain checker records the exact current stream session and last published position, requires the supervisor stop and released producer leases, checks for pending raw spool files, and verifies the tail intervals against actual Parquet bytes and PostgreSQL mappings at the final commit. The scheduled polling receipt identifies the first due time at risk. Tests refuse an unlisted enabled poll, an incorrect final commit, and a remaining raw spool file.

Actual deployment needs fresh provider, WAL, lease, archive, and filesystem receipts under the approved outage plan. The fixture covers a planned disconnect between sessions and an injected subsequent anomaly. The capture bridge, live exchange behavior, full fleet interruption, and actual missed polling schedules require separate qualification. The prior BIP evidence is a seeded test event.
