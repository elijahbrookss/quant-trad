# Rehearse the provider stop and restart boundary

This fixture uses the real continuous stream runtime, Coinbase parser, durable spool, archive finalizer, and PostgreSQL repositories. It substitutes a finite scripted transport for the provider connection. The tests use a disposable database and temporary spool/archive roots. They stop two fixture streams only. Production boundary capture remains the operator procedure in the [cutover runbook](../../docs/engineering/canonical-cold-cutover-runbook.md).

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

Actual deployment needs fresh live provider, WAL, lease, archive, and filesystem receipts under the approved outage plan. This fixture covers a planned disconnect between sessions and an injected subsequent anomaly. It does not exercise a capture bridge, live exchange behavior, simultaneous fleet interruption, missed polling schedules, or recovery of the historical BIP incident. The prior BIP evidence is a seeded test event.
