from time import monotonic

import pytest
from sqlalchemy import create_engine

from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore, publish_spool_archive
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_structure
from scripts.db import canonical_cold_raw_benchmark as benchmark
from tests.test_market_data.test_fact_storage_tiers_db import storage  # noqa: F401


pytestmark = pytest.mark.db


def test_native_raw_publication_with_concurrent_lineage(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(market_structure, "db", storage.database)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(tmp_path))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    spool = tmp_path / "spool"
    staging = tmp_path / "staging"
    staging.mkdir()
    repo = market_structure.PostgresMarketStructureRepository()
    database = storage.database
    seed = benchmark._register(database, repo, "qt-raw-fixture-seed")
    claim = repo.claim_stream(definition_id=seed["market_trades"], owner_id="seed", lease_seconds=30, bounded=True)
    segment = DurableRawSpoolSegment(root=spool, definition_id=claim.definition_id, session_id=claim.session_id,
                                    connection_epoch=0)
    raw = benchmark._append(segment, claim, ordinal=1, channel="market_trades")
    segment.seal()
    encoded, ack, rows = publish_spool_archive(segment, object_store=store, temporary_directory=staging)
    commit = repo.commit_archive(claim, encoded=encoded, acknowledgement=ack, records=rows)
    segment.mark_database_acknowledged(manifest_id=commit.manifest_id, object_key=ack.object_key, object_sha256=ack.sha256)
    segment.discard_acknowledged_spool()
    repo.release(claim)
    deadline = monotonic() + 30

    def check():
        assert monotonic() < deadline

    plan = benchmark.validate_plan({"seed_sha256": "a" * 64, "historical_record_ids": [raw.raw_record_id],
                                    "segments": 4, "rows_per_segment": 8, "backlog_segments": 2,
                                    "records_per_second": 0, "historical_lookup_rows": 1, "historical_lookups": 30,
                                    "retry_every_segments": 1, "timeout_seconds": 30})
    reader = Database()
    reader._engine = create_engine(storage.dsn, connect_args={"options": "-c default_transaction_read_only=on"})
    from sqlalchemy.orm import sessionmaker
    reader._session_factory = sessionmaker(bind=reader._engine)
    reader._available = True
    placement = {key: {"device_id": "0:0"} for key in ("index", "archive", "working")}
    try:
        before = benchmark._settled_history(reader, store, plan["historical_record_ids"], check)
        definitions = benchmark._register(database, repo, "qt-raw-fixture-trial")
        initial = benchmark._pressure(reader._engine, placement)
        writes, reads, manifests, normalized, backlog, elapsed = benchmark.exercise(
            reader, repo, definitions=definitions, plan=plan, spool_root=spool, staging=staging, store=store, check=check)
        pressure = benchmark._delta(initial, benchmark._pressure(reader._engine, placement))
        after = benchmark._settled_history(reader, store, plan["historical_record_ids"], check)
        verification = benchmark._verify_fresh(reader, store, manifests, plan["rows_per_segment"], check)
    finally:
        reader._engine.dispose()
    summary = benchmark._summary(writes, reads, elapsed, plan)
    assert before == after and {row["ordered_sha256"] for row in reads} == {before["ordered_sha256"]}
    assert backlog["terminal_bytes"] == 0 and backlog["spool_bytes"] > 0
    assert verification["mappings"] == 32 and verification["manifests"] == 4 and len(normalized) == 64
    assert summary["commits_overlapping_history"] > 0 and summary["retry_ns"]["count"] == 4
    assert pressure["wal_position"] > 0 and pressure["peak_process_rss_kib_at_end"] > 0
    print({"summary": summary, "backlog": backlog, "verification": verification, "pressure": pressure})
