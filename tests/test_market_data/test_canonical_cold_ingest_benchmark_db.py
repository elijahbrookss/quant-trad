from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from scripts.db import canonical_cold_ingest_benchmark as mixed
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage  # noqa: F401


pytestmark = pytest.mark.db


def test_native_mixed_workload_preserves_pinned_history_and_live_hot_placement(storage, tmp_path, monkeypatch):
    from market_data.archive import FilesystemRawArchiveObjectStore
    from market_data.cold_history import ColdHistoryReader
    from market_data.fact_archive import publish_canonical_fact_archive
    from portal.backend.db.fact_identity_schema import fact_header_partition_name
    from portal.backend.db.fact_storage_schema import fact_partition_name
    from portal.backend.service.storage.repos.cold_history import PostgresColdHistory
    from portal.backend.service.storage.repos.fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM

    source = storage.database._engine
    candidate_name = "qt_bench_mixed_" + uuid4().hex[:12]
    candidate = create_engine(source.url.set(database=candidate_name))
    admin = create_engine(source.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    old_day = storage.today - timedelta(days=1)
    _placement(monkeypatch, old_day)
    _ingest(storage)
    _placement(monkeypatch, storage.today)
    fresh = replace(storage.fact, observation_key="second-fixture", observation_time=BASE + timedelta(seconds=1))
    commit = _ingest(storage, fresh).max_commit_seq
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    old, hot = storage.repo.read_fact_revisions(series_id=storage.series_id, start=BASE - timedelta(seconds=1),
                                                   end=BASE + timedelta(seconds=2))
    with source.connect() as conn:
        archived = [dict(row) for row in conn.execute(text(
            f"SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM} WHERE versions.storage_day=:day"),
            {"day": old_day}).mappings()]
    for row in archived:
        row.pop("storage_day")
    page = publish_canonical_fact_archive(archived, object_store=store, temporary_directory=tmp_path)
    plan = {
        "reads": {"restore_pair_hash": "a" * 64, "commit_seq": commit, "retire_before": storage.today.isoformat(),
            "repeats": 16, "timeout_seconds": 15, "max_rows": 50, "max_archive_bytes": 2**20,
            "cases": [{"name": "precutover", "mode": "all_revisions", "series_id": storage.series_id,
                       "start": (BASE - timedelta(seconds=1)).isoformat(), "end": (BASE + timedelta(seconds=2)).isoformat(),
                       "known_at_lte": (BASE + timedelta(days=1)).isoformat(), "source_identity_keys": [],
                       "storage_witnesses": {old.fact_version_id: old_day.isoformat(),
                                             hot.fact_version_id: storage.today.isoformat()}}]},
        "batches": 8, "batch_rows": 3, "interval_ms": 5, "observation_start": BASE.isoformat(),
    }
    def fixture_evidence(engine):
        # The repo's standard database lane is the disposable Docker container.
        # The existing benchmark fixture separately exercises socket-only admission.
        with engine.connect() as conn:
            assert conn.scalar(text("SHOW transaction_read_only")) == "on"
            layout, cutoff = conn.execute(text("SELECT layout_version,evidence->>'cold_before_day' FROM market.fact_storage_state")).one()
            system, oid = conn.execute(text("""SELECT system_identifier::text,
                (SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()""")).one()
            assert conn.scalar(text("SELECT extversion FROM pg_extension WHERE extname='timescaledb'")) is not None
        return {"system_identifier": system, "oid": oid, "layout": layout, "cold_before_day": cutoff,
                "postgres": "fixture", "timescaledb": "fixture", "settings": {}}

    def trial(side, engine):
        monkeypatch.setattr(mixed.reads, "_db_url", lambda _dsn: engine.url)
        monkeypatch.setattr(mixed.reads, "_database_evidence", fixture_evidence)
        guard = create_engine(engine.url, connect_args={"options": "-c default_transaction_read_only=on"})
        try:
            declared = fixture_evidence(guard)
        finally:
            guard.dispose()
        return mixed.run(SimpleNamespace(side=side, revision="b" * 40, system_identifier=declared["system_identifier"],
            database_oid=declared["oid"], archive_root=tmp_path, disposable_database=engine.url.database,
            runtime_digest="sha256:" + "c" * 64), plan)

    try:
        source.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE {candidate_name} TEMPLATE {source.url.database}")
        with candidate.begin() as conn:
            PostgresColdHistory(ColdHistoryReader(store)).admit(conn, page)
            conn.execute(text("""INSERT INTO market.cold_fact_retired_days
                (storage_day,manifest_set_hash,row_count,reclaimed_header_bytes,protected_dataset_ranges,handoff_kind)
                VALUES (:day,:digest,1,0,0,'initial')"""), {"day": old_day, "digest": page.manifest_hash})
            conn.execute(text("""UPDATE market.fact_storage_state SET layout_version='market.canonical_cold_history.v1',
                evidence=jsonb_build_object('cold_before_day',CAST(:cutoff AS text))"""), {"cutoff": storage.today})
            conn.exec_driver_sql(f"DROP TABLE market.{fact_header_partition_name(old_day)}")
            conn.exec_driver_sql(f"DROP TABLE market.{fact_partition_name(old_day)}")
        monkeypatch.setattr(mixed.reads, "_code_revision", lambda _module, expected: expected)
        left, right = trial("source", source), trial("candidate", candidate)
        result = mixed.compare(left, right)
        assert left["ingestion"]["rows"] == right["ingestion"]["rows"] == 12
        assert result["candidate"]["history"]["precutover"]["overlapping"]["count"] >= 1
        assert all(item["archive_counters"].get("archive_source_bytes", 0) > 0 for item in right["history_samples"])
        assert [tuple(row["counts"]) for row in right["write_samples"]] == [(3, 0, 0), (0, 0, 3), (0, 3, 0), (0, 0, 3)] * 2
    finally:
        candidate.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {candidate_name} WITH (FORCE)")
        admin.dispose()
