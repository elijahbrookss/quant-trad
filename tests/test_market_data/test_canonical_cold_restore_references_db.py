from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical import CanonicalFactRecord
from market_data.canonical_storage import record_to_storage_row
from market_data.cold_history import ColdHistoryReader, publish_cold_page
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import (
    ARCHIVE_FACT_REFERENCE_TABLES,
    assert_cold_dependency_bindings,
    prepare_cold_dependency_bindings,
)
from portal.backend.db.fact_identity_schema import assert_fact_identity_references
from portal.backend.db.fact_storage_schema import (
    FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract, ensure_fact_payload_partition,
)
from portal.backend.db.market_data_models import MarketFactVersionRecord
from portal.backend.db.market_storage_models import MarketFactHotPayloadRecord
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from scripts.db.canonical_cold_database_fence import close_database_fence
from scripts.db.canonical_cold_restore_references import (
    capture_original_reference_bindings,
    restore_original_reference_bindings,
)
from tests.test_market_data.test_canonical_cold_activate_db import _fence_connections
from tests.test_market_data.test_cold_dependency_bindings_db import _reference
from tests.test_market_data.test_fact_storage_tiers_db import _ingest, _placement, storage  # noqa: F401


pytestmark = pytest.mark.db


def test_restore_original_archive_foreign_keys_requires_rematerialized_identity(storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    _ingest(storage)
    engine = storage.database._engine
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
        original = capture_original_reference_bindings(conn)
        previous_evidence = conn.scalar(text("SELECT evidence FROM market.fact_storage_state"))
    original_hash = archive_evidence_hash(original)

    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
    )
    archive.seal_partition(day)
    old_manifest = archive.stage_next_page(day)["manifest_id"]
    later = record_to_storage_row(CanonicalFactRecord(
        series_id=storage.series_id, source_id=storage.source_id, revision=1, market_commit_seq=100,
        fact=replace(storage.fact, observation_key="restored-reference"), ingestion_run_id=None,
    ), series_dimensions={})

    with storage.database.session() as session:
        prepare_cold_dependency_bindings(session.connection())
        PostgresColdHistory(ColdHistoryReader(store)).admit(
            session, publish_cold_page([later], object_store=store, temporary_directory=tmp_path),
        )
        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                        {"layout": COLD_HISTORY_LAYOUT_VERSION})
        _reference(session, "fact_archive_material_aliases", parent=old_manifest,
                   fact_id=later["id"], series_id=storage.series_id)
        assert_cold_dependency_bindings(session.connection())

    with _fence_connections(storage) as (conn, control):
        fence = close_database_fence(conn, control, expected_database=original["database"], timeout_seconds=60,
                                     record=lambda *_: None)
        args = {"fence": fence, "original": original, "expected_hash": original_hash, "timeout_seconds": 20}
        with conn.begin():
            with pytest.raises(ValueError, match="original_evidence_changed"):
                restore_original_reference_bindings(conn, **{**args, "expected_hash": "a" * 64})
            # The cold trigger had admitted this reference. The old FK must
            # refuse it while the referenced identity remains cold-only.
            with pytest.raises(IntegrityError, match="violates foreign key constraint"):
                restore_original_reference_bindings(conn, **args)
            assert_cold_dependency_bindings(conn)

        # The reference switch owns constraints, with the header and payload
        # rehydration handled by the separate reverse projection step.
        with conn.begin():
            ensure_fact_payload_partition(conn, storage.today)
            values = {**later, "storage_day": storage.today}
            for model in (MarketFactVersionRecord, MarketFactHotPayloadRecord):
                table = model.__table__
                conn.execute(table.insert(), {name: values[name] for name in table.columns.keys()})
            with pytest.raises(RuntimeError, match="fixture abort"):
                with conn.begin_nested():
                    assert not restore_original_reference_bindings(conn, **args)["already_restored"]
                    raise RuntimeError("fixture abort")
            assert_cold_dependency_bindings(conn)
            assert not restore_original_reference_bindings(conn, **args)["already_restored"]
            for table in ARCHIVE_FACT_REFERENCE_TABLES:
                assert conn.scalar(text("""
                    SELECT convalidated FROM pg_constraint WHERE conrelid=to_regclass(:table) AND conname=:name
                """), {"table": f"market.{table}", "name": original["constraints"][table]})
        with conn.begin():
            assert restore_original_reference_bindings(conn, **args)["already_restored"]
            assert_fact_identity_references(conn)
            assert conn.scalar(text("SELECT evidence FROM market.fact_storage_state")) == previous_evidence
            assert conn.scalar(text("SELECT count(*) FROM market.fact_archive_material_aliases WHERE fact_version_id=:id"),
                               {"id": later["id"]}) == 1
            conn.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                         {"layout": FACT_STORAGE_LAYOUT_VERSION})
            assert_fact_storage_contract(conn)
