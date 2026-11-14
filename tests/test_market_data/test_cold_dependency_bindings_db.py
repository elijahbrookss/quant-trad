from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdHistoryReader, publish_cold_page
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import (
    ARCHIVE_FACT_REFERENCE_TABLES,
    assert_cold_dependency_bindings,
    prepare_cold_dependency_bindings,
)
from portal.backend.db.fact_identity_schema import assert_fact_identity_references, fact_header_partition_name
from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_storage import _catalog_manifest
from tests.test_market_data.test_canonical_fact_archive import _row
from tests.test_market_data.test_fact_storage_tiers_db import _ingest, _placement, storage

pytestmark = pytest.mark.db


def _reference(session, table, *, parent, fact_id, series_id):
    prepared = "prepared" in table
    parent_column = "preparation_id" if prepared else "manifest_id"
    columns = [parent_column, "fact_version_id"]
    values = [":parent", ":fact_id"]
    if prepared:
        columns.append("batch_ordinal")
        values.append("0")
    if table.endswith("material_aliases"):
        columns.extend(("evidence_key", "series_id", "material_hash"))
        values.extend(("'test-binding'", ":series_id", ":hash"))
    else:
        columns.append("row_hash")
        values.append(":hash")
    session.execute(text(f"""
        INSERT INTO market.{table} ({','.join(columns)}) VALUES ({','.join(values)})
    """), {"parent": parent, "fact_id": fact_id, "series_id": series_id, "hash": "a" * 64})


def test_all_archive_reference_roots_keep_generation_guards_and_check_cold_authority(storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    _ingest(storage)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
    )
    archive.seal_partition(day)
    manifest = archive.stage_next_page(day)["manifest_id"]
    with storage.database.session() as session:
        conn = session.connection()
        assert_fact_identity_references(conn)
        hot_id = session.scalar(text("SELECT id FROM market.fact_identities"))
        session.execute(text("""
            INSERT INTO market.fact_archive_preparations (
                id, manifest_id, storage_day, page_ordinal, generation, state, fence, owner_token_hash,
                lease_expires_at, verifier_version, source_binding_hash, manifest_descriptor, series_catalog,
                alias_count, canonical_dependency_count, alias_root, canonical_dependency_root
            ) VALUES ('test-building', :manifest, :day, 0, 2, 'building', 3, :hash,
                now()+interval '1 minute', 'fixture', :hash, '{}', '[]', 0, 0, :hash, :hash)
        """), {"manifest": manifest, "day": day, "hash": "a" * 64})
        for setting, value in (
            ("qt.archive_preparation_id", "test-building"),
            ("qt.archive_preparation_owner", "a" * 64),
            ("qt.archive_preparation_fence", "3"),
        ):
            session.scalar(text("SELECT set_config(:setting,:value,true)"), {"setting": setting, "value": value})

        with pytest.raises(RuntimeError, match="fixture rollback"):
            with session.begin_nested():
                prepare_cold_dependency_bindings(session.connection())
                assert_fact_identity_references(session.connection(), authoritative_cold=True)
                raise RuntimeError("fixture rollback")
        assert_fact_identity_references(conn)
        prepare_cold_dependency_bindings(conn)
        assert_cold_dependency_bindings(conn)
        with pytest.raises(RuntimeError, match="cold_fact_dependency_binding_invalid"):
            with session.begin_nested():
                session.execute(text("ALTER TABLE market.fact_archive_material_aliases DISABLE TRIGGER trg_require_hot_or_cold_fact"))
                assert_cold_dependency_bindings(session.connection())
        with pytest.raises(RuntimeError, match="cold_fact_dependency_function_invalid"):
            with session.begin_nested():
                session.execute(text("""
                    CREATE OR REPLACE FUNCTION market.require_hot_or_cold_fact() RETURNS trigger
                    LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog
                    AS $$ BEGIN RETURN NULL; END; $$
                """))
                assert_cold_dependency_bindings(session.connection())

        visible = _row(series_id=storage.series_id, commit=120)
        staged = _row(revision=2, series_id=storage.series_id, commit=121)
        cold = PostgresColdHistory(ColdHistoryReader(store))
        cold.admit(session, publish_cold_page([visible], object_store=store, temporary_directory=tmp_path))
        staged_page = publish_cold_page([staged], object_store=store, temporary_directory=tmp_path)
        staged_day = day - timedelta(days=1)
        cold.admit(session, staged_page, staged_storage_day=staged_day)

        for table in ARCHIVE_FACT_REFERENCE_TABLES:
            parent = "test-building" if "prepared" in table else manifest
            _reference(session, table, parent=parent, fact_id=hot_id, series_id=storage.series_id)
            with pytest.raises(DBAPIError, match="canonical_archive_fact_reference_missing") as refused:
                with session.begin_nested():
                    _reference(session, table, parent=parent, fact_id=visible["id"], series_id=storage.series_id)
            assert refused.value.orig.pgcode == "23503"

        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:value"),
                        {"value": COLD_HISTORY_LAYOUT_VERSION})
        for table in ARCHIVE_FACT_REFERENCE_TABLES:
            parent = "test-building" if "prepared" in table else manifest
            _reference(session, table, parent=parent, fact_id=visible["id"], series_id=storage.series_id)
            with pytest.raises(DBAPIError, match="canonical_archive_fact_reference_missing"):
                with session.begin_nested():
                    _reference(session, table, parent=parent, fact_id=staged["id"], series_id=storage.series_id)

        session.execute(text("""
            INSERT INTO market.cold_fact_retired_days
                (storage_day, manifest_set_hash, row_count, reclaimed_header_bytes, protected_dataset_ranges)
            VALUES (:day, :hash, 1, 0, 0)
        """), {"day": staged_day, "hash": staged_page.manifest_hash})
        for table in ARCHIVE_FACT_REFERENCE_TABLES:
            parent = "test-building" if "prepared" in table else manifest
            _reference(session, table, parent=parent, fact_id=staged["id"], series_id=storage.series_id)

        session.scalar(text("SELECT set_config('qt.archive_preparation_fence','4',true)"))
        with pytest.raises(DBAPIError, match="prepared archive insert refused"):
            with session.begin_nested():
                session.execute(text("""
                    INSERT INTO market.fact_archive_prepared_material_aliases
                        (preparation_id, fact_version_id, evidence_key, batch_ordinal, series_id, material_hash)
                    VALUES ('test-building', :id, 'wrong-owner', 0, :series, :hash)
                """), {"id": visible["id"], "series": storage.series_id, "hash": "a" * 64})

        # Model the interval after a header is retired but its identity still
        # exists. With neither a live header nor a cold locator, fail closed.
        conn.exec_driver_sql(f"DROP TABLE market.{fact_partition_name(day)}")
        conn.exec_driver_sql(f"DROP TABLE market.{fact_header_partition_name(day)}")
        with pytest.raises(DBAPIError, match="canonical_archive_fact_reference_missing"):
            with session.begin_nested():
                session.execute(text("""
                    INSERT INTO market.fact_archive_material_aliases
                        (manifest_id, fact_version_id, evidence_key, series_id, material_hash)
                    VALUES (:manifest, :id, 'no-header', :series, :hash)
                """), {"manifest": manifest, "id": hot_id, "series": storage.series_id, "hash": "a" * 64})

        saved = session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE id=:id"),
                                {"id": manifest}).mappings().one()
        cold.admit(session, _catalog_manifest(saved), staged_storage_day=day)
        session.execute(text("""
            INSERT INTO market.cold_fact_retired_days
                (storage_day, manifest_set_hash, row_count, reclaimed_header_bytes, protected_dataset_ranges)
            VALUES (:day, :hash, 1, 0, 0)
        """), {"day": day, "hash": saved["manifest_hash"]})
        session.execute(text("""
            INSERT INTO market.fact_archive_material_aliases
                (manifest_id, fact_version_id, evidence_key, series_id, material_hash)
            VALUES (:manifest, :id, 'retired-header', :series, :hash)
        """), {"manifest": manifest, "id": hot_id, "series": storage.series_id, "hash": "a" * 64})
