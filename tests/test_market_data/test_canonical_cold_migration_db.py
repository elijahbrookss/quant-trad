"""Native PostgreSQL source and prepared-catalog smoke on a disposable database."""
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import JSON, bindparam, create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import publish_canonical_fact_archive
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION, assert_cold_history_placement
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.db.storage_target_models import StorageHeaderTablespaceRecord, StorageTargetRecord
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory, cold_layout_active
from scripts.db.canonical_cold_load import TargetDatabase, load_checkpoint
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from scripts.db.prepare_canonical_cold_catalog import prepare_catalog
from tests.test_market_data.migration_test_support import fresh_migration_database
from tests.test_market_data.test_canonical_cold_migration import DAY, LIMITS
from tests.test_market_data.test_canonical_fact_archive import _row


pytestmark = pytest.mark.db


@pytest.fixture
def prepared_database(tmp_path):
    tablespace = f"qt_cold_{uuid4().hex[:16]}"
    root = tmp_path / "pg-history"
    root.mkdir()
    admin = None
    try:
        with fresh_migration_database("canonical_cold", install_extensions=False) as dsn:
            engine = create_engine(dsn)
            try:
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    conn.execute(text(f"CREATE TABLESPACE {tablespace} LOCATION :root"), {"root": str(root)})
                admin = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
                yield engine, tablespace, root
            finally:
                engine.dispose()
    finally:
        if admin is not None:
            try:
                with admin.connect() as conn:
                    conn.execute(text(f"DROP TABLESPACE {tablespace}"))
            finally:
                admin.dispose()


SOURCE_FIELDS = {"storage_day": "date", "series_id": "bigint", "source_id": "bigint",
                 "revision": "integer", "market_commit_seq": "bigint",
                 **dict.fromkeys(("observation_time", "known_at", "accepted_at", "received_at",
                                  "source_published_at"), "timestamptz")}
SOURCE_JOIN_FIELDS = {"source_identity_key", "source_provider", "source_venue", "source_kind",
                      "source_adapter_version", "series_dimensions", "payload", "provenance", "quality"}


def _prepare(engine, rows, archived, tablespace, history_root):
    # Minimal old-source relations use the production query's shape. Cold
    # prepared tables below use the exact new ORM schema.
    fields = sorted(set(rows[0]) - SOURCE_JOIN_FIELDS | {"storage_day"})
    columns = ", ".join(f"{name} {SOURCE_FIELDS.get(name, 'text')}" for name in fields)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA market"))
        conn.execute(text("""
            CREATE FUNCTION market.reject_immutable_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'immutable canonical fixture'; END;
            $$
        """))
        conn.execute(text(f"CREATE TABLE market.fact_versions ({columns})"))
        conn.execute(text("""CREATE TABLE market.sources (id bigint PRIMARY KEY, identity_key text, provider text,
                          venue text, source_kind text, adapter_version text)"""))
        conn.execute(text("CREATE TABLE market.series (id bigint PRIMARY KEY, dimensions jsonb)"))
        conn.execute(text("""CREATE TABLE market.fact_hot_payloads (id text, storage_day date,
                          payload jsonb, provenance jsonb, quality jsonb)"""))
        conn.execute(text("""CREATE TABLE market.fact_retention_partitions (storage_day date, state text,
                          sealed_at timestamptz, expected_rows bigint)"""))
        conn.execute(text("""CREATE TABLE market.fact_archive_manifests (id text, storage_day date,
                          page_ordinal integer, object_key text, object_sha256 text, manifest_hash text,
                          row_count bigint, byte_count bigint, first_commit_seq bigint, first_id text,
                          last_commit_seq bigint, last_id text, descriptor jsonb)"""))
        conn.execute(text("""CREATE TABLE market.fact_storage_state (layout_version text PRIMARY KEY,
                          state text NOT NULL, completed_at timestamptz)"""))
        conn.execute(text("INSERT INTO market.fact_storage_state VALUES (:version,'ready',clock_timestamp())"),
                     {"version": FACT_STORAGE_LAYOUT_VERSION})
        StorageTargetRecord.__table__.create(conn)
        StorageHeaderTablespaceRecord.__table__.create(conn)
        conn.execute(StorageTargetRecord.__table__.insert(), {
            "id": "fixture-hdd", "label": "History fixture", "filesystem_uuid": "fixture-mount",
            "root": str(history_root), "medium": "hdd", "roles": ["history"],
        })
        sample = rows[0]
        conn.execute(text("""INSERT INTO market.sources VALUES (:source_id, :source_identity_key, :source_provider,
                          :source_venue, :source_kind, :source_adapter_version)"""), sample)
        conn.execute(text("""INSERT INTO market.series VALUES (1, '{"currency": "USD"}'),
                          (2, '{"currency": "USD"}')"""))
        conn.execute(text("""INSERT INTO market.fact_retention_partitions VALUES (:day, 'sealed', :seal, :expected)"""),
                     {"day": DAY, "seal": datetime(2026, 8, 10, tzinfo=UTC), "expected": len(rows)})
        values = ", ".join(":" + name for name in fields)
        conn.execute(text(f"INSERT INTO market.fact_versions ({', '.join(fields)}) VALUES ({values})"),
                     [row | {"storage_day": DAY} for row in rows])
        conn.execute(text("INSERT INTO market.fact_hot_payloads VALUES (:id, :day, :payload, :provenance, :quality)")
                     .bindparams(*(bindparam(name, type_=JSON) for name in ("payload", "provenance", "quality"))),
                     [row | {"day": DAY} for row in rows])
        conn.execute(text("""INSERT INTO market.fact_archive_manifests VALUES (:id,:day,0,:key,:sha,:hash,
                          :count,:bytes,:first_seq,:first_id,:last_seq,:last_id,:descriptor)""")
                     .bindparams(bindparam("descriptor", type_=JSON)), {
                         "id": archived.manifest_id, "day": DAY, "key": archived.object_key,
                         "sha": archived.object_sha256, "hash": archived.manifest_hash,
                         "count": archived.row_count, "bytes": archived.byte_count,
                         "first_seq": archived.first_cursor[0], "first_id": archived.first_cursor[1],
                         "last_seq": archived.last_cursor[0], "last_id": archived.last_cursor[1],
                         "descriptor": archived.to_dict(),
                     })
        target = conn.execute(text("""SELECT control.system_identifier::text, db.datname, db.oid::bigint
            FROM pg_catalog.pg_control_system() control CROSS JOIN pg_catalog.pg_database db
            WHERE db.datname=current_database()""")).one()
        tablespace_oid = conn.execute(text("SELECT oid::bigint FROM pg_tablespace WHERE spcname=:name"),
                                      {"name": tablespace}).scalar_one()
        identity = f"{target[0]}/{target[2]}"
        conn.execute(StorageHeaderTablespaceRecord.__table__.insert(), {
            "database_identity": identity, "target_id": "fixture-hdd", "tablespace_oid": tablespace_oid,
            "binding": {},
        })
        prepare_catalog(conn, expected_database=identity, target_id="fixture-hdd")
        assert_cold_history_placement(conn)
    return TargetDatabase(*target)


def test_native_pg_stages_and_resumes_inactive_catalog(prepared_database, tmp_path):
    engine, tablespace, history_root = prepared_database
    rows = [_row(), _row(2), _row(3, state="invalidated"), _row(series_id=2, commit=4)]
    original, destination = [FilesystemRawArchiveObjectStore(tmp_path / name) for name in ("source", "stage")]
    archived = publish_canonical_fact_archive(rows[:2], object_store=original,
                                              temporary_directory=tmp_path, limits=LIMITS)
    target = _prepare(engine, rows, archived, tablespace, history_root)
    checkpoint = MigrationCheckpoint(tmp_path / "progress.db")
    source = PostgresMigrationSource(engine, expected_system_identifier=target.system_identifier)
    kwargs = {"day": DAY, "source_store": original, "destination_store": destination,
              "temporary_directory": tmp_path, "limits": LIMITS}
    first = stage(source, checkpoint, **kwargs)
    assert first["rows"] == 2 and not first["cohort_complete"]
    assert stage(source, checkpoint, **kwargs)["cohort_complete"]
    assert checkpoint.verify_completed_cohort(day=DAY, destination_store=destination, limits=LIMITS)["verified_rows"] == 4

    loading = {"target": target, "object_store": destination, "day": DAY}
    with pytest.raises(RuntimeError, match="wrong_database"):
        load_checkpoint(engine, checkpoint, **(loading | {"target": TargetDatabase(target.system_identifier, "wrong", target.oid)}))
    with pytest.raises(RuntimeError, match="resume_page_not_admitted"):
        load_checkpoint(engine, checkpoint, after_ordinal=0, **loading)
    assert load_checkpoint(engine, checkpoint, **loading)["last_processed_ordinal"] == 0
    assert load_checkpoint(engine, checkpoint, **loading)["last_processed_ordinal"] == 0
    assert load_checkpoint(engine, checkpoint, after_ordinal=0, **loading)["checkpoint_end_reached"]
    with Session(engine) as session:
        assert not cold_layout_active(session)
        repo = PostgresColdHistory(ColdHistoryReader(destination, limits=LIMITS))
        assert repo.read_by_ids(session, [row["id"] for row in rows]) == {row["id"]: row for row in rows}
        with pytest.raises(DBAPIError, match="immutable canonical fixture"):
            with session.begin_nested():
                session.execute(text("DELETE FROM market.cold_fact_locators"))
        assert session.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one() == 4
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO market.fact_storage_state VALUES (:version, 'ready', clock_timestamp())"),
                     {"version": COLD_HISTORY_LAYOUT_VERSION})
    with pytest.raises(RuntimeError, match="layout_already_active"):
        load_checkpoint(engine, checkpoint, **loading)
