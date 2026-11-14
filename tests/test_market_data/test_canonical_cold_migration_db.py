"""Native PostgreSQL source and prepared-catalog smoke on a disposable database."""
from datetime import UTC, datetime, timedelta
from contextlib import contextmanager
from dataclasses import replace
import os
from pathlib import Path
import pwd
import shutil
import subprocess
from tempfile import TemporaryDirectory
from time import monotonic
from uuid import uuid4

import pytest
from sqlalchemy import JSON, bindparam, create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import publish_canonical_fact_archive
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION, assert_cold_history_placement
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.db.market_storage_models import (
    MarketFactArchiveActiveGenerationRecord, MarketFactArchiveCatalogBatchRecord,
    MarketFactArchivePreparationRecord, MarketFactArchivePreparedCanonicalDependencyRecord,
    MarketFactArchivePreparedMaterialAliasRecord, MarketFactArchiveVerificationGenerationRecord,
    MarketFactArchiveVerificationRecord,
)
from portal.backend.db.storage_target_models import StorageHeaderTablespaceRecord, StorageTargetRecord
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory, cold_layout_active
from portal.backend.service.storage.repos.fact_archival import FACT_ARCHIVE_VERIFIER_VERSION, _receipt
from portal.backend.service.storage.repos.fact_archive_generation import (
    archive_preparation_id, catalog_batch_hash, catalog_commitment, catalog_root,
)
from scripts.db.canonical_cold_load import TargetDatabase, load_checkpoint
from scripts.db import canonical_cold_load
from scripts.db.canonical_cold_load_pressure import CatalogLoadLimits, LoadPressure, supervise_page
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from scripts.db.prepare_canonical_cold_catalog import prepare_catalog
from tests.test_market_data.test_canonical_cold_migration import DAY, LIMITS
from tests.test_market_data.test_canonical_fact_archive import _row


pytestmark = pytest.mark.db


@pytest.fixture(scope="module")
def local_catalog_server():
    """Give the catalog SQL and the test process the same filesystem view."""
    if os.getenv("QT_DB_TEST_ISOLATED") != "1" or os.getenv("RUN_DB_TESTS") != "1":
        raise RuntimeError("cold_catalog_tests_require_disposable_db_runner")
    bindir = Path("/usr/lib/postgresql/15/bin")
    pg_config = shutil.which("pg_config")
    if not (bindir / "initdb").is_file() and pg_config is not None:
        bindir = Path(subprocess.run([pg_config, "--bindir"], check=True, capture_output=True,
                                     text=True, timeout=10).stdout.strip())
    if not all((bindir / name).is_file() for name in ("initdb", "pg_ctl")):
        raise RuntimeError("cold_catalog_tests_require_local_postgresql_server_tools")
    command_prefix = []
    account = pwd.getpwuid(os.geteuid())
    if os.geteuid() == 0:
        runuser = shutil.which("runuser")
        if runuser is None:
            raise RuntimeError("cold_catalog_tests_require_runuser_as_root")
        account = pwd.getpwnam("postgres")
        command_prefix = [runuser, "-u", account.pw_name, "--"]

    environment = {"PATH": os.environ["PATH"], "LC_ALL": "C"}
    with TemporaryDirectory(prefix="qt-cold-catalog-", dir="/tmp") as directory:
        root = Path(directory)
        if os.geteuid() == 0:
            os.chown(root, account.pw_uid, account.pw_gid)
        data, socket = root / "pgdata", root / "socket"
        socket.mkdir(mode=0o700)
        if os.geteuid() == 0:
            os.chown(socket, account.pw_uid, account.pw_gid)
        subprocess.run([*command_prefix, str(bindir / "initdb"), "-D", str(data), "--no-locale",
                        "--encoding=UTF8", "--auth-local=trust", "--auth-host=reject", "--no-sync"],
                       check=True, capture_output=True, text=True, timeout=120, env=environment)
        ctl = [*command_prefix, str(bindir / "pg_ctl"), "-D", str(data), "-w", "-t", "45"]
        log = root / "postgres.log"
        options = (f"-c listen_addresses= -c unix_socket_directories={socket} "
                   "-c shared_buffers=16MB -c max_connections=10 -c fsync=off")
        try:
            subprocess.run([*ctl, "-l", str(log), "-o", options, "start"], check=True,
                           capture_output=True, text=True, timeout=60, env=environment)
            url = URL.create("postgresql+psycopg2", username=account.pw_name,
                             database="postgres", query={"host": str(socket)})
            yield url, root, account
        finally:
            if (data / "postmaster.pid").exists():
                subprocess.run([*ctl, "-m", "fast", "stop"], check=True,
                               capture_output=True, text=True, timeout=60, env=environment)


@pytest.fixture
def prepared_database(local_catalog_server):
    url, server_root, account = local_catalog_server
    database = f"qt_cold_{uuid4().hex[:16]}"
    tablespace = f"qt_cold_{uuid4().hex[:16]}"
    devices_root = Path(os.environ.get("QT_COLD_LOAD_FIXTURE_HISTORY_ROOT", server_root))
    root = devices_root / tablespace
    root.mkdir(mode=0o700)
    if os.geteuid() == 0:
        os.chown(root, account.pw_uid, account.pw_gid)
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    engine = create_engine(url.set(database=database))
    try:
        with admin.connect() as conn:
            conn.execute(text("SET statement_timeout = '15s'"))
            conn.exec_driver_sql(f"CREATE DATABASE {database} TEMPLATE template0")
            conn.execute(text(f"CREATE TABLESPACE {tablespace} LOCATION :root"), {"root": str(root)})
        yield engine, tablespace, root
    finally:
        engine.dispose()
        try:
            with admin.connect() as conn:
                conn.execute(text("SET statement_timeout = '15s'"))
                conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)")
                conn.exec_driver_sql(f"DROP TABLESPACE IF EXISTS {tablespace}")
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
        conn.execute(text("""CREATE TABLE market.fact_retention_partitions (storage_day date PRIMARY KEY, state text,
                          sealed_at timestamptz, expected_rows bigint)"""))
        conn.execute(text("""CREATE TABLE market.fact_archive_manifests (id text PRIMARY KEY, storage_day date,
                          page_ordinal integer, object_key text, object_sha256 text, manifest_hash text,
                          row_count bigint, byte_count bigint, first_commit_seq bigint, first_id text,
                          last_commit_seq bigint, last_id text, descriptor jsonb)"""))
        conn.execute(text("""CREATE TABLE market.fact_identities (id text PRIMARY KEY,storage_day date,
            series_id bigint,observation_key text,revision integer,UNIQUE(series_id,observation_key,revision))"""))
        for model in (MarketFactArchivePreparationRecord, MarketFactArchiveCatalogBatchRecord,
                      MarketFactArchivePreparedMaterialAliasRecord, MarketFactArchivePreparedCanonicalDependencyRecord,
                      MarketFactArchiveActiveGenerationRecord, MarketFactArchiveVerificationRecord,
                      MarketFactArchiveVerificationGenerationRecord):
            model.__table__.create(conn)
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


def _published_generation(conn, manifest, dependency):
    preparation = archive_preparation_id(manifest.manifest_id, 1)
    document = {"fact_version_id": dependency["id"], "row_hash": dependency["row_hash"]}
    batch_hash = catalog_batch_hash(preparation_id=preparation, catalog_kind="canonical_dependencies",
                                    ordinal=0, rows=[document])
    commitment = catalog_commitment(manifest_id=manifest.manifest_id, preparation_id=preparation,
        generation=1, alias_count=0, canonical_dependency_count=1, alias_batches=[], canonical_dependency_batches=[batch_hash])
    conn.execute(MarketFactArchivePreparationRecord.__table__.insert(), {
        "id": preparation, "manifest_id": manifest.manifest_id, "storage_day": DAY, "page_ordinal": 0,
        "generation": 1, "state": "active", "fence": 1, "owner_token_hash": "a" * 64,
        "lease_expires_at": datetime.now(UTC) + timedelta(hours=1), "verifier_version": FACT_ARCHIVE_VERIFIER_VERSION,
        "source_binding_hash": "b" * 64, "manifest_descriptor": manifest.to_dict(), "series_catalog": [],
        "alias_count": 0, "canonical_dependency_count": 1, "alias_root": catalog_root([]),
        "canonical_dependency_root": catalog_root([document]), "catalog_commitment": commitment,
    })
    conn.execute(text("""INSERT INTO market.fact_identities VALUES (:id,:day,:series_id,:observation_key,:revision)
        ON CONFLICT (id) DO NOTHING"""),
                 dependency | {"day": DAY})
    conn.execute(MarketFactArchivePreparedCanonicalDependencyRecord.__table__.insert(),
                 document | {"preparation_id": preparation, "batch_ordinal": 0})
    conn.execute(MarketFactArchiveCatalogBatchRecord.__table__.insert(), {
        "preparation_id": preparation, "catalog_kind": "canonical_dependencies", "batch_ordinal": 0,
        "row_count": 1, "first_key": [dependency["id"]], "last_key": [dependency["id"]], "content_hash": batch_hash,
    })
    pointer = {"manifest_id": manifest.manifest_id, "preparation_id": preparation,
               "generation": 1, "catalog_commitment": commitment}
    conn.execute(MarketFactArchiveActiveGenerationRecord.__table__.insert(), pointer)
    receipt = _receipt({"id": manifest.manifest_id, "manifest_hash": manifest.manifest_hash,
                        "storage_day": DAY, "page_ordinal": 0}, "c" * 64)
    conn.execute(MarketFactArchiveVerificationRecord.__table__.insert(), receipt)
    conn.execute(MarketFactArchiveVerificationGenerationRecord.__table__.insert(), {
        "manifest_id": manifest.manifest_id, "verifier_version": FACT_ARCHIVE_VERIFIER_VERSION,
        "preparation_id": preparation, "catalog_commitment": commitment,
    })
    return pointer, receipt


@pytest.mark.parametrize("with_prepared_authority", [False, True])
def test_native_pg_stages_and_resumes_inactive_catalog(prepared_database, tmp_path, with_prepared_authority, monkeypatch):
    engine, tablespace, history_root = prepared_database
    rows = [_row(), _row(2), _row(3, state="invalidated"), _row(series_id=2, commit=4)]
    original, destination = [FilesystemRawArchiveObjectStore(tmp_path / name) for name in ("source", "stage")]
    archived = publish_canonical_fact_archive(rows[:2], object_store=original,
                                              temporary_directory=tmp_path, limits=LIMITS)
    target = _prepare(engine, rows, archived, tablespace, history_root)
    if with_prepared_authority:
        with engine.begin() as conn:
            pointer, receipt = _published_generation(conn, archived, rows[0])
    checkpoint = MigrationCheckpoint(tmp_path / "progress.db")
    source = PostgresMigrationSource(engine, expected_system_identifier=target.system_identifier)
    kwargs = {"day": DAY, "source_store": original, "destination_store": destination,
              "temporary_directory": tmp_path, "limits": LIMITS}
    first = stage(source, checkpoint, **kwargs)
    assert first["rows"] == 2 and not first["cohort_complete"]
    assert stage(source, checkpoint, **kwargs)["cohort_complete"]
    first, last = checkpoint.iter_completed_pages()
    assert first.reused and first.manifest == archived
    assert not last.reused and last.source_authority is None
    if with_prepared_authority:
        assert first.source_authority["kind"] == "prepared_generation"
        assert first.source_authority["catalog_commitment"] == pointer["catalog_commitment"]
        assert first.source_authority["verification_hash"] == receipt["verification_hash"]
        assert first.source_authority["prepared_catalogs"]["canonical_dependencies"]["count"] == 1
    else:
        assert first.source_authority["kind"] == "legacy_catalog"
        assert first.source_authority["verification_hash"] is None
    assert checkpoint.verify_completed_cohort(day=DAY, destination_store=destination, limits=LIMITS)["verified_rows"] == 4

    loading = {"target": target, "object_store": destination, "day": DAY}
    with pytest.raises(RuntimeError, match="wrong_database"):
        load_checkpoint(engine, checkpoint, **(loading | {"target": TargetDatabase(target.system_identifier, "wrong", target.oid)}))
    with pytest.raises(RuntimeError, match="resume_page_not_admitted"):
        load_checkpoint(engine, checkpoint, after_ordinal=0, **loading)
    calls = []

    @contextmanager
    def measured_boundary(session, **_scope):
        calls.append(session.scalar(text("SELECT count(*) FROM market.cold_fact_pages")))
        yield
        if calls == [0, 1]:
            raise LoadPressure("test pressure after inserting the second page")

    monkeypatch.setattr(canonical_cold_load, "supervise_page", measured_boundary)
    caps = CatalogLoadLimits("fixture-ssd", "fixture-hdd", 1024, 1024, 1024, 1024, 1024, 1024, 0, 64)
    stopped = load_checkpoint(engine, checkpoint, max_pages=2, pressure_limits=caps,
                              pg_controldata=Path("/fixture/pg_controldata"), **loading)
    assert stopped["last_processed_ordinal"] == 0
    assert stopped["pages_processed"] == 1 and stopped["deferred_reason"] == "test pressure after inserting the second page"
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT count(*) FROM market.cold_fact_pages")) == 1
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


def test_supervised_load_admission_checks_native_paths_and_restores_transaction_limits(prepared_database, monkeypatch):
    engine, tablespace, history_root = prepared_database
    if "QT_COLD_LOAD_FIXTURE_HISTORY_ROOT" not in os.environ:
        pytest.skip("requires an isolated second filesystem for the supervised loader")
    server_root = Path(engine.url.query["host"]).parent
    assert server_root.stat().st_dev != history_root.stat().st_dev
    udev = server_root / "fixture-udev"
    udev.mkdir()
    for path, uuid in ((server_root, "fixture-recent"), (history_root, "fixture-history")):
        device = path.stat().st_dev
        (udev / f"b{os.major(device)}:{os.minor(device)}").write_text(f"E:ID_FS_UUID={uuid}\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    with engine.begin() as conn:
        StorageTargetRecord.__table__.create(conn)
        StorageHeaderTablespaceRecord.__table__.create(conn)
        for key, path, medium, role in (("fixture-recent", server_root, "ssd", "recent"),
                                        ("fixture-history", history_root, "hdd", "history")):
            conn.execute(StorageTargetRecord.__table__.insert(), {
                "id": key, "label": key, "filesystem_uuid": key, "root": str(path), "medium": medium, "roles": [role],
            })
        conn.execute(text("""
            INSERT INTO public.portal_storage_header_tablespaces(database_identity,target_id,tablespace_oid,binding)
            SELECT control.system_identifier::text || '/' || d.oid::text,'fixture-history',s.oid,'{}'
            FROM pg_control_system() control CROSS JOIN pg_database d CROSS JOIN pg_tablespace s
            WHERE d.datname=current_database() AND s.spcname=:space
        """), {"space": tablespace})
    args = {"pg_controldata": Path("/usr/lib/postgresql/15/bin/pg_controldata"), "deadline": monotonic() + 20}
    limits = CatalogLoadLimits("fixture-recent", "fixture-history", 1024, 1024, 32 << 20, 4 << 20, 4 << 20,
                               1 << 30, 8, 64)
    with Session(engine) as session:
        original = session.scalar(text("SHOW temp_file_limit"))
        with supervise_page(session, limits=limits, **args):
            assert session.scalar(text("SHOW temp_file_limit")) == "64kB"
            assert session.scalar(text("SHOW max_parallel_workers_per_gather")) == "0"
        session.rollback()
        assert session.scalar(text("SHOW temp_file_limit")) == original
        with pytest.raises(LoadPressure, match="page_capacity: target=fixture-history"):
            with supervise_page(session, limits=replace(limits, min_history_free_bytes=2**63 - 1), **args):
                pytest.fail("admitted work without filesystem headroom")
