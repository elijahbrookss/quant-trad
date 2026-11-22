from contextlib import ExitStack, contextmanager
from dataclasses import asdict, replace
from datetime import timedelta
from functools import partial
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.pool import NullPool

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from core.storage_mounts import FilesystemEvidence
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db import InstrumentRecord
from portal.backend.db.cold_history_models import (
    COLD_CUSTODY_TABLE_NAMES, COLD_HISTORY_LAYOUT_VERSION, COLD_IMMUTABILITY_TRIGGER, COLD_TABLE_NAMES,
)
from portal.backend.db.fact_identity_schema import assert_fact_identity_references, fact_header_partition_name
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, fact_partition_name
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from scripts.automation import canonical_cold_execute
from scripts.automation import canonical_cold_capacity_supervision as capacity_supervision
from scripts.automation.canonical_cold_recovery_checks import _ApprovedRecoveryChecks
from scripts.db import canonical_cold_activate as activation
from scripts.db import canonical_cold_transition as transition_runner
from scripts.db import fact_header_v2_online_proof as online
from scripts.db.canonical_cold_audit import audit_candidate
from scripts.db.canonical_cold_commit_outcome import observe_activation_transaction, prepare_activation_transaction
from scripts.db.canonical_cold_custody_placement import inspect_custody_placement
from scripts.db.canonical_cold_database_fence import _database, close_database_fence, reopen_database_fence, retain_read_session
from scripts.db.canonical_cold_identity_rebuild import PREVIOUS, SOURCE, STAGED, prepare_identity_suffix
from scripts.db.canonical_cold_load import TargetDatabase, load_checkpoint
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from scripts.db.canonical_cold_raw_index import TABLE, inspect_raw_mapping_index_member
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_transition import run_fenced_transition
from scripts.db.canonical_cold_witness import measure_witnesses
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage

pytestmark = pytest.mark.db


@contextmanager
def _fence_connections(storage):
    engine = storage.database._engine
    control = create_engine(engine.url.set(database="postgres"), poolclass=NullPool)
    engine.dispose()
    try:
        with engine.connect() as conn, control.connect() as admin:
            yield conn, admin
    finally:
        # Exact disposable test database only. Allow test teardown even when an
        # assertion fails while the production-shaped SQL gate is closed.
        with control.begin() as admin:
            quoted = admin.dialect.identifier_preparer.quote_identifier(engine.url.database)
            admin.exec_driver_sql(f"ALTER DATABASE {quoted} ALLOW_CONNECTIONS true")
        control.dispose()


def test_retained_reader_requires_native_transactions_and_writer_role(storage):
    engine = storage.database._engine
    autocommit = create_engine(engine.url, poolclass=NullPool, isolation_level="AUTOCOMMIT",
                               connect_args={"options": "-c default_transaction_read_only=on"})
    try:
        with _fence_connections(storage) as (writer, control), autocommit.connect() as auto, engine.connect() as wrong_role:
            with writer.begin():
                database = _database(writer, engine.url.database)
            with ExitStack() as stack:
                with pytest.raises(ValueError, match="fence_invalid_reader"):
                    retain_read_session(auto, writer=writer, control=control, expected_database=database, stack=stack)
            for target, admin in ((auto, control), (writer, auto)):
                with pytest.raises(ValueError, match="fence_invalid_control"):
                    close_database_fence(target, admin, expected_database=database, timeout_seconds=30,
                                         record=lambda *args: pytest.fail("autocommit must fail before the fence"))
            with wrong_role.begin():
                wrong_role.exec_driver_sql("SET ROLE pg_read_all_data")
            with ExitStack() as stack:
                with pytest.raises(RuntimeError, match="fence_reader_role_changed"):
                    retain_read_session(wrong_role, writer=writer, control=control, expected_database=database, stack=stack)
    finally:
        autocommit.dispose()


@pytest.mark.parametrize("committed", (False, True))
def test_native_control_observes_lost_activation_connection_behind_closed_gate(storage, committed):
    with _fence_connections(storage) as (conn, control):
        with conn.begin():
            database = _database(conn, storage.database._engine.url.database)
        fence = close_database_fence(
            conn, control, expected_database=database, timeout_seconds=30, record=lambda *args: None,
        )
        transaction = conn.begin()
        identity = prepare_activation_transaction(conn, fence)
        conn.exec_driver_sql("CREATE TABLE market.fixture_pending_cutover (value integer PRIMARY KEY)")
        conn.exec_driver_sql("INSERT INTO market.fixture_pending_cutover VALUES (1)")
        mark = conn.scalar(text("SELECT pg_current_wal_insert_lsn()::text")) if committed else None
        arguments = {"database": fence.database, "backend_pid": fence.backend_pid, "transaction": identity,
                     "precommit": mark}
        active = observe_activation_transaction(control, **arguments)
        assert active["transaction_status"] == "in progress" and active["source_backend_present"]
        if committed:
            transaction.commit()
        # Invalidating the checked-out connection closes the actual backend.
        # When it still owns a write transaction, the server must abort it.
        conn.invalidate()
        conn.close()
        with pytest.raises(OperationalError):
            storage.database._engine.connect()
        result = observe_activation_transaction(control, **arguments)
        assert result["transaction_status"] == ("committed" if committed else "aborted")
        assert not result["source_backend_present"]
        if committed and identity["fsync"] == "on":
            assert result["commit_flushed"]
        with pytest.raises(RuntimeError, match="transaction_source_changed"):
            observe_activation_transaction(control, **{
                **arguments, "database": {**fence.database, "postmaster_started_at": "unknown"},
            })
        if committed:
            with pytest.raises(RuntimeError, match="missing_precommit_checks"):
                observe_activation_transaction(control, **{**arguments, "precommit": None})


def _configure_fixture_catalog(conn, database_identity, root):
    # The disposable default tablespace stands in for the registered history
    # device. Validate placement and all immutable relations without depending
    # on a client-local path being present in the PostgreSQL server namespace.
    conn.execute(text("""
        INSERT INTO public.portal_storage_targets(id,label,filesystem_uuid,root,medium,roles)
        VALUES ('activation-fixture','Activation fixture','fixture-device',:root,'hdd','["history"]')
    """), {"root": str(root)})
    conn.execute(text("""
        INSERT INTO public.portal_storage_header_tablespaces(database_identity,target_id,tablespace_oid,binding)
        SELECT :id,'activation-fixture',dattablespace,'{}'
        FROM pg_database WHERE datname=current_database()
    """), {"id": database_identity})
    for table in COLD_TABLE_NAMES:
        conn.execute(text(f"""
            CREATE TRIGGER {COLD_IMMUTABILITY_TRIGGER} BEFORE UPDATE OR DELETE OR TRUNCATE
            ON market.{table} FOR EACH STATEMENT EXECUTE FUNCTION market.reject_immutable_mutation()
        """))
        conn.execute(text(f"ALTER TABLE market.{table} ENABLE ALWAYS TRIGGER {COLD_IMMUTABILITY_TRIGGER}"))


def _stub_raw_index_case(monkeypatch):
    # Single-tablespace Fact parity fixtures exercise the rest of the handoff.
    monkeypatch.setattr(activation, "move_raw_mapping_index", lambda conn, **kwargs: None)
    monkeypatch.setattr(activation, "verify_committed_raw_mapping_index", lambda conn, **kwargs: None)
    monkeypatch.setattr(activation, "assert_raw_mapping_index_placement", lambda conn, **kwargs: None)
    monkeypatch.setattr(transition_runner, "assert_raw_mapping_index_placement", lambda conn, **kwargs: None)
    monkeypatch.setattr(canonical_cold_execute, "_raw_index_headroom",
                        lambda conn, scope: {"history_filesystem_uuid": "fixture-history", "raw_index_bytes": 8192,
                                             "database_directory": "/fixture/db", "history_directory": "/fixture/archive"})
    return {"target_id": "fixture-raw-history", "tablespace_oid": 50, "table_oid": 90, "index_oid": 91,
            "max_copy_bytes": 1024**2, "timeout_seconds": 15}


@pytest.fixture
def native_raw_index_target(storage):
    path = os.environ.get("QT_TRANSITION_RAW_INDEX_TABLESPACE_DIRECTORY")
    if not path:
        pytest.skip("requires the native raw index tablespace fixture directory")
    engine = storage.database._engine
    name = "qt_transition_raw_" + uuid4().hex[:12]
    admin = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin.connect() as conn:
            conn.execute(text(f"CREATE TABLESPACE {name} LOCATION :path"), {"path": path})
        with engine.begin() as conn:
            # Reproduce the deployed split with the raw heap already on HDD.
            conn.exec_driver_sql(f"ALTER TABLE {TABLE} SET TABLESPACE {name}")
            member = inspect_raw_mapping_index_member(conn)
            conn.execute(text("""
                INSERT INTO public.portal_storage_targets(id,label,filesystem_uuid,root,medium,roles)
                VALUES ('fixture-raw-history','Raw history fixture','fixture-raw-device',:root,'hdd','["history"]')
            """), {"root": path})
            conn.execute(text("""
                INSERT INTO public.portal_storage_header_tablespaces (database_identity,target_id,tablespace_oid,binding)
                SELECT control.system_identifier::text || '/' || db.oid::text,'fixture-raw-history',:space,'{}'
                FROM pg_control_system() control CROSS JOIN pg_database db WHERE db.datname=current_database()
            """), {"space": member["table_tablespace_oid"]})
        yield {"target_id": "fixture-raw-history", "tablespace_oid": member["table_tablespace_oid"],
               "table_oid": member["table_oid"], "index_oid": member["index_oid"],
               "max_copy_bytes": 1024**2, "timeout_seconds": 15}
    finally:
        try:
            with engine.begin() as conn:
                observed = inspect_raw_mapping_index_member(conn)
                if observed["tablespace_oid"] != 1663:
                    conn.exec_driver_sql("ALTER INDEX market.pk_market_raw_archive_record_mapping SET TABLESPACE pg_default")
                if observed["table_tablespace_oid"] != 1663:
                    conn.exec_driver_sql(f"ALTER TABLE {TABLE} SET TABLESPACE pg_default")
                for table in COLD_CUSTODY_TABLE_NAMES:
                    conn.exec_driver_sql(f"ALTER TABLE market.{table} SET TABLESPACE pg_default")
                    indexes = conn.scalars(text("""
                        SELECT indexrelid::regclass::text FROM pg_index
                        WHERE indrelid=CAST(:table AS regclass)
                    """), {"table": f"market.{table}"}).all()
                    for index in indexes:
                        conn.exec_driver_sql(f"ALTER INDEX {index} SET TABLESPACE pg_default")
            engine.dispose()
            with admin.connect() as conn:
                conn.exec_driver_sql(f"DROP TABLESPACE IF EXISTS {name}")
        finally:
            admin.dispose()


def _attach_legacy_child(conn, legacy_day, end_day):
    source = fact_header_partition_name(legacy_day)
    # Use the actual former daily child so its heap, indexes and FK shape
    # are preserved. The app-owned daily check is removed when its bound widens.
    conn.execute(text(f"ALTER TABLE market.fact_versions DETACH PARTITION market.{source}"))
    conn.execute(text(f"ALTER TABLE market.{source} DROP CONSTRAINT {source}_day"))
    conn.execute(text(f"ALTER TABLE market.{source} RENAME TO fact_versions_legacy"))
    conn.execute(text(f"""
        ALTER TABLE market.fact_versions ATTACH PARTITION market.fact_versions_legacy
        FOR VALUES FROM (MINVALUE) TO ('{end_day.isoformat()}')
    """))
    for trigger in ("trg_guard_fact_header_partitions", "trg_reject_mutation_fact_header_partitions"):
        conn.execute(text(f"ALTER TABLE market.fact_header_partitions DISABLE TRIGGER {trigger}"))
    conn.execute(text("DELETE FROM market.fact_header_partitions WHERE storage_day=:day"), {"day": legacy_day})
    conn.execute(text("ALTER TABLE market.fact_header_partitions ENABLE ALWAYS TRIGGER trg_guard_fact_header_partitions"))
    conn.execute(text("ALTER TABLE market.fact_header_partitions ENABLE TRIGGER trg_reject_mutation_fact_header_partitions"))
    conn.execute(text("""
        INSERT INTO market.fact_header_legacy(id,end_day,relation_oid)
        VALUES (1,:day,'market.fact_versions_legacy'::regclass::oid::bigint)
    """), {"day": end_day})
    for trigger, events, scope in (
        ("trg_seal_fact_versions_legacy", "INSERT OR UPDATE OR DELETE", "ROW"),
        ("trg_seal_fact_versions_legacy_truncate", "TRUNCATE", "STATEMENT"),
    ):
        conn.execute(text(f"""
            CREATE TRIGGER {trigger} BEFORE {events} ON market.fact_versions_legacy
            FOR EACH {scope} EXECUTE FUNCTION market.reject_fact_header_legacy_mutation()
        """))
        conn.execute(text(f"ALTER TABLE market.fact_versions_legacy ENABLE ALWAYS TRIGGER {trigger}"))


def _create_retained_parent(conn):
    retained = f"{online.SCHEMA}.fact_versions"
    conn.exec_driver_sql(f"CREATE SCHEMA {online.SCHEMA}")
    conn.exec_driver_sql(f"""
        CREATE TABLE {retained} (LIKE market.fact_versions)
        PARTITION BY RANGE (storage_day)
    """)
    conn.exec_driver_sql(f"""
        ALTER TABLE {retained}
        ADD CONSTRAINT fk_market_fact_identity_day FOREIGN KEY (id,storage_day)
        REFERENCES market.fact_identities(id,storage_day) ON DELETE RESTRICT
    """)
    legacy_oid, raw_oid = conn.execute(text(f"""
        SELECT 'market.fact_versions_legacy'::regclass::oid, '{online.raw.SOURCE}'::regclass::oid
    """)).one()
    binding = {
        "source_oids": {online.headers.SOURCE: legacy_oid, online.raw.SOURCE: raw_oid},
        "targets": {"fact_versions": online.headers._shape(conn, "fact_versions")},
    }
    bodies = online._bodies(binding)
    for role in ("header", "truncate"):
        function = f"{online.SCHEMA}.online_guard_{role}()"
        conn.execute(text(f"CREATE FUNCTION {function} RETURNS trigger "
                          "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $qt$"
                          + bodies[role] + "$qt$"))
        conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {function} FROM PUBLIC")
    conn.exec_driver_sql(f"""
        CREATE TRIGGER {online.ROW_TRIGGER} BEFORE INSERT OR UPDATE OR DELETE ON {retained}
        FOR EACH ROW EXECUTE FUNCTION {online.SCHEMA}.online_guard_header()
    """)
    conn.exec_driver_sql(f"ALTER TABLE {retained} ENABLE ALWAYS TRIGGER {online.ROW_TRIGGER}")
    online._install_truncate(conn, retained)
    conn.exec_driver_sql(f"""
        CREATE TABLE {online.STATE}(id integer PRIMARY KEY CHECK(id=1), binding jsonb NOT NULL,
                                    leaf_bindings jsonb NOT NULL DEFAULT '{{}}'::jsonb)
    """)
    conn.execute(text(f"INSERT INTO {online.STATE}(id,binding) VALUES (1,CAST(:binding AS jsonb))"),
                 {"binding": json.dumps(binding)})


@pytest.mark.parametrize("owner_path", [False, True])
def test_exact_activation_retires_legacy_and_daily_headers_and_preserves_frozen_reads(
    storage, tmp_path, monkeypatch, owner_path,
):
    _exercise_exact_activation(storage, tmp_path, monkeypatch, owner_path=owner_path)


@pytest.mark.parametrize(("lose_commit_reply", "retain_reader"), ((False, False), (True, False), (True, True)))
def test_fenced_activation_moves_native_raw_index(storage, tmp_path, monkeypatch, native_raw_index_target,
                                                lose_commit_reply, retain_reader):
    _exercise_exact_activation(storage, tmp_path, monkeypatch, owner_path=True, native_raw_index=native_raw_index_target,
                               lose_commit_reply=lose_commit_reply, retain_reader=retain_reader)


@pytest.mark.parametrize("storage", ["benchmark"], indirect=True)
def test_private_activation_rehearses_real_shared_hdd_transaction(storage, tmp_path, monkeypatch, native_raw_index_target):
    _exercise_exact_activation(storage, tmp_path, monkeypatch, owner_path=True, native_raw_index=native_raw_index_target,
                               private=True)


def _exercise_exact_activation(storage, tmp_path, monkeypatch, *, owner_path, native_raw_index=None, lose_commit_reply=False,
                               retain_reader=False, private=False):
    native = native_raw_index is not None
    raw_scope = native_raw_index if native else _stub_raw_index_case(monkeypatch)
    engine = storage.database._engine
    legacy_day, daily_day = storage.today - timedelta(days=2), storage.today - timedelta(days=1)
    with storage.database.session() as session:
        session.add(InstrumentRecord(
            id="activation-retained", datasource="TEST", exchange="ISOLATED", symbol="ETH-TEST",
            instrument_type="spot", can_short=False, short_requires_borrow=False,
            has_funding=False, extra_metadata={},
        ))
    retained_series = storage.repo.register_series(
        instrument_id="activation-retained", fact_type=storage.fact.fact_type,
        timeframe_seconds=None, contract_version=storage.fact.payload_schema_id,
    )
    _placement(monkeypatch, storage.today)
    storage.repo.ingest_facts(series_id=retained_series, source_id=storage.source_id, facts=[storage.fact])
    _placement(monkeypatch, legacy_day)
    _ingest(storage)
    start, end = BASE - timedelta(seconds=1), BASE + timedelta(seconds=1)
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(storage.series_id, start, end)])
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    assert len(before) == 1
    _placement(monkeypatch, daily_day)
    correction = replace(storage.fact, payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"})
    latest_commit = _ingest(storage, correction).max_commit_seq
    assert latest_commit > frozen.max_commit_seq

    old_store = FilesystemRawArchiveObjectStore(tmp_path / "source")
    new_store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=old_store, temporary_directory=tmp_path / "source-staging",
    )
    for day in (legacy_day, daily_day):
        archive.seal_partition(day)
        assert archive.stage_next_page(day)["status"] == "page_acknowledged"
    if private:
        # The actual private runner seeds the existing immutable objects before
        # staging cold pages. Use a distinct fixture copy of that source tree.
        shutil.copytree(old_store.root, new_store.root, dirs_exist_ok=True)

    with engine.begin() as conn:
        _attach_legacy_child(conn, legacy_day, daily_day)
        _create_retained_parent(conn)
        cluster, name, oid = conn.execute(text("""
            SELECT control.system_identifier::text,database.datname,database.oid::bigint
            FROM pg_control_system() AS control CROSS JOIN pg_database AS database
            WHERE database.datname=current_database()
        """)).one()
        database_identity = f"{cluster}/{oid}"
        _configure_fixture_catalog(conn, database_identity, new_store.root)
        custody_target = raw_scope["target_id"] if native else "activation-fixture"
        custody_before = inspect_custody_placement(conn, expected_database=database_identity, target_id=custody_target)
        custody_tablespace = custody_before["target"]["tablespace_oid"]
        custody_oids = custody_before["relation_oids"]
        if native:
            assert custody_tablespace == raw_scope["tablespace_oid"]
            assert {item["tablespace_oid"] for item in custody_before["members"]} == {1663}
        payload_children = {
            fact_partition_name(day): conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                                  {"name": f"market.{fact_partition_name(day)}"})
            for day in (legacy_day, daily_day)
        }
    target = TargetDatabase(cluster, name, oid)
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
        identity = prepare_identity_suffix(conn, expected_database=database_identity, cutoff=storage.today)
        assert identity["retained_rows"] == 1

    source = PostgresMigrationSource(engine, expected_system_identifier=cluster)
    checkpoint = MigrationCheckpoint(tmp_path / "checkpoint.sqlite")
    for day in (legacy_day, daily_day):
        assert stage(source, checkpoint, day=day, source_store=old_store, destination_store=new_store,
                     temporary_directory=tmp_path / "cold-staging")["cohort_complete"]
        assert load_checkpoint(engine, checkpoint, target=target, object_store=new_store,
                               day=day)["checkpoint_end_reached"]
    scope = {"target": target, "object_store": new_store, "cutoff": storage.today}
    plan = audit_candidate(engine, checkpoint, **scope)
    receipts = [plan, *(audit_candidate(engine, checkpoint, day=day, **scope) for day in (legacy_day, daily_day)),
                audit_candidate(engine, checkpoint, inventory=True, max_pages=4, **scope)]
    audit = validate_audit_receipts(receipts, target_identity=plan["database"], source_identity=plan["source_database"])
    if not owner_path:
        from scripts.db.canonical_cold_source_authority import verify_source_authority_preflight

        preflight = verify_source_authority_preflight(
            engine, audit=audit, expected_audit_hash=audit["audit_set_hash"], max_seconds=10,
        )
        assert (preflight["published_pages"], preflight["published_rows"], preflight["prepared_pages"]) == (2, 2, 2)
        assert preflight["source_authority_inventory_hash"] == audit["source_authority_inventory_hash"]
        assert 0 < preflight["inventory_seconds"] < preflight["elapsed_seconds"] <= 10
        print("native_source_authority_preflight", preflight["inventory_seconds"], preflight["elapsed_seconds"])
        changed = {key: value for key, value in audit.items() if key != "audit_set_hash"}
        changed["source_authority_inventory_hash"] = "0" * 64
        changed["audit_set_hash"] = archive_evidence_hash(changed)
        with pytest.raises(RuntimeError, match="source_authority_inventory_changed"):
            verify_source_authority_preflight(engine, audit=changed, expected_audit_hash=changed["audit_set_hash"],
                                              max_seconds=10)
        changed = {key: value for key, value in audit.items() if key != "audit_set_hash"}
        changed["verified_pages"] = 1
        changed["audit_set_hash"] = archive_evidence_hash(changed)
        with pytest.raises(RuntimeError, match="exceeds_audited_pages"):
            verify_source_authority_preflight(engine, audit=changed, expected_audit_hash=changed["audit_set_hash"],
                                              max_seconds=10)
        checks = 0

        def early_deadline():
            nonlocal checks
            checks += 1
            if checks == 5:
                raise TimeoutError("fixture budget expired inside page proof")

        with pytest.raises(TimeoutError, match="inside page proof"):
            with source.snapshot() as conn:
                activation.verify_source_authority_inventory(conn, storage.today, audit, check_budget=early_deadline)
    if owner_path:
        current = storage.repo.read_facts(series_id=storage.series_id, start=start, end=end)[0]
        retained_fact = storage.repo.read_facts(series_id=retained_series, start=start, end=end)[0]
        time_range = {"series_id": storage.series_id, "start": start.isoformat(), "end": end.isoformat(),
                      "known_at_lte": before[0].fact.known_at.isoformat(), "max_rows": 2}
        cases = [
            {"name": "retained and retired IDs", "mode": "exact", "max_rows": 3,
             "ids": [retained_fact.fact_version_id, before[0].fact_version_id, current.fact_version_id]},
            {"name": "frozen before correction", "mode": "frozen_revisions", **time_range,
             "dataset_id": frozen.dataset_id, "dataset_hash": frozen.dataset_hash},
            {"name": "as-of before correction", "mode": "as_of", **time_range, "as_of_commit_seq": frozen.max_commit_seq},
        ]
        with engine.begin() as conn:
            witness = measure_witnesses(conn, cases, store=old_store, timeout_seconds=15, max_archive_bytes=1024**2)
        assert [case["expected_rows"] for case in witness["cases"]] == [3, 1, 1]
        candidate_witness = json.dumps(witness).encode()
    if private:
        _exercise_private_activation(
            storage, monkeypatch, plan, audit, receipts, identity, custody_before, raw_scope,
            payload_children, candidate_witness, new_store, latest_commit, frozen, before, retained_series,
        )
        return
    # The release caller supplies digests for independent recovery, provider
    # handoff and tested rollback. Synthetic digests here exercise the record
    # boundary and carry no qualification for any of those external gates.
    execution = {
        "schema": activation.ACTIVATION_VERSION, "evidence": {gate: "e" * 64 for gate in activation._PHYSICAL_GATES},
        "approved_package_hash": "a" * 64,
        "target_database": plan["database"], "source_database": plan["source_database"],
        "audit_set_hash": audit["audit_set_hash"], "source_commit_seq": latest_commit,
        "loaded_inventory_hash": audit["loaded_inventory_hash"],
        "payload_children": payload_children,
        "custody_target_id": custody_target, "custody_tablespace_oid": custody_tablespace,
        "custody_relation_oids": custody_oids,
        "raw_mapping_index": raw_scope,
        "identity_registry": {key: identity[key] for key in ("old_oid", "new_oid")},
    }
    if native:
        execution["custody_move"] = {"source_placement_hash": custody_before["placement_hash"],
                                     "max_copy_bytes": 1024**2, "timeout_seconds": 15}
    if owner_path:
        execution["evidence"]["candidate_transaction_witness"] = sha256(candidate_witness).hexdigest()
    outcome_arguments = {"execution": execution, "expected_execution_hash": archive_evidence_hash(execution)}
    if owner_path:
        phases = []
        events = {}
        package = {
            "scope": {key: value for key, value in execution.items() if key not in {
                "schema", "evidence", "source_commit_seq", "approved_package_hash",
            }},
            "source_commit_floor": frozen.max_commit_seq,
            "artifacts": {key: {"sha256": value} for key, value in execution["evidence"].items()},
        }

        def record(phase, evidence):
            phases.append(phase)
            events[phase] = evidence

        def provider_boundary(conn, commit_seq):
            phases.append("provider")
            assert not conn.scalar(text("SELECT datallowconn FROM pg_database WHERE datname=current_database()"))
            assert commit_seq == latest_commit
            return execution["evidence"]["provider_boundary"]

        def recovery_boundary(fence, control, commit_seq, provider, remaining, check_archive):
            phases.append("recovery")
            assert commit_seq == latest_commit and provider == execution["evidence"]["provider_boundary"]
            assert remaining() > 0
            check_archive()
            with control.begin():
                assert control.scalar(text("SELECT current_database()")) == "postgres"
            with fence.connection.begin():
                fence.assert_quiet(fence.connection)
            return execution["evidence"]["independent_recovery"]

        verified_candidate = []

        def verify_candidate(conn, result):
            phases.append("accepted")
            assert activation.read_activation_outcome(conn, **outcome_arguments) == "committed"
            assert result["source_commit_seq"] == latest_commit
            assert conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NULL"))
            if native:
                raw = result["raw_mapping_index"]
                assert raw["copy_bytes"] > 0
                assert raw["before"]["tablespace_oid"] == 1663
                assert raw["after"]["tablespace_oid"] == raw_scope["tablespace_oid"]
                assert raw["after"]["index_oid"] == raw_scope["index_oid"]
                custody = result["custody_move"]
                assert custody["relation_oids"] == custody_oids and custody["copy_bytes"] == custody_before["copy_bytes"] > 0
                assert {item["tablespace_oid"] for item in custody["before"]} == {1663}
                assert {item["tablespace_oid"] for item in custody["after"]} == {custody_tablespace}
            verified_candidate.append(result)

        with _fence_connections(storage) as (retained, control):
            owner = dict(package=package, approved_package_hash=execution["approved_package_hash"],
                         receipts=receipts, store=new_store, limits=ArchiveVerificationLimits(), timeout_seconds=60,
                         assert_host_held=lambda: None, host_remaining=lambda: 60,
                         verify_provider_boundary=provider_boundary, verify_recovery_boundary=recovery_boundary,
                         record=record, verify_candidate=verify_candidate, candidate_witness=candidate_witness)

            def fail_provider_boundary(conn, commit_seq):
                provider_boundary(conn, commit_seq)
                raise RuntimeError("fixture provider proof incomplete")

            with pytest.raises(ValueError, match="canonical_cold_witness_approved_digest_changed"):
                run_fenced_transition(
                    retained, control, **{**owner, "candidate_witness": candidate_witness + b" "},
                )
            assert not phases

            with pytest.raises(RuntimeError, match="fixture provider proof incomplete"):
                run_fenced_transition(
                    retained, control, **{**owner, "verify_provider_boundary": fail_provider_boundary},
                )
            assert phases == ["login_closing", "jobs_stopping", "database_fenced", "provider",
                              "activation_rolled_back", "login_opening", "jobs_restarting", "database_reopened"]
            phases.clear()
            with pytest.raises(ValueError, match="canonical_cold_package_final_boundary_missing"):
                run_fenced_transition(retained, control, **{**owner, "verify_recovery_boundary": lambda *args: None})
            assert phases == ["login_closing", "jobs_stopping", "database_fenced", "provider",
                              "activation_rolled_back", "login_opening", "jobs_restarting", "database_reopened"]
            assert events["activation_rolled_back"] == {"execution_hash": None, "source_commit_seq": latest_commit}
            phases.clear()
            limits = ArchiveVerificationLimits()
            with retained.begin():
                scope = activation.measure_mounted_inventory(retained, audit=audit, store=new_store)
                assert scope["objects"] == 2 and scope["bytes"] > 0
                with pytest.raises(RuntimeError, match="inventory_exceeds_verified_budget"):
                    activation.verify_mounted_candidate(retained, audit=audit, store=new_store,
                                                        limits=ArchiveVerificationLimits(max_bytes=scope["bytes"] - 1))
                verified = activation.verify_mounted_candidate(retained, audit=audit, store=new_store, limits=limits)

            if native:
                stale_scope = {**package["scope"], "custody_move": {
                    **execution["custody_move"], "source_placement_hash": "0" * 64,
                }}
                with pytest.raises(RuntimeError, match="canonical_custody_source_placement_changed"):
                    run_fenced_transition(
                        retained, control, **{**owner, "package": {**package, "scope": stale_scope}},
                        verified_objects=verified,
                    )
                assert phases == []

            changed = {**witness, "cases": [dict(case) for case in witness["cases"]]}
            changed["cases"][1]["expected_hash"] = "f" * 64
            changed_witness = json.dumps(changed).encode()
            rejected_package = {**package, "artifacts": {**package["artifacts"],
                "candidate_transaction_witness": {"sha256": sha256(changed_witness).hexdigest()}}}

            with pytest.raises(RuntimeError, match="canonical_cold_witness_content_changed: frozen before correction"):
                run_fenced_transition(
                    retained, control, **{**owner, "package": rejected_package, "candidate_witness": changed_witness},
                    verified_objects=verified,
                )
            assert phases == ["login_closing", "jobs_stopping", "database_fenced", "provider", "recovery", "activation_committing",
                              "activation_rolled_back", "login_opening", "jobs_restarting", "database_reopened"]
            with retained.begin():
                assert activation.read_activation_outcome(retained, **outcome_arguments) == "original"
                assert retained.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NOT NULL"))
                assert retained.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == identity["old_oid"]
                assert retained.scalar(text(f"SELECT '{STAGED}'::regclass::oid")) == identity["new_oid"]
                if native:
                    assert inspect_raw_mapping_index_member(retained)["tablespace_oid"] == 1663
                    assert activation.assert_original_custody_placement(retained, execution) == custody_before
            phases.clear()

            if native:
                def fail_after_registry_drop(connection, **arguments):
                    finalize_registry(connection, **arguments)
                    assert connection.scalar(text(f"SELECT to_regclass('{PREVIOUS}') IS NULL"))
                    raise RuntimeError("fixture interrupted after identity drop")

                finalize_registry = transition_runner.finalize_identity_suffix
                with monkeypatch.context() as patch:
                    patch.setattr(transition_runner, "finalize_identity_suffix", fail_after_registry_drop)
                    with pytest.raises(RuntimeError, match="fixture interrupted after identity drop"):
                        run_fenced_transition(retained, control, **owner, verified_objects=verified)
                with retained.begin():
                    assert activation.read_activation_outcome(retained, **outcome_arguments) == "original"
                    assert retained.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == identity["old_oid"]
                    assert inspect_raw_mapping_index_member(retained)["tablespace_oid"] == 1663
                    assert activation.assert_original_custody_placement(retained, execution) == custody_before
                phases.clear()
            if retain_reader:
                def block_raw_copy(fence, *args):
                    receipt = recovery_boundary(fence, *args)
                    fence.reader.connection.exec_driver_sql(f"LOCK TABLE ONLY {TABLE} IN ACCESS SHARE MODE")
                    return receipt

                def unexpected_copy(*args, **kwargs):
                    pytest.fail("copy started before admitting every owning-table lock")

                with engine.connect() as standby, monkeypatch.context() as patch:
                    patch.setattr(activation, "move_custody_tablespace", unexpected_copy)
                    patch.setattr(activation, "move_raw_mapping_index", unexpected_copy)
                    with pytest.raises(DBAPIError, match="canceling statement due to lock timeout") as blocked:
                        run_fenced_transition(
                            retained, control, **{**owner, "verify_recovery_boundary": block_raw_copy},
                            verified_objects=verified, read_connection=standby,
                        )
                    assert blocked.value.orig.pgcode == "55P03"
                    assert events["database_reopened"]["outcome"] == "original"
                    assert "activation_precommit_verified" not in phases
                    with retained.begin():
                        assert inspect_raw_mapping_index_member(retained)["tablespace_oid"] == 1663
                        assert activation.assert_original_custody_placement(retained, execution) == custody_before
                phases.clear()

                def lose_writer_before_commit(connection, _result):
                    assert connection is retained
                    with pytest.raises(DBAPIError, match="read-only transaction"):
                        with standby.begin_nested():
                            standby.exec_driver_sql("CREATE TABLE market.unapproved_writer (id int)")
                    retained.invalidate()
                    raise ConnectionError("fixture writer lost before commit")

                with engine.connect() as standby:
                    with pytest.raises(ConnectionError, match="fixture writer lost before commit"):
                        run_fenced_transition(
                            retained, control, **{**owner, "verify_candidate": lose_writer_before_commit},
                            verified_objects=verified, read_connection=standby,
                        )
                    assert events["database_reopened"]["outcome"] == "original"
                    assert events["login_opening"]["backend_pid"] == events["database_fenced"]["retained_reader"]["backend_pid"]
                    with standby.begin():
                        assert activation.read_activation_outcome(standby, **outcome_arguments) == "original"
                with retained.begin():
                    assert activation.read_activation_outcome(retained, **outcome_arguments) == "original"
                phases.clear()
            reopened = []

            @contextmanager
            def disposable_host(*args, before_pause, **kwargs):
                before_pause(lambda: 60)

                def resume(ready, *, verify_before_start=None):
                    if verify_before_start is None:
                        confirmed = ready({})
                        assert confirmed == {"ready": True}
                        reopened.append("original")
                        return confirmed
                    assert verify_before_start({}) == {"ready": True}
                    result = ready({})
                    assert result == {"ready": True, "collector_restart_sha256": "d" * 64}
                    reopened.append(True)
                    return result

                yield package, lambda: None, record, lambda: 60, resume

            def verify_started(activation_result, _services):
                assert activation_result["source_commit_seq"] == latest_commit
                active = reader if retained.invalidated else retained
                with active.begin():
                    assert active.scalar(text("SELECT datallowconn FROM pg_database WHERE datname=current_database()"))
                    assert activation.read_activation_outcome(
                        active, **outcome_arguments, require_identity_retired=True,
                    ) == "committed"
                return {"ready": True}

            def bound_provider(connection, commit_seq, tail):
                assert tail == b"fixture pre-stop spool receipt"
                return provider_boundary(connection, commit_seq)

            def verify_original_ready(_services):
                with retained.begin():
                    assert retained.scalar(text("SELECT datallowconn FROM pg_database WHERE datname=current_database()"))
                    assert activation.read_activation_outcome(retained, **outcome_arguments) == "original"
                return {"ready": True}

            package["max_outage_seconds"] = 60
            with monkeypatch.context() as patch, ExitStack() as connections:
                reader = connections.enter_context(engine.connect()) if retain_reader else None
                physical_samples = []
                refuse_final_capacity = retain_reader
                lose_space_in_transaction = False
                reserve, allowance, available = 2**30, 2**29, 2**31
                devices = {
                    "ssd": FilesystemEvidence("/fixture/ssd", "8:1", "recent-ssd", 2**34, 2**32, available, False),
                    "hdd": FilesystemEvidence("/fixture/hdd", "8:2", "history-hdd", 2**34, 2**32, available, False),
                }

                def observe_device(path, *, expected_uuid, require_writable):
                    assert require_writable is False
                    item = devices[path.name]
                    assert item.filesystem_uuid == expected_uuid
                    return item

                def capacity(*, history_filesystem_uuid, raw_index_bytes, database_directory, history_directory,
                             timeout_seconds, custody_bytes=0):
                    assert history_filesystem_uuid and raw_index_bytes > 0 and timeout_seconds > 0
                    assert database_directory.startswith("/") and history_directory.startswith("/")
                    physical_samples.append(raw_index_bytes)
                    if refuse_final_capacity and len(physical_samples) == 3:
                        raise RuntimeError("fixture free space fell during independent restore")
                    assert custody_bytes == (custody_before["copy_bytes"] if native else 0)
                    receipt = {"raw_index_bytes": raw_index_bytes, "custody_bytes": custody_bytes, "devices": {
                        tier: {**asdict(item), "reserve_bytes": reserve, "temporary_bytes": allowance}
                        for tier, item in devices.items()
                    }}
                    return {**receipt, "receipt_hash": archive_evidence_hash(receipt)}

                def verify_with_pressure(connection, result):
                    verify_candidate(connection, result)
                    if lose_space_in_transaction:
                        devices["ssd"] = replace(devices["ssd"], available_bytes=available - allowance - 1)
                        connection.exec_driver_sql("SELECT pg_sleep(5)")
                        pytest.fail("physical resource watcher did not cancel the running statement")

                def restarted(_services, *, connection=None):
                    assert connection is (reader if retain_reader else None)
                    return "d" * 64

                patch.setattr(canonical_cold_execute, "load_approved_package", lambda *args, **kwargs: package)
                patch.setattr(canonical_cold_execute, "read_approved_candidate_witness", lambda *args: candidate_witness)
                patch.setattr(canonical_cold_execute, "bind_approved_capacity_checks", lambda **kwargs: capacity)
                patch.setattr(capacity_supervision, "inspect_filesystem", observe_device)
                patch.setattr(canonical_cold_execute, "hold_cold_cutover", disposable_host)
                patch.setattr(canonical_cold_execute, "bind_approved_collector_checks",
                              lambda *args, **kwargs: (lambda _: b"fixture pre-stop spool receipt",
                                                       bound_provider, restarted, None))
                patch.setattr(canonical_cold_execute, "bind_approved_recovery_checks",
                              lambda **kwargs: _ApprovedRecoveryChecks(
                                  lambda: "e" * 64, kwargs["verify_site_restore"], lambda *a, **kw: None,
                              ))
                def execute_once():
                    return canonical_cold_execute.execute_approved_cutover(
                        retained, control, state_root=tmp_path, inventory={"services": {"tsdb": {"id": "a" * 64}}},
                        package_path=tmp_path / "fixture.json",
                        approved_package_hash=execution["approved_package_hash"], receipts=receipts, store=new_store, limits=limits,
                        preflight_seconds=60, outage_seconds=60, provider_receipt_root=tmp_path,
                        verify_recovery_boundary=recovery_boundary, verify_candidate=verify_with_pressure,
                        verify_candidate_prestart=verify_started, verify_candidate_ready=verify_started,
                        verify_original_ready=verify_original_ready, read_connection=reader,
                    )

                if refuse_final_capacity:
                    with pytest.raises(RuntimeError, match="free space fell during independent restore"):
                        execute_once()
                    assert len(physical_samples) == 3 and reopened == ["original"]
                    assert "activation_committing" not in phases
                    assert events["database_reopened"]["outcome"] == "original"
                    reopened.clear()
                    physical_samples.clear()
                    phases.clear()
                    refuse_final_capacity = False
                    lose_space_in_transaction = True
                    with pytest.raises(RuntimeError, match="capacity_supervision_failed: storage_move_space_budget_exceeded"):
                        execute_once()
                    assert reopened == ["original"] and events["database_reopened"]["outcome"] == "original"
                    assert "activation_committing" in phases and "activation_precommit_verified" not in phases
                    with retained.begin():
                        assert inspect_raw_mapping_index_member(retained)["tablespace_oid"] == 1663
                    devices["ssd"] = replace(devices["ssd"], available_bytes=available)
                    reopened.clear()
                    physical_samples.clear()
                    phases.clear()
                    lose_space_in_transaction = False

                completed = None
                if lose_commit_reply:
                    actual_commit = engine.dialect.do_commit
                    lost_replies = []

                    def commit_without_reply(connection):
                        actual_commit(connection)
                        if not lost_replies and phases[-1:] == ["activation_precommit_verified"]:
                            lost_replies.append(True)
                            retained.invalidate()
                            raise ConnectionError("fixture activation COMMIT reply lost")

                    with monkeypatch.context() as driver:
                        driver.setattr(engine.dialect, "do_commit", commit_without_reply)
                        if retain_reader:
                            completed = execute_once()
                        else:
                            with pytest.raises(ConnectionError, match="fixture activation COMMIT reply lost"):
                                execute_once()
                    assert lost_replies == [True]
                    if retain_reader:
                        assert events["login_opening"]["backend_pid"] == events["database_fenced"]["retained_reader"]["backend_pid"]
                    else:
                        journal = events["activation_precommit_verified"]
                        source = events["database_fenced"]
                        observed = observe_activation_transaction(
                            control, database={**source["database"], "allow_connections": False}, backend_pid=source["backend_pid"],
                            transaction=events["activation_committing"]["transaction"], precommit=journal["wal_before_commit"],
                        )
                        assert observed["transaction_status"] == "committed" and not observed["source_backend_present"]
                        assert not reopened
                        with pytest.raises(OperationalError, match="not currently accepting connections"):
                            engine.connect()
                else:
                    completed = execute_once()
                if completed is not None:
                    assert reopened == [True] and completed["runtime"] == {"ready": True, "collector_restart_sha256": "d" * 64}
                    result = completed["activation"]
                else:
                    result = verified_candidate[-1]
                assert len(physical_samples) == 3
                assert events["activation_committing"]["physical_capacity"]["raw_index_bytes"] == physical_samples[-1]
                physical = events["activation_precommit_verified"]["physical_supervision"]
                assert physical["capacity_receipt_hash"] == events["activation_committing"]["physical_capacity"]["receipt_hash"]
                assert physical["devices"]["ssd"]["floor_bytes"] == available - allowance
                assert physical["after_wal"]["generated_bytes"] > 0
        expected_phases = ["login_closing", "jobs_stopping", "database_fenced", "provider", "recovery", "activation_committing",
                           "accepted", "activation_precommit_verified"]
        if not lose_commit_reply or retain_reader:
            expected_phases += ["activation_verified", "login_opening", "jobs_restarting", "database_reopened"]
            assert events["activation_precommit_verified"] == events["activation_verified"]
        assert phases == expected_phases
        precommit = events["activation_precommit_verified"]
        assert events["activation_committing"]["execution_hash"] == outcome_arguments["expected_execution_hash"]
        assert events["activation_committing"]["transaction"]["xid"] == precommit["xid"]
        assert precommit["identity_retirement"] == result["identity_retirement"]
        assert result["identity_retirement"]["verified_recovery_digest"] == execution["evidence"]["independent_recovery"]
        with engine.begin() as conn:
            assert conn.scalar(text(f"SELECT to_regclass('{PREVIOUS}') IS NULL"))
            assert activation.read_activation_outcome(conn, **outcome_arguments, require_identity_retired=True) == "committed"
        if native:
            with engine.begin() as conn:
                receipt_hash = conn.scalar(text("SELECT evidence->'raw_mapping_index'->>'receipt_hash' FROM market.fact_storage_state"))
                custody_hash = conn.scalar(text("SELECT evidence->'custody_move'->>'receipt_hash' FROM market.fact_storage_state"))
                observed_custody = inspect_custody_placement(
                    conn, expected_database=database_identity, target_id=custody_target,
                )
            assert precommit["raw_mapping_index_receipt_hash"] == receipt_hash
            assert precommit["custody_move_receipt_hash"] == custody_hash == result["custody_move"]["receipt_hash"]
            assert observed_custody["copy_bytes"] == 0
            assert observed_custody["members"] == result["custody_move"]["after"]
    else:
        _exercise_fence_failures(storage, new_store, audit, outcome_arguments, receipts,
                                 identity, (legacy_day, daily_day))

    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
        monkeypatch.setattr(market_data, "db", restarted)
        monkeypatch.setattr(market_data, "canonical_fact_storage_repository", PostgresCanonicalFactStorageRepository(
            object_store_factory=lambda: new_store, read_cache_factory=lambda: None,
        ))
        reader = market_data.PostgresMarketDataRepository()
        assert reader.current_commit_seq() == latest_commit
        assert reader.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
        assert reader.read_facts(series_id=storage.series_id, start=start, end=end)[0].fact.payload["raw_rate"] == "0.2"
        assert reader.read_facts(series_id=retained_series, start=start, end=end)[0].fact == storage.fact
        retention = PostgresCanonicalFactRetentionRepository(database=restarted).plan(
            policy=CanonicalFactRetentionPolicy(), storage_root=new_store.root,
        )
        assert {item["storage_day"] for item in retention["actions"]} == {
            storage.today.isoformat(), (storage.today + timedelta(days=1)).isoformat(),
        }
    finally:
        restarted._reset_engine()


def _exercise_private_activation(storage, monkeypatch, plan, audit, receipts, identity, custody, raw_scope,
                                 payload_children, witness, store, source_commit, frozen, before, retained_series):
    from scripts.db import canonical_cold_private_activate as private
    from scripts.db import canonical_cold_private_prepare_activation as prepare

    target = plan["database"]
    with storage.database._engine.connect() as conn:
        postmaster_epoch = conn.scalar(text("SELECT extract(epoch FROM pg_postmaster_start_time())::text"))
        workers_disabled = conn.scalar(text("SHOW timescaledb.max_background_workers")) == "0"
    clone = SimpleNamespace(url=storage.database._engine.url, proof={
        "database_identity": f"{target['system_identifier']}/{target['database_oid']}",
        "accepted_commit_seq": source_commit, "tablespace_oid": raw_scope["tablespace_oid"],
        "postmaster_epoch": postmaster_epoch,
    })
    placement = {
        "history_target_id": raw_scope["target_id"], "archive_root": str(store.root.parent),
        "clone_proof_sha256": "a" * 64,
        "native_binding": {"tablespace_oid": raw_scope["tablespace_oid"]},
    }
    source_witness = json.loads(witness)
    cases = [{key: value for key, value in case.items() if key not in ("expected_rows", "expected_hash")}
             for case in source_witness["cases"]]
    monkeypatch.setattr(prepare, "assert_private_placement", lambda conn, **scope: None)
    with storage.database._engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        execution, witness, measured = prepare.measure_private_activation(
            conn, clone=clone, clone_sha256="a" * 64, placement=placement, placement_sha256="b" * 64,
            audit_receipts=receipts, cases=cases, store=store, raw_copy_bytes=raw_scope["max_copy_bytes"],
            custody_copy_bytes=1024**2, copy_timeout_seconds=15, witness_max_bytes=1024**2, witness_timeout_seconds=15,
        )
    assert json.loads(witness) == source_witness
    assert (execution["audit_set_hash"], execution["payload_children"]) == (audit["audit_set_hash"], payload_children)
    assert execution["custody_move"]["source_placement_hash"] == custody["placement_hash"]
    assert (measured["verified_rows"], measured["verified_pages"]) == (2, 2)
    digest = archive_evidence_hash(execution)
    with pytest.raises(ValueError, match="incomplete_execution_record"):
        activation.validate_execution_record(execution, expected_execution_hash=digest)

    def admit_fixture(conn, control, *, clone, placement, store):
        # Mount, socket and promoted-postmaster admission has separate native
        # coverage. Keep the retained login, writer fence and all SQL real.
        assert conn is not control and store.root.name == "objects"
        assert clone.url.database == target["database"] and clone.url.database.startswith("qt_bench_")
        assert placement["history_target_id"] == raw_scope["target_id"]

    monkeypatch.setattr(private, "_assert_private_owner", admit_fixture)

    @contextmanager
    def observe_transaction(conn, remaining, *, interrupt):
        assert conn.in_transaction() and remaining() > 0
        assert conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NOT NULL"))
        yield {"fixture": True}
        assert conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NULL"))
        if interrupt:
            raise RuntimeError("fixture interrupted after authority switch")

    phases = []
    common = dict(clone=clone, placement=placement, execution=execution, expected_execution_hash=digest,
                  receipts=receipts, candidate_witness=witness, store=store, limits=ArchiveVerificationLimits(),
                  timeout_seconds=90, record=lambda phase, evidence: phases.append(phase))
    with _fence_connections(storage) as (conn, control):
        with pytest.raises(ValueError, match="witness"):
            private.run_private_activation(
                conn, control, **(common | {"candidate_witness": witness + b" "}),
                supervise_capacity=partial(observe_transaction, interrupt=False),
            )
        with pytest.raises(RuntimeError, match="fixture interrupted after authority switch"):
            private.run_private_activation(conn, control, **common,
                                           supervise_capacity=partial(observe_transaction, interrupt=True))
        with conn.begin():
            assert conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NOT NULL"))
            assert conn.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == FACT_STORAGE_LAYOUT_VERSION
            assert conn.scalar(text("SELECT count(*) FROM market.cold_fact_retired_days")) == 0
            assert inspect_raw_mapping_index_member(conn)["tablespace_oid"] == 1663
        outcome = private.run_private_activation(conn, control, **common,
                                                 supervise_capacity=partial(observe_transaction, interrupt=False))
    assert outcome["verified_rows"] == 2
    assert outcome["raw_mapping_index"]["copy_bytes"] > 0 and outcome["custody_move"]["copy_bytes"] > 0
    assert outcome["physical_supervision"] == {"fixture": True}
    assert outcome["witness_hash"] == archive_evidence_hash(json.loads(witness))
    assert "approved_package_hash" not in outcome and "independent_recovery" not in outcome
    if workers_disabled:
        assert phases.count("private_jobs_already_disabled") == 2
        assert phases.count("private_jobs_remain_disabled") == 2
        assert "jobs_stopping" not in phases
    with storage.database._engine.begin() as conn:
        certificate = conn.scalar(text("SELECT evidence FROM market.fact_storage_state"))
        assert certificate["rehearsal"]["schema"] == private.SCHEMA
        assert set(certificate["rehearsal"].values()) >= {"a" * 64, "b" * 64, sha256(witness).hexdigest()}
        assert "approved_package_hash" not in certificate and "evidence" not in certificate
        assert conn.scalar(text(f"SELECT '{PREVIOUS}'::regclass::oid")) == identity["old_oid"]
        assert inspect_raw_mapping_index_member(conn)["tablespace_oid"] == raw_scope["tablespace_oid"]

    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
        monkeypatch.setattr(market_data, "db", restarted)
        monkeypatch.setattr(market_data, "canonical_fact_storage_repository", PostgresCanonicalFactStorageRepository(
            object_store_factory=lambda: store, read_cache_factory=lambda: None,
        ))
        reader = market_data.PostgresMarketDataRepository()
        assert reader.current_commit_seq() == source_commit
        assert reader.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
        selection = {"start": BASE - timedelta(seconds=1), "end": BASE + timedelta(seconds=1)}
        assert reader.read_facts(series_id=storage.series_id, **selection)[0].fact.payload["raw_rate"] == "0.2"
        assert len(reader.read_facts(series_id=retained_series, **selection)) == 1
    finally:
        restarted._reset_engine()


def _exercise_fence_failures(storage, new_store, audit, outcome_arguments, receipts, identity, days):
    execution = outcome_arguments["execution"]
    engine = storage.database._engine
    custody_oids = execution["custody_relation_oids"]
    legacy_day, daily_day = days
    with engine.connect() as conn:
        verified = activation.verify_mounted_candidate(conn, audit=audit, store=new_store, limits=ArchiveVerificationLimits())
    path = new_store.local_path(next(iter(verified.objects)))
    original_bytes = path.read_bytes()
    stages = []
    with _fence_connections(storage) as (retained, control):
        fence = close_database_fence(
            retained, control, expected_database=execution["target_database"], timeout_seconds=120,
            record=lambda phase, evidence: stages.append((phase, evidence)),
        )
        assert [stage for stage, _ in stages] == ["login_closing", "jobs_stopping", "database_fenced"]
        with pytest.raises(OperationalError, match="not currently accepting connections"):
            create_engine(engine.url, poolclass=NullPool).connect()

        transition = {**outcome_arguments, "receipts": receipts, "database_fence": fence}
        path.write_bytes(original_bytes + b"changed")
        with pytest.raises(RuntimeError, match="archive_verification_object_changed"):
            with retained.begin():
                activation.activate_cold_storage(retained, verified_objects=verified, **transition)
        path.write_bytes(original_bytes)
        with retained.begin():
            verified = activation.verify_mounted_candidate(
                retained, audit=audit, store=new_store, limits=ArchiveVerificationLimits(),
            )

        changed_oids = dict(custody_oids, fact_archive_dependencies=custody_oids["fact_archive_dependencies"] + 1)
        changed_execution = {**execution, "custody_relation_oids": changed_oids}
        with pytest.raises(RuntimeError, match="canonical_custody_relation_oid_changed"):
            with retained.begin():
                activation.activate_cold_storage(
                    retained, execution=changed_execution, expected_execution_hash=archive_evidence_hash(changed_execution),
                    receipts=receipts, verified_objects=verified, database_fence=fence,
                )

        with pytest.raises(RuntimeError, match="cold_identity_retained_cutover_parent_proof_mismatch"):
            with retained.begin():
                retained.execute(text(f"""
                    UPDATE {online.STATE} SET binding=jsonb_set(binding, ARRAY['source_oids',:source],
                        to_jsonb('market.fact_versions'::regclass::oid::bigint))
                """), {"source": online.headers.SOURCE})
                activation.activate_cold_storage(retained, verified_objects=verified, **transition)

        with pytest.raises(RuntimeError, match="fixture interrupted after switch"):
            with retained.begin():
                activation.activate_cold_storage(retained, verified_objects=verified, **transition)
                raise RuntimeError("fixture interrupted after switch")
        with retained.begin():
            assert activation.read_activation_outcome(retained, **outcome_arguments) == "original"
            assert retained.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == identity["old_oid"]
            assert retained.scalar(text(f"SELECT '{STAGED}'::regclass::oid")) == identity["new_oid"]
            assert retained.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NOT NULL"))
            assert_fact_identity_references(retained)
            assert retained.scalar(text("SELECT count(*) FROM market.cold_fact_retired_days")) == 0

        with retained.begin():
            switched = activation.activate_cold_storage(retained, verified_objects=verified, **transition)
            assert switched["cold_before_day"] == storage.today.isoformat()
            assert switched["custody"] == {
                "target_id": "activation-fixture", "tablespace_oid": execution["custody_tablespace_oid"],
                "relation_oids": custody_oids,
            }
            assert switched["verified_rows"] == 2
            assert switched["retired_header_bytes"] > 0 and switched["retired_payload_bytes"] > 0
            assert retained.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == identity["new_oid"]
            assert retained.scalar(text(f"SELECT '{PREVIOUS}'::regclass::oid")) == identity["old_oid"]
            assert "qt_fact_header_cutover_v2.fact_versions" in switched["identity"]["rebound_foreign_keys"]
            assert retained.scalar(text(f"SELECT count(*) FROM {SOURCE}")) == 1
            assert retained.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == COLD_HISTORY_LAYOUT_VERSION
            assert retained.execute(text("""
                SELECT storage_day,state FROM market.fact_retention_partitions WHERE storage_day<:cutoff ORDER BY storage_day
            """), {"cutoff": storage.today}).all() == [(legacy_day, "sealed"), (daily_day, "sealed")]
            for relation in ("fact_versions_legacy", fact_header_partition_name(daily_day), *execution["payload_children"]):
                assert retained.scalar(text("SELECT to_regclass(:name) IS NULL"), {"name": f"market.{relation}"})

        with pytest.raises(RuntimeError, match="canonical_cold_database_fence_release_outcome_changed"):
            reopen_database_fence(
                fence, control, expected_outcome="original", record=lambda *args: pytest.fail("reopened mismatched outcome"),
                read_outcome=lambda conn: activation.read_activation_outcome(conn, **outcome_arguments),
            )
        reopen_database_fence(
            fence, control, expected_outcome="committed", record=lambda phase, evidence: stages.append((phase, evidence)),
            read_outcome=lambda conn: activation.read_activation_outcome(conn, **outcome_arguments),
        )
        assert [stage for stage, _ in stages[3:]] == ["login_opening", "jobs_restarting", "database_reopened"]
