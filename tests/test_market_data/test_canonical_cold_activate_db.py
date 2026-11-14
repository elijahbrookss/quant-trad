from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db import InstrumentRecord
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION, COLD_IMMUTABILITY_TRIGGER, COLD_TABLE_NAMES
from portal.backend.db.fact_identity_schema import assert_fact_identity_references, fact_header_partition_name
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, fact_partition_name
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from scripts.db import canonical_cold_activate as activation
from scripts.db.canonical_cold_audit import audit_candidate
from scripts.db.canonical_cold_identity_rebuild import PREVIOUS, SOURCE, STAGED, prepare_identity_suffix
from scripts.db.canonical_cold_load import TargetDatabase, load_checkpoint
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage

pytestmark = pytest.mark.db


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
    conn.exec_driver_sql("CREATE SCHEMA qt_fact_header_cutover_v2")
    conn.exec_driver_sql("""
        CREATE TABLE qt_fact_header_cutover_v2.fact_versions (LIKE market.fact_versions)
        PARTITION BY RANGE (storage_day)
    """)
    conn.exec_driver_sql("""
        ALTER TABLE qt_fact_header_cutover_v2.fact_versions
        ADD CONSTRAINT fk_market_fact_identity_day FOREIGN KEY (id,storage_day)
        REFERENCES market.fact_identities(id,storage_day) ON DELETE RESTRICT
    """)
    conn.exec_driver_sql("""
        CREATE TRIGGER trg_retained_source_closed BEFORE INSERT ON qt_fact_header_cutover_v2.fact_versions
        FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
    """)
    conn.exec_driver_sql("""
        ALTER TABLE qt_fact_header_cutover_v2.fact_versions ENABLE ALWAYS TRIGGER trg_retained_source_closed
    """)


def test_exact_activation_retires_legacy_and_daily_headers_and_preserves_frozen_reads(storage, tmp_path, monkeypatch):
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
    # The release caller supplies digests for independent recovery, provider
    # handoff and tested rollback. Synthetic digests here exercise the record
    # boundary and carry no qualification for any of those external gates.
    execution = {
        "schema": activation.ACTIVATION_VERSION, "evidence": {gate: "e" * 64 for gate in activation._PHYSICAL_GATES},
        "target_database": plan["database"], "source_database": plan["source_database"],
        "audit_set_hash": audit["audit_set_hash"], "source_commit_seq": latest_commit,
        "payload_children": payload_children,
    }
    expected_hash = archive_evidence_hash(execution)
    with engine.connect() as conn:
        verified = activation.verify_mounted_candidate(conn, audit=audit, store=new_store, limits=ArchiveVerificationLimits())

    path = new_store.local_path(next(iter(verified.objects)))
    original_bytes = path.read_bytes()
    path.write_bytes(original_bytes + b"changed")
    transition = {"execution": execution, "expected_execution_hash": expected_hash, "receipts": receipts}
    with pytest.raises(RuntimeError, match="archive_verification_object_changed"):
        with engine.begin() as conn:
            activation.activate_cold_storage(conn, verified_objects=verified, **transition)
    path.write_bytes(original_bytes)
    with engine.connect() as conn:
        verified = activation.verify_mounted_candidate(conn, audit=audit, store=new_store, limits=ArchiveVerificationLimits())

    with pytest.raises(RuntimeError, match="fixture interrupted after switch"):
        with engine.begin() as conn:
            activation.activate_cold_storage(conn, verified_objects=verified, **transition)
            raise RuntimeError("fixture interrupted after switch")
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == FACT_STORAGE_LAYOUT_VERSION
        assert conn.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == identity["old_oid"]
        assert conn.scalar(text(f"SELECT '{STAGED}'::regclass::oid")) == identity["new_oid"]
        assert conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NOT NULL"))
        assert_fact_identity_references(conn)
        assert conn.scalar(text("SELECT count(*) FROM market.cold_fact_retired_days")) == 0

    with engine.begin() as conn:
        switched = activation.activate_cold_storage(conn, verified_objects=verified, **transition)
        assert switched["cold_before_day"] == storage.today.isoformat()
        assert switched["verified_rows"] == 2
        assert switched["retired_header_bytes"] > 0 and switched["retired_payload_bytes"] > 0
        assert conn.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == identity["new_oid"]
        assert conn.scalar(text(f"SELECT '{PREVIOUS}'::regclass::oid")) == identity["old_oid"]
        assert "qt_fact_header_cutover_v2.fact_versions" in switched["identity"]["rebound_foreign_keys"]
        assert conn.scalar(text(f"SELECT count(*) FROM {SOURCE}")) == 1
        assert conn.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == COLD_HISTORY_LAYOUT_VERSION
        assert conn.execute(text("""
            SELECT storage_day,state FROM market.fact_retention_partitions WHERE storage_day<:cutoff ORDER BY storage_day
        """), {"cutoff": storage.today}).all() == [(legacy_day, "sealed"), (daily_day, "sealed")]
        for relation in ("fact_versions_legacy", fact_header_partition_name(daily_day), *payload_children):
            assert conn.scalar(text("SELECT to_regclass(:name) IS NULL"), {"name": f"market.{relation}"})

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
        from portal.backend.service.storage.history_maintenance import _prepare as prepare_header_move

        assert prepare_header_move(restarted, pg_controldata=None, limits={}, cancelled=None) == {
            "state": "disabled", "reason": "canonical_cold_authority",
        }
        retention = PostgresCanonicalFactRetentionRepository(database=restarted).plan(
            policy=CanonicalFactRetentionPolicy(), storage_root=new_store.root,
        )
        assert {item["storage_day"] for item in retention["actions"]} == {
            storage.today.isoformat(), (storage.today + timedelta(days=1)).isoformat(),
        }
    finally:
        restarted._reset_engine()
