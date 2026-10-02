"""Native retained-header contract, real readers and fail-loud drift checks.

Tiny owned fixtures only; this is not a forward-cutover operator.
"""
from dataclasses import replace
from datetime import timedelta
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, BASE
from tests.test_market_data.test_fact_header_copy_db import source, _headers, _frozen_records
from portal.backend.db.market_data_models import (MarketFactIdentityRecord, MarketFactHeaderPartitionRecord, MarketFactHeaderLegacyRecord, MarketFactHeaderSeriesDayRecord, MarketFactVersionRecord)
from portal.backend.db.fact_storage_schema import install_fact_storage_functions
from portal.backend.db.fact_identity_schema import assert_fact_identity_contract, assert_fact_identity_references
from portal.backend.db.fact_storage_schema import assert_fact_storage_contract
from portal.backend.db.session import Database
from portal.backend.service.storage.header_catalog import read_header_catalog, read_locked_header_group
from portal.backend.db.fact_identity_schema import ensure_fact_header_partition

pytestmark=pytest.mark.db

def stage(conn, cutoff):
    assert conn.scalar(text('SELECT current_database()')).startswith('qt_migration_')
    assert conn.scalar(text('SELECT count(*) FROM market.fact_versions'))<=32
    refs=conn.execute(text("SELECT conrelid::regclass::text AS relation,conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE contype='f' AND confrelid='market.fact_versions'::regclass AND conparentid=0 ORDER BY conrelid,conname")).mappings().all()
    assert len(refs)==3
    MarketFactIdentityRecord.__table__.create(conn)
    # Tiny fixture setup, not an approved full-size identity backfill.
    conn.exec_driver_sql('INSERT INTO market.fact_identities SELECT id,storage_day,series_id,observation_key,revision FROM market.fact_versions')
    for r in refs:
        q=conn.dialect.identifier_preparer.quote(r['conname'])
        conn.exec_driver_sql(f"ALTER TABLE {r['relation']} DROP CONSTRAINT {q}")
        conn.exec_driver_sql(f"ALTER TABLE {r['relation']} ADD CONSTRAINT {q} "+r['definition'].replace('market.fact_versions','market.fact_identities'))
    conn.exec_driver_sql('DROP VIEW market.fact_rows')
    conn.exec_driver_sql('DROP TRIGGER trg_assert_fact_hot_payload_valid ON market.fact_hot_payloads')
    triggers=conn.execute(text("SELECT tgname FROM pg_trigger WHERE tgrelid='market.fact_versions'::regclass AND NOT tgisinternal")).scalars().all()
    for n in triggers:
        conn.exec_driver_sql('DROP TRIGGER '+conn.dialect.identifier_preparer.quote(n)+' ON market.fact_versions')
    conn.exec_driver_sql('CREATE SCHEMA qt_legacy_probe')
    conn.exec_driver_sql('ALTER TABLE market.fact_versions SET SCHEMA qt_legacy_probe')
    pk=conn.scalar(text("SELECT conname FROM pg_constraint WHERE conrelid='qt_legacy_probe.fact_versions'::regclass AND contype='p'"))
    conn.exec_driver_sql('ALTER TABLE qt_legacy_probe.fact_versions DROP CONSTRAINT '+conn.dialect.identifier_preparer.quote(pk))
    conn.exec_driver_sql('ALTER TABLE qt_legacy_probe.fact_versions ADD PRIMARY KEY USING INDEX qt_probe_day_pk')
    conn.exec_driver_sql('ALTER TABLE qt_legacy_probe.fact_versions ADD UNIQUE USING INDEX qt_probe_revision_day')
    MarketFactHeaderLegacyRecord.__table__.create(conn)
    MarketFactHeaderPartitionRecord.__table__.create(conn)
    MarketFactHeaderSeriesDayRecord.__table__.create(conn)
    MarketFactVersionRecord.__table__.create(conn)
    # Keep all physical file identities while freeing the canonical index names.
    indexes = conn.execute(text("SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid='qt_legacy_probe.fact_versions'::regclass")).scalars().all()
    for number, name in enumerate(indexes):
        conn.exec_driver_sql(f'ALTER INDEX {name} RENAME TO qt_legacy_index_{number}')
    conn.exec_driver_sql('ALTER TABLE qt_legacy_probe.fact_versions RENAME TO fact_versions_legacy')
    conn.exec_driver_sql('ALTER TABLE qt_legacy_probe.fact_versions_legacy SET SCHEMA market')
    conn.exec_driver_sql("ALTER TABLE market.fact_versions ATTACH PARTITION market.fact_versions_legacy FOR VALUES FROM(MINVALUE) TO ('"+cutoff.isoformat()+"')")
    conn.exec_driver_sql('CREATE TRIGGER trg_assert_fact_version_valid BEFORE INSERT ON market.fact_versions FOR EACH ROW EXECUTE FUNCTION market.assert_fact_version_valid()')
    install_fact_storage_functions(conn)
    conn.exec_driver_sql('CREATE TRIGGER trg_reject_mutation_fact_versions BEFORE UPDATE OR DELETE ON market.fact_versions FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()')
    for name, events, scope in (
        ("trg_seal_fact_versions_legacy", "INSERT OR UPDATE OR DELETE", "ROW"),
        ("trg_seal_fact_versions_legacy_truncate", "TRUNCATE", "STATEMENT"),
    ):
        conn.exec_driver_sql(f"CREATE TRIGGER {name} BEFORE {events} ON market.fact_versions_legacy FOR EACH {scope} EXECUTE FUNCTION market.reject_fact_header_legacy_mutation()")
        conn.exec_driver_sql(f"ALTER TABLE market.fact_versions_legacy ENABLE ALWAYS TRIGGER {name}")
    conn.execute(text("INSERT INTO market.fact_header_legacy(id,end_day,relation_oid) VALUES(1,:day,'market.fact_versions_legacy'::regclass::oid)"), {"day":cutoff})
    conn.exec_driver_sql("UPDATE market.fact_storage_state SET layout_version='market.fact_storage_tiers.v2' WHERE layout_version='market.fact_storage_tiers.v1'")
    assert_fact_identity_references(conn)
    assert_fact_storage_contract(conn)


@pytest.fixture
def legacy(source):
    engine=source.database._engine
    cutoff=source.open_day+timedelta(days=1)
    with engine.connect() as c:
        original=dict(c.execute(text("SELECT oid,relfilenode FROM pg_class WHERE oid='market.fact_versions'::regclass")).mappings().one())
        search=[dict(r) for r in c.execute(text("SELECT c.oid,c.relfilenode FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='market.fact_versions'::regclass AND NOT i.indisunique ORDER BY c.oid")).mappings()]
        assert len(search)==11
    with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as c:
        c.exec_driver_sql('CREATE UNIQUE INDEX CONCURRENTLY qt_probe_day_pk ON market.fact_versions(id,storage_day)')
        c.exec_driver_sql('CREATE UNIQUE INDEX CONCURRENTLY qt_probe_revision_day ON market.fact_versions(series_id,observation_key,revision,storage_day)')
        c.exec_driver_sql("ALTER TABLE market.fact_versions ADD CONSTRAINT qt_probe_bound CHECK(storage_day<'"+cutoff.isoformat()+"') NOT VALID")
        c.exec_driver_sql('ALTER TABLE market.fact_versions VALIDATE CONSTRAINT qt_probe_bound')
    # Lost/interrupted transaction must retain the original table and rows.
    with engine.connect() as c:
        tx=c.begin();stage(c,cutoff);tx.rollback()
    with engine.connect() as c:
        assert c.scalar(text("SELECT 'market.fact_versions'::regclass::oid"))==original['oid']
        assert _headers(c,'market.fact_versions')==source.source_before
        assert _frozen_records(c)==source.frozen_before
    with engine.begin() as c:stage(c,cutoff)
    with engine.connect() as c:
        after=dict(c.execute(text("SELECT oid,relfilenode FROM pg_class WHERE oid='market.fact_versions_legacy'::regclass")).mappings().one())
        assert after==original
        for row in search:
            assert c.scalar(text('SELECT relfilenode FROM pg_class WHERE oid=:oid'),row)==row['relfilenode']
            assert c.scalar(text('SELECT count(*) FROM pg_inherits WHERE inhrelid=:oid'),row)==1
        assert _headers(c,'market.fact_versions')==source.source_before
        assert_fact_identity_contract(c)
        assert c.scalar(text('SELECT count(*) FROM market.fact_header_series_days'))==0
    source.legacy_cutoff=cutoff
    yield source


def test_real_qt_legacy_attach_read_compatibility(legacy,monkeypatch):
    source=legacy
    engine=source.database._engine
    cutoff=source.legacy_cutoff
    start=source.dataset_request.start;end=BASE+timedelta(days=3)
    assert source.repo.read_facts(series_id=source.series_id,start=start,end=end)==source.query_before
    before=source.repo.read_dataset_fact_revisions(dataset_id=source.frozen_dataset_id,series_id=source.series_id)
    assert before==source.frozen_result
    assert source.repo.freeze_dataset([source.dataset_request]).dataset_id==source.frozen_dataset_id
    _placement(monkeypatch,cutoff)
    assert source.repo.ingest_facts(series_id=source.series_id,source_id=source.source_id,facts=source.original_facts).noop_count==6
    old=source.original_facts[0]
    corrected=replace(old,payload={**old.payload,'rate':'0.2','raw_rate':'0.2'},accepted_at=BASE+timedelta(seconds=10),known_at=BASE+timedelta(seconds=10))
    result=source.repo.ingest_facts(series_id=source.series_id,source_id=source.source_id,facts=[corrected])
    assert result.corrected_count==1
    latest=source.repo.read_facts(series_id=source.series_id,start=start,end=end)
    selected=next(r for r in latest if r.fact.observation_key==old.observation_key)
    assert selected.revision==2
    assert source.repo.read_dataset_fact_revisions(dataset_id=source.frozen_dataset_id,series_id=source.series_id)==before
    frozen_now=source.repo.freeze_dataset([source.dataset_request])
    assert frozen_now.series[0]['row_count']==7
    causal=source.repo.read_facts(series_id=source.series_id,start=start,end=end,known_at_lte=BASE)
    assert next(r for r in causal if r.fact.observation_key==old.observation_key).revision==1
    with engine.connect() as c:
        assert c.scalar(text('SELECT count(*) FROM market.fact_versions_legacy'))==7
        assert c.scalar(text('SELECT count(*) FROM market.fact_versions'))==8
        assert _frozen_records(c)['dataset_series']
        assert_fact_identity_contract(c)
    original_bytes=source.archive_path.read_bytes()
    try:
        source.archive_path.write_bytes(b'corrupt')
        with pytest.raises(RuntimeError,match='canonical_archive_checksum_mismatch'):
            source.repo.read_dataset_fact_revisions(dataset_id=source.frozen_dataset_id,series_id=source.series_id)
    finally:source.archive_path.write_bytes(original_bytes)
    assert source.archive_path.read_bytes()==source.archive_bytes

    restarted=Database(source.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
    finally:
        restarted._reset_engine()


@pytest.mark.parametrize("sql", [
    "UPDATE market.fact_header_legacy SET end_day=end_day+1",
    "DELETE FROM market.fact_header_legacy",
    "TRUNCATE market.fact_header_legacy",
    "UPDATE market.fact_versions_legacy SET revision=revision+1",
    "DELETE FROM market.fact_versions_legacy",
    "TRUNCATE market.fact_versions_legacy",
    "INSERT INTO market.fact_versions_legacy SELECT * FROM market.fact_versions_legacy LIMIT 1",
])
def test_retained_range_and_binding_are_sealed(legacy,sql):
    with pytest.raises(DBAPIError,match="fact_header_legacy_sealed|immutable"):
        with legacy.database._engine.begin() as conn:
            conn.exec_driver_sql(sql)
    with pytest.raises(RuntimeError,match="fact_header_legacy_day_sealed"):
        with legacy.database._engine.begin() as conn:
            ensure_fact_header_partition(conn,legacy.legacy_cutoff-timedelta(days=1))


@pytest.mark.parametrize("sql,expected", [
    ("ALTER TABLE market.fact_versions_legacy DISABLE TRIGGER trg_seal_fact_versions_legacy", "seal_invalid"),
    ("ALTER TABLE market.fact_header_legacy DISABLE TRIGGER trg_guard_fact_header_legacy_truncate", "seal_invalid"),
    ("ALTER TABLE market.fact_versions DETACH PARTITION market.fact_versions_legacy", "binding_changed"),
    ("ALTER TABLE market.fact_versions_legacy RENAME TO foreign_legacy", "binding_changed"),
    ("DROP TABLE market.fact_versions_legacy", "binding_changed"),
    ("ALTER FUNCTION market.fact_header_legacy_end_day() VOLATILE", "function_incompatible"),
])
def test_changed_legacy_cannot_pass_startup(legacy,sql,expected):
    with legacy.database._engine.begin() as conn:
        conn.exec_driver_sql(sql)
    restarted=Database(legacy.dsn)
    try:
        assert not restarted.ensure_schema()
        assert expected in str(restarted.last_error)
    finally:
        restarted._reset_engine()
    if expected=="binding_changed":
        with pytest.raises(DBAPIError,match="fact_header_legacy_binding_changed"):
            legacy.repo.read_facts(series_id=legacy.series_id,start=legacy.dataset_request.start,end=BASE+timedelta(days=3))


def test_missing_legacy_catalog_is_not_recreated(storage):
    with storage.database._engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE market.fact_header_legacy")
    restarted=Database(storage.dsn)
    try:
        assert not restarted.ensure_schema()
        assert "fact_header_legacy" in str(restarted.last_error)
        with storage.database._engine.connect() as conn:
            assert conn.scalar(text("SELECT to_regclass('market.fact_header_legacy')")) is None
    finally:
        restarted._reset_engine()


@pytest.mark.parametrize("damage", ["oid", "empty_binding", "invalid_index", "disabled_reference"])
def test_native_legacy_proof_drift_refuses_admission(legacy,damage):
    engine=legacy.database._engine
    with engine.begin() as conn:
        if damage=="oid":
            conn.exec_driver_sql("ALTER TABLE market.fact_header_legacy DISABLE TRIGGER trg_guard_fact_header_legacy")
            conn.exec_driver_sql("UPDATE market.fact_header_legacy SET relation_oid=relation_oid+1")
            conn.exec_driver_sql("ALTER TABLE market.fact_header_legacy ENABLE ALWAYS TRIGGER trg_guard_fact_header_legacy")
        elif damage=="empty_binding":
            conn.exec_driver_sql("ALTER TABLE market.fact_header_legacy DISABLE TRIGGER trg_guard_fact_header_legacy_truncate")
            conn.exec_driver_sql("TRUNCATE market.fact_header_legacy")
            conn.exec_driver_sql("ALTER TABLE market.fact_header_legacy ENABLE ALWAYS TRIGGER trg_guard_fact_header_legacy_truncate")
        elif damage=="invalid_index":
            # Privileged damage injection in the owned disposable database only.
            conn.exec_driver_sql("UPDATE pg_index SET indisvalid=false WHERE indexrelid=(SELECT indexrelid FROM pg_index WHERE indrelid='market.fact_versions_legacy'::regclass AND NOT indisunique LIMIT 1)")
        else:
            trigger=conn.scalar(text("SELECT t.tgname FROM pg_trigger t JOIN pg_constraint c ON c.oid=t.tgconstraint WHERE t.tgrelid='market.fact_versions_legacy'::regclass AND c.contype='f' LIMIT 1"))
            assert trigger
            conn.exec_driver_sql('ALTER TABLE market.fact_versions_legacy DISABLE TRIGGER '+conn.dialect.identifier_preparer.quote(trigger))
    expected={"oid":"binding_changed", "empty_binding":"unregistered", "invalid_index":"index_binding_invalid", "disabled_reference":"reference_binding_invalid"}[damage]
    with pytest.raises((RuntimeError,DBAPIError),match=expected):
        with engine.connect() as conn:
            assert_fact_identity_contract(conn)
    catalog_error = "registry_attachment_mismatch" if damage == "empty_binding" else expected
    with pytest.raises((RuntimeError,DBAPIError),match=catalog_error):
        read_header_catalog(engine)


def test_legacy_physical_catalog_counts_all_files_and_locks_exact_retained_group(legacy):
    engine = legacy.database._engine
    inventory = read_header_catalog(engine)
    group, = [g for g in inventory.snapshot.partitions if g.legacy_end_day is not None]
    assert group.legacy_end_day == legacy.legacy_cutoff
    assert group.storage_day == legacy.legacy_cutoff-timedelta(days=1)
    with engine.connect() as conn:
        assert sum(r.byte_count for r in group.relations) == conn.scalar(text(
            "SELECT pg_total_relation_size('market.fact_versions_legacy')"))
        assert len(group.indexes) == conn.scalar(text(
            "SELECT count(*) FROM pg_index WHERE indrelid='market.fact_versions_legacy'::regclass"))
        assert len(group.indexes) >= 13
    with engine.begin() as conn:
        partial = read_locked_header_group(conn, storage_day=group.storage_day, heap_oid=group.heap.oid)
        assert not partial.snapshot.inventory_complete
        assert partial.snapshot.partitions == (group,)
        assert conn.scalar(text("SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() "
                                "AND relation=:oid AND mode='AccessExclusiveLock' AND granted"),
                           {"oid":group.heap.oid}) == 1


def test_daily_group_observation_does_not_wait_on_locked_legacy_heap(legacy):
    engine = legacy.database._engine
    day = legacy.legacy_cutoff
    with engine.begin() as conn:
        name = ensure_fact_header_partition(conn, day)
        oid = conn.scalar(text("SELECT to_regclass(:name)::oid"), {"name":name})
    with engine.begin() as blocker:
        blocker.exec_driver_sql("LOCK TABLE ONLY market.fact_versions_legacy IN ACCESS EXCLUSIVE MODE")
        with engine.begin() as conn:
            partial = read_locked_header_group(conn, storage_day=day, heap_oid=oid, timeout_seconds=2)
            group, = partial.snapshot.partitions
            assert group.storage_day == day and group.legacy_end_day is None


def test_catalog_budget_includes_legacy_group(legacy):
    engine = legacy.database._engine
    with engine.begin() as conn:
        ensure_fact_header_partition(conn, legacy.legacy_cutoff)
    with pytest.raises(RuntimeError, match="partition_budget_exceeded"):
        read_header_catalog(engine, max_partitions=1)
