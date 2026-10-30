"""Real PostgreSQL catalogue routing; independent hot/cold byte and hash gates."""
from datetime import date, timedelta
from types import SimpleNamespace
import json

import pytest
from sqlalchemy import create_engine, event, text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.fact_archive import archive_evidence_hash, publish_canonical_fact_archive
from portal.backend.service.storage.repos.fact_dependencies import verify_canonical_source_placements
from tests.test_market_data.migration_test_support import fresh_migration_database
from tests.test_market_data.test_canonical_fact_archive import _row

pytestmark = pytest.mark.db


@pytest.fixture(scope="module")
def catalogue(tmp_path_factory):
    root = tmp_path_factory.mktemp("source-placement")
    store = FilesystemRawArchiveObjectStore(root / "objects")
    cold = _row()
    manifest = publish_canonical_fact_archive([cold], object_store=store,
                                            temporary_directory=root / "staging")
    with fresh_migration_database("source_placement", install_extensions=False) as dsn:
        engine = create_engine(dsn)
        try:
            with engine.begin() as c:
                c.execute(text("CREATE SCHEMA market"))
                c.execute(text("CREATE TABLE market.fact_identities(id text PRIMARY KEY,storage_day date NOT NULL)"))
                c.execute(text("CREATE TABLE market.fact_versions(id text NOT NULL,row_hash text NOT NULL,storage_day date NOT NULL,market_commit_seq bigint NOT NULL,PRIMARY KEY(storage_day,id)) PARTITION BY RANGE(storage_day)"))
                c.execute(text("CREATE TABLE market.fact_versions_legacy PARTITION OF market.fact_versions FOR VALUES FROM ('2020-01-01') TO ('2026-10-09')"))
                for offset in range(10):
                    day = date(2026,10,9)+timedelta(days=offset)
                    c.execute(text(f"CREATE TABLE market.fact_versions_{day:%Y%m%d} PARTITION OF market.fact_versions FOR VALUES FROM ('{day}') TO ('{day+timedelta(days=1)}')"))
                c.execute(text("CREATE TABLE market.fact_hot_payloads(id text NOT NULL,storage_day date NOT NULL,PRIMARY KEY(storage_day,id)) PARTITION BY RANGE(storage_day)"))
                for offset in range(79):
                    day = date(2026,8,1)+timedelta(days=offset)
                    if day == date(2026,8,21):
                        continue  # Retired source day has permanent headers, no hot child.
                    c.execute(text(f"CREATE TABLE market.fact_hot_payloads_{day:%Y%m%d} PARTITION OF market.fact_hot_payloads FOR VALUES FROM ('{day}') TO ('{day+timedelta(days=1)}')"))
                c.execute(text("CREATE TABLE market.fact_retention_partitions(storage_day date PRIMARY KEY,state text NOT NULL)"))
                c.execute(text("CREATE TABLE market.fact_archive_manifests(id text PRIMARY KEY,storage_day date,page_ordinal int,object_key text,object_sha256 text,row_count bigint,byte_count bigint,first_commit_seq bigint,first_id text,last_commit_seq bigint,last_id text,manifest_hash text,descriptor jsonb)"))
                c.execute(text("INSERT INTO market.fact_versions SELECT 'hot-'||lpad(i::text,5,'0'),repeat('a',64),DATE '2026-08-22',i+1 FROM generate_series(1,1000)i"))
                c.execute(text("INSERT INTO market.fact_hot_payloads SELECT id,storage_day FROM market.fact_versions"))
                c.execute(text("INSERT INTO market.fact_versions VALUES(:id,:hash,DATE '2026-08-21',1)"),{'id':cold['id'],'hash':cold['row_hash']})
                c.execute(text("INSERT INTO market.fact_versions SELECT 'unrelated-'||i,repeat('c',64),DATE '2026-08-23',i+2000 FROM generate_series(1,25000)i"))
                c.execute(text("INSERT INTO market.fact_identities SELECT id,storage_day FROM market.fact_versions"))
                c.execute(text("INSERT INTO market.fact_retention_partitions VALUES(DATE '2026-08-21','reclaimed')"))
                params={'id':manifest.manifest_id,'key':manifest.object_key,'sha':manifest.object_sha256,
                    'rows':manifest.row_count,'bytes':manifest.byte_count,'first':manifest.first_cursor[0],
                    'first_id':manifest.first_cursor[1],'last':manifest.last_cursor[0],
                    'last_id':manifest.last_cursor[1],'hash':manifest.manifest_hash,
                    'descriptor':json.dumps(manifest.to_dict())}
                c.execute(text("INSERT INTO market.fact_archive_manifests VALUES(:id,DATE '2026-08-21',0,:key,:sha,:rows,:bytes,:first,:first_id,:last,:last_id,:hash,CAST(:descriptor AS jsonb))"),params)
                c.execute(text("ANALYZE market.fact_versions"));c.execute(text("ANALYZE market.fact_identities"));c.execute(text("ANALYZE market.fact_hot_payloads"))
            dependencies=[{'fact_version_id':f'hot-{i:05d}','row_hash':'a'*64} for i in range(1,1001)]
            dependencies.append({'fact_version_id':cold['id'],'row_hash':cold['row_hash']})
            yield SimpleNamespace(engine=engine,store=store,manifest=manifest,cold=cold,dependencies=dependencies)
        finally:
            engine.dispose()


def test_exact_cross_day_placement_hash_and_current_cold_bytes(catalogue):
    f=catalogue
    placements=[{'id':x['fact_version_id'],'storage_day':'2026-08-22','tier':'hot'} for x in f.dependencies[:-1]]
    placements.append({'id':f.cold['id'],'storage_day':'2026-08-21','tier':'cold',
        'manifest_id':f.manifest.manifest_id,'manifest_hash':f.manifest.manifest_hash})
    with f.engine.connect() as c:
        objects=ArchiveVerificationBatch(f.store, limits=ArchiveVerificationLimits())
        result=verify_canonical_source_placements(c,f.dependencies,objects=objects)
        objects.assert_unchanged()
    assert result==archive_evidence_hash({'placements':sorted(placements,key=lambda x:x['id'])})
    assert len(objects.objects)==1 and objects.byte_count==f.manifest.byte_count


@pytest.mark.parametrize('mode',['directory_missing','directory_wrong_day','header_missing','header_hash','cold_not_verified','cold_overlap'])
def test_bad_directory_headers_and_cold_coverage_still_refuse(catalogue,mode):
    f=catalogue
    with f.engine.connect() as c:
        transaction=c.begin()
        try:
            if mode=='directory_missing':c.execute(text("DELETE FROM market.fact_identities WHERE id='hot-00001'"))
            elif mode=='directory_wrong_day':c.execute(text("UPDATE market.fact_identities SET storage_day=DATE '2026-08-24' WHERE id='hot-00001'"))
            elif mode=='header_missing':c.execute(text("DELETE FROM market.fact_versions WHERE id='hot-00001'"))
            elif mode=='header_hash':c.execute(text("UPDATE market.fact_versions SET row_hash=repeat('b',64) WHERE id='hot-00001'"))
            elif mode=='cold_not_verified':c.execute(text("UPDATE market.fact_retention_partitions SET state='sealed'"))
            else:c.execute(text("INSERT INTO market.fact_archive_manifests SELECT id||'-overlap',storage_day,page_ordinal+1,object_key,object_sha256,row_count,byte_count,first_commit_seq,first_id,last_commit_seq,last_id,manifest_hash,descriptor FROM market.fact_archive_manifests"))
            with pytest.raises(RuntimeError,match='canonical_source_dependency_'):
                verify_canonical_source_placements(c,f.dependencies,objects=ArchiveVerificationBatch(f.store, limits=ArchiveVerificationLimits()))
        finally:transaction.rollback()


def test_cold_bytes_changed_after_receipt_still_refuse(catalogue):
    f=catalogue;path=f.store.local_path(f.manifest.object_key);original=path.read_bytes()
    try:
        path.write_bytes(b'corrupted cold source')
        with f.engine.connect() as c,pytest.raises(RuntimeError,match='archive_verification'):
            verify_canonical_source_placements(c,f.dependencies,objects=ArchiveVerificationBatch(f.store, limits=ArchiveVerificationLimits()))
    finally:path.write_bytes(original)


def test_source_queries_prune_unrelated_partitions(catalogue):
    f=catalogue;queries=[]
    def capture(conn,cursor,statement,parameters,context,executemany):
        if ('AS hot_present' in statement or 'source.id AS source_id' in statement) and not statement.startswith('EXPLAIN'):
            queries.append((statement,parameters))
    event.listen(f.engine,'before_cursor_execute',capture)
    try:
        with f.engine.connect() as c:
            verify_canonical_source_placements(c,f.dependencies,objects=ArchiveVerificationBatch(f.store, limits=ArchiveVerificationLimits()))
            assert len(queries)==3  # Two header batches, one current cold-page lookup.
            for statement,params in queries:
                root=c.exec_driver_sql('EXPLAIN (FORMAT JSON) '+statement,params).scalar_one()[0]['Plan']
                names=[]
                def walk(node):
                    if 'Relation Name' in node:names.append(node['Relation Name'])
                    for child in node.get('Plans',[]):walk(child)
                walk(root)
                assert all(name in {'fact_versions_legacy','fact_hot_payloads_20260822',
                    'fact_retention_partitions','fact_archive_manifests'} for name in names),names
    finally:event.remove(f.engine,'before_cursor_execute',capture)


def test_cancellation_after_directory_lookup_never_reads_headers(catalogue):
    calls=[]
    def checkpoint():
        calls.append(True)
        if len(calls)==2:raise RuntimeError('cancelled after directory lookup')
    with catalogue.engine.connect() as c,pytest.raises(RuntimeError,match='cancelled after directory'):
        verify_canonical_source_placements(c,catalogue.dependencies,check_budget=checkpoint)
