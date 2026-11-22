from copy import deepcopy
from hashlib import sha256
import shutil
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from scripts.db import canonical_cold_custody_audit as audit
from tests.test_market_data.test_canonical_cold_migration_db import local_catalog_server  # noqa: F401


@pytest.mark.db
def test_custody_pair_checks_exact_raw_ordinals_and_both_hold_generations(local_catalog_server, monkeypatch, tmp_path):
    from data_providers.streams.contracts import ProviderRawMessage
    from market_data.archive import encode_raw_records_to_parquet
    from market_data.structure import RawStreamRecord

    url = local_catalog_server[0].update_query_dict({"port": "5432"})
    source_name, candidate_name = "qt_bench_custody_source", "qt_bench_custody_candidate"
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    source, candidate = (create_engine(url.set(database=name)) for name in (source_name, candidate_name))
    monkeypatch.setattr(audit, "_code_revision", lambda module, expected: expected)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    (tmp_path / "objects").mkdir()
    raws = [RawStreamRecord.from_provider_message(ProviderRawMessage.build(
        provider="COINBASE", venue="TEST", stream_session_id="session", connection_epoch=1, receive_ordinal=i,
        raw_frame=f'{{"ordinal": {i}}}', received_at="2026-08-09T00:00:00+00:00"), definition_id="definition",
        spool_segment_id="segment", provider_product_id="BTC-USD", requested_channel="level2",
        observed_channel="level2") for i in (1, 2)]
    raw_object = encode_raw_records_to_parquet(raws, archive_segment_id="segment", temporary_directory=tmp_path)
    shutil.copyfile(raw_object.path, tmp_path / "objects" / "raw.parquet")
    checkpoint = b"fixture checkpoint file scoped only to custody and checksums"
    (tmp_path / "objects" / "checkpoint.parquet").write_bytes(checkpoint)
    plan = audit._validate_plan({"restore_pair_hash": "a" * 64, "commit_seq": 9, "retire_before": "2026-08-10",
        "targets": [{"kind": "raw_manifest", "id": "raw", "required_holds": ["explicit", "published", "prepared"]},
                    {"kind": "book_checkpoint", "id": "checkpoint", "required_holds": ["published"]}]})

    def run(side, engine):
        with engine.connect() as conn:
            system_id, oid = conn.execute(text("""SELECT system_identifier::text,
                (SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()""")).one()
        monkeypatch.setenv("PG_DSN", engine.url.render_as_string(hide_password=False))
        return audit.run(SimpleNamespace(side=side, revision="b" * 40, system_identifier=system_id, database_oid=oid,
            archive_root=tmp_path, max_raw_rows=10, max_hold_rows=10, max_archive_bytes=2**20, max_seconds=15), plan)

    try:
        with admin.connect() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE {source_name} TEMPLATE template0")
        with source.begin() as conn:
            conn.exec_driver_sql("""
                CREATE SCHEMA market;
                CREATE TABLE market.fact_storage_state (layout_version text,state text,completed_at timestamptz,evidence jsonb);
                CREATE TABLE market.series (id bigint);
                INSERT INTO market.series VALUES (1);
                CREATE TABLE market.fact_versions (series_id bigint,market_commit_seq bigint);
                INSERT INTO market.fact_versions VALUES (1,9);
                CREATE TABLE market.cold_fact_pages (staged_storage_day date,last_commit_seq bigint);
                CREATE TABLE market.cold_fact_retired_days (storage_day date);
                CREATE TABLE market.raw_archive_manifests (id text, object_key text, object_uri text, object_sha256 text,
                    byte_count bigint,content_fingerprint text,record_count bigint,first_receive_ordinal bigint,
                    last_receive_ordinal bigint,definition_id text,session_id text,connection_epoch int);
                CREATE TABLE market.book_checkpoint_manifests (id text,object_key text,object_uri text,object_sha256 text,
                    byte_count bigint,content_fingerprint text,session_id text,source_manifest_ids jsonb);
                CREATE TABLE market.raw_archive_record_mappings (manifest_id text,raw_record_id text,
                    spool_segment_id text,session_id text,connection_epoch int,receive_ordinal bigint,object_row_index bigint,
                    object_row_group int,raw_frame_sha256 text,mapped_at timestamptz,known_at timestamptz);
                CREATE TABLE market.archive_retention_pin_versions (id text,pin_id text,revision int,target_kind text,
                    target_id text,owner_kind text,owner_id text,status text,reason text,effective_at timestamptz,known_at timestamptz);
                CREATE TABLE market.dataset_archive_refs (raw_archive_manifest_id text,dataset_id text);
                CREATE TABLE market.raw_archive_compaction_sources (replacement_manifest_id text,source_manifest_id text,source_ordinal bigint);
                CREATE TABLE market.fact_archive_dependencies (manifest_id text,target_kind text,target_id text,object_key text,object_sha256 text);
                CREATE TABLE market.fact_archive_active_generations (manifest_id text,preparation_id text,generation bigint,catalog_commitment text);
                CREATE TABLE market.fact_archive_preparations (id text,manifest_id text,state text,generation bigint,catalog_commitment text);
                CREATE TABLE market.fact_archive_preparation_dependencies (preparation_id text,target_kind text,target_id text,
                    object_key text,object_sha256 text);
                CREATE TABLE market.fact_book_prefix_dependencies (chunk_id text,target_id text);
                CREATE TABLE market.fact_book_prefix_chunks (definition_id text,session_id text);
                CREATE TABLE market.storage_lifecycle_events (target_kind text,target_id text,action text,event_type text,
                    occurred_at timestamptz,event_ordinal bigint,reason text,evidence jsonb);
                CREATE TABLE market.stream_coverage_interval_versions (definition_id text,session_id text,interval_id text);
                CREATE TABLE market.fact_hot_payloads (provenance jsonb);
                INSERT INTO market.fact_storage_state VALUES ('market.fact_storage_tiers.v2','ready',now(),'{}');
                INSERT INTO market.archive_retention_pin_versions VALUES
                    ('v1','pin',1,'raw_manifest','raw','operator','owner','released','old',now(),now()),
                    ('v2','pin',2,'raw_manifest','raw','operator','owner','active','hold',now(),now());
                INSERT INTO market.fact_archive_preparations VALUES ('prep','fact-page','building',1,null);
                INSERT INTO market.fact_archive_active_generations VALUES ('fact-page','active-prep',1,'fixture-commitment');
            """)
            conn.execute(text("""INSERT INTO market.raw_archive_manifests VALUES
                ('raw','raw.parquet','market-archive://raw.parquet',:sha,:size,:content,2,1,2,'definition','session',1)"""),
                {"sha": raw_object.sha256, "size": raw_object.byte_count, "content": raw_object.content_fingerprint})
            conn.execute(text("""INSERT INTO market.book_checkpoint_manifests VALUES
                ('checkpoint','checkpoint.parquet','market-archive://checkpoint.parquet',:sha,:bytes,:sha,'session','[\"raw\"]')"""),
                {"sha": sha256(checkpoint).hexdigest(), "bytes": len(checkpoint)})
            conn.execute(text("""INSERT INTO market.raw_archive_record_mappings VALUES
                ('raw',:id,'segment','session',1,:ord,:row,0,:sha,now(),now())"""),
                [{"id": item.raw_record_id, "ord": item.receive_ordinal, "row": i, "sha": item.raw_frame_sha256}
                 for i, item in enumerate(raws)])
            conn.execute(text("""INSERT INTO market.fact_archive_dependencies VALUES
                ('fact-page','raw_manifest','raw','raw.parquet',:raw),('fact-page','book_checkpoint','checkpoint','checkpoint.parquet',:check)"""),
                {"raw": raw_object.sha256, "check": sha256(checkpoint).hexdigest()})
            conn.execute(text("""INSERT INTO market.fact_archive_preparation_dependencies VALUES
                ('prep','raw_manifest','raw','raw.parquet',:sha)"""), {"sha": raw_object.sha256})
        source.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE {candidate_name} TEMPLATE {source_name}")
        with candidate.begin() as conn:
            conn.exec_driver_sql("""UPDATE market.fact_storage_state SET layout_version='market.canonical_cold_history.v1',
                evidence='{"cold_before_day":"2026-08-10"}'""")
        left, right = run("source", source), run("candidate", candidate)
        result = audit.compare(left, right)
        raw = result["outcomes"][0]
        assert raw["raw_ordinals"]["count"] == 2
        assert {key: raw["holds"][key]["count"] for key in ("explicit", "published", "prepared")} == {
            "explicit": 2, "published": 1, "prepared": 1}
        incomplete = deepcopy(right)
        incomplete["outcomes"][0]["raw_ordinals"]["count"] = 0
        with pytest.raises(RuntimeError, match="incomplete or mismatched receipt"):
            audit.compare(left, incomplete)
        with candidate.begin() as conn:
            conn.exec_driver_sql("UPDATE market.archive_retention_pin_versions SET reason='different' WHERE revision=1")
        with pytest.raises(RuntimeError, match="ordered archive evidence differs"):
            audit.compare(left, run("candidate", candidate))
        with candidate.begin() as conn:
            conn.exec_driver_sql("UPDATE market.raw_archive_record_mappings SET receive_ordinal=3 WHERE object_row_index=1")
        with pytest.raises(RuntimeError, match="raw ordinal mismatch"):
            run("candidate", candidate)
    finally:
        source.dispose()
        candidate.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {candidate_name} WITH (FORCE)")
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {source_name} WITH (FORCE)")
        admin.dispose()
