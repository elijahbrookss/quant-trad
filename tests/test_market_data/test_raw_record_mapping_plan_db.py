"""Plan and exact result parity for a broad, bound raw-record verification request."""

import pytest
from sqlalchemy import create_engine, text

from portal.backend.service.storage.repos.fact_lineage import raw_archive_mapping_lookup_statement
from tests.test_market_data.migration_test_support import fresh_migration_database


pytestmark = pytest.mark.db


_REFERENCE = text("""
    SELECT manifests.*, mappings.raw_record_id, mappings.object_row_index, mappings.object_row_group,
           mappings.spool_segment_id AS mapped_segment_id, mappings.session_id AS mapped_session_id,
           mappings.connection_epoch AS mapped_epoch, mappings.receive_ordinal AS mapped_ordinal,
           mappings.raw_frame_sha256 AS mapped_frame_sha256
    FROM market.raw_archive_record_mappings AS mappings
    JOIN market.raw_archive_manifests AS manifests ON manifests.id=mappings.manifest_id
    WHERE mappings.raw_record_id=ANY(:ids) AND manifests.id=ANY(:bound_ids) AND NOT EXISTS (
        SELECT 1 FROM market.storage_lifecycle_events AS expired
        WHERE expired.action='archive_expire' AND expired.event_type='completed'
          AND expired.target_kind='raw_manifest' AND expired.target_id=manifests.id)
    ORDER BY mappings.raw_record_id,manifests.byte_count,manifests.id LIMIT :limit
""")


def _nodes(tree):
    yield tree
    for child in tree.get("Plans", []):
        yield from _nodes(child)


def test_wide_bound_record_lookup_seeks_by_requested_pk_and_preserves_exact_order():
    with fresh_migration_database("record_lineage", install_extensions=False) as dsn:
        engine = create_engine(dsn)
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql("""
                    CREATE SCHEMA market;
                    CREATE TABLE market.raw_archive_manifests (
                        id varchar(128) PRIMARY KEY, byte_count bigint NOT NULL, object_key text NOT NULL
                    );
                    CREATE TABLE market.raw_archive_record_mappings (
                        raw_record_id varchar(80) NOT NULL, manifest_id varchar(128) NOT NULL,
                        object_row_index bigint NOT NULL, object_row_group integer NOT NULL DEFAULT 0,
                        spool_segment_id varchar(80) NOT NULL, session_id varchar(64) NOT NULL,
                        connection_epoch integer NOT NULL DEFAULT 0, receive_ordinal bigint NOT NULL,
                        raw_frame_sha256 varchar(64) NOT NULL, known_at timestamptz NOT NULL DEFAULT now(),
                        CONSTRAINT pk_market_raw_archive_record_mapping PRIMARY KEY (raw_record_id,manifest_id),
                        UNIQUE (manifest_id,object_row_index)
                    );
                    CREATE TABLE market.storage_lifecycle_events (
                        target_kind text NOT NULL, target_id text NOT NULL, action text NOT NULL, event_type text NOT NULL
                    );
                    CREATE INDEX raw_expiry_scope ON market.storage_lifecycle_events(target_kind,target_id,action,event_type);
                    INSERT INTO market.raw_archive_manifests
                        SELECT 'manifest-'||i,1000+mod(i,17), 'fixture/'||i FROM generate_series(1,2000) i;
                    INSERT INTO market.raw_archive_record_mappings (
                        raw_record_id,manifest_id,object_row_index,spool_segment_id,session_id,receive_ordinal,raw_frame_sha256
                    ) SELECT 'raw-'||i,'manifest-'||(mod(i,2000)+1),i,'segment','session',i,repeat('a',64)
                      FROM generate_series(1,400000) i;
                    -- Each requested ID can carry more than one placement.
                    INSERT INTO market.raw_archive_record_mappings (
                        raw_record_id,manifest_id,object_row_index,spool_segment_id,session_id,receive_ordinal,raw_frame_sha256
                    ) SELECT 'raw-'||i,'manifest-'||(mod(i+1,2000)+1),400000+i,'compacted','session',i,repeat('a',64)
                      FROM generate_series(1,6840) i;
                    INSERT INTO market.storage_lifecycle_events VALUES
                        ('raw_manifest','manifest-2','archive_expire','started'),
                        ('raw_manifest','manifest-3','archive_expire','completed'),
                        ('book_checkpoint','manifest-4','archive_expire','completed');
                    ANALYZE market.raw_archive_record_mappings;
                    ANALYZE market.raw_archive_manifests;
                    ANALYZE market.storage_lifecycle_events;
                """)
            inputs = {"ids": [f"raw-{i}" for i in range(1, 6841)] + ["raw-1", "unknown"],
                      "bound_ids": [f"manifest-{i}" for i in range(1, 1743)], "limit": 50_001}
            query = raw_archive_mapping_lookup_statement(mode="record", bound=True)
            with engine.begin() as conn:
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
                expected = conn.execute(_REFERENCE, inputs).all()
                assert len(expected) > 6840
                assert conn.execute(query, inputs).all() == expected
                assert conn.execute(query, inputs | {"limit": 4}).all() == expected[:4]
                assert conn.execute(query, inputs | {"bound_ids": []}).all() == []
                assert "manifest-3" not in {row.id for row in expected}
                assert {"manifest-2", "manifest-4"} <= {row.id for row in expected}
                details = conn.execute(text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + str(query)), inputs).scalar_one()[0]
            nodes = list(_nodes(details["Plan"]))
            probes = [node for node in nodes if node.get("Relation Name") == "raw_archive_record_mappings"]
            assert len(probes) == 1
            probe, = probes
            assert probe["Node Type"] == "Index Scan"
            assert probe["Index Name"] == "pk_market_raw_archive_record_mapping"
            assert "requested.raw_record_id" in probe["Index Cond"] or "records.raw_record_id" in probe["Index Cond"]
            assert probe["Actual Loops"] == 6841
            assert probe["Actual Rows"] <= 2
        finally:
            engine.dispose()
