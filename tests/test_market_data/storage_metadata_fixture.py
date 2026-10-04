"""Fixed shared-metadata candidate for the disposable storage demonstration only.

This is not a production cutover command. The four existing per-record catalogs
are the current growth problem; no alternate placements or rebalancing are added.
"""
import os
from pathlib import Path
from time import monotonic

import pytest
from sqlalchemy import text

TABLES = (
    "fact_identities", "raw_archive_record_mappings",
    "fact_archive_material_aliases", "fact_archive_canonical_dependencies",
)


def observe_metadata(engine, source_root):
    report = {}
    with engine.begin() as conn:
        for name in TABLES:
            count = conn.scalar(text(f'SELECT count(*) FROM market."{name}"'))
            assert count <= 1024, "only bounded disposable fixture data is permitted"
            digest = conn.scalar(text(f"""
                SELECT md5(COALESCE(string_agg(row_to_json(t)::text, ''
                       ORDER BY row_to_json(t)::text), '')) FROM market."{name}" t
            """))
            members = conn.execute(text("""
                WITH heap AS (
                    SELECT oid,reltoastrelid FROM pg_class
                    WHERE oid=to_regclass(:relation)
                ), tables AS (
                    SELECT oid FROM heap
                    UNION ALL SELECT reltoastrelid FROM heap WHERE reltoastrelid<>0
                ), members AS (
                    SELECT oid FROM tables
                    UNION ALL SELECT indexrelid FROM pg_index WHERE indrelid IN (SELECT oid FROM tables)
                )
                SELECT c.oid::bigint AS oid,c.relname AS name,c.relfilenode::bigint AS node,
                       pg_relation_filepath(c.oid) AS path,
                       pg_relation_size(c.oid) AS bytes
                FROM members JOIN pg_class c ON c.oid=members.oid ORDER BY c.oid
            """), {"relation": "market."+name}).mappings().all()
            assert members
            files = []
            for member in members:
                physical = source_root / member["path"]
                files.append({**dict(member), "device": physical.stat().st_dev})
            report[name] = {"rows": count, "content_hash": digest, "files": files}
    return report


def prepare_metadata_candidate(engine, source_root, history_root, *,
                               tablespace_name="qt_demo_history_hdd", require_populated=True):
    assert os.getenv("QT_DB_TEST_ISOLATED") == "1" and os.getenv("QT_STORAGE_DEMO") == "1"
    assert source_root == Path("/qt-source/pgdata") and history_root == Path("/qt-history")
    assert tablespace_name in {"qt_demo_history_hdd", "qt_demo_history_cold"}
    before = observe_metadata(engine, source_root)
    if require_populated:
        assert before["fact_identities"]["rows"] > 0
        assert before["raw_archive_record_mappings"]["rows"] > 0
    source_device, history_device = source_root.stat().st_dev, history_root.stat().st_dev
    assert source_device != history_device
    assert all(f["device"] == source_device for entry in before.values() for f in entry["files"])

    def move(interrupt=False):
        with engine.begin() as conn:
            conn.exec_driver_sql("SET LOCAL statement_timeout = '30s'")
            conn.exec_driver_sql("SET LOCAL lock_timeout = '1s'")
            for name in TABLES:
                conn.exec_driver_sql(f'ALTER TABLE market."{name}" SET TABLESPACE {tablespace_name}')
                if interrupt:
                    raise RuntimeError("disposable metadata move interrupted")
                indexes = conn.execute(text("""
                    SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
                    WHERE i.indrelid=to_regclass(:relation) ORDER BY c.oid
                """), {"relation": "market."+name}).scalars().all()
                for index in indexes:
                    quoted = conn.dialect.identifier_preparer.quote(index)
                    conn.exec_driver_sql(f"ALTER INDEX market.{quoted} SET TABLESPACE {tablespace_name}")

    with pytest.raises(RuntimeError, match="disposable metadata move interrupted"):
        move(interrupt=True)
    assert observe_metadata(engine, source_root) == before
    started = monotonic()
    move()
    elapsed = monotonic()-started
    after = observe_metadata(engine, source_root)
    assert {name:(entry["rows"],entry["content_hash"]) for name,entry in after.items()} == {
        name:(entry["rows"],entry["content_hash"]) for name,entry in before.items()}
    assert all(f["device"] == history_device for entry in after.values() for f in entry["files"])
    return {"preserved_rows_and_content": True, "rollback_preserved_source_files": True,
            "movement_seconds": round(elapsed,4), "tables": after}
