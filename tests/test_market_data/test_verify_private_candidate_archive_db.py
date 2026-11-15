"""Verify the real accepted SQL catalog across all four archive families."""

from hashlib import sha256

import pytest
from sqlalchemy import create_engine, text

from scripts.ci.verify_private_candidate_archive import _verify_catalog
from tests.test_market_data.migration_test_support import fresh_migration_database


@pytest.mark.db
def test_selected_catalog_includes_raw_checkpoints_original_and_cold_pages(tmp_path):
    buckets = {
        "raw_archive_manifests": {"raw": b"raw accepted", "expired": b"expired"},
        "book_checkpoint_manifests": {"checkpoint": b"book accepted"},
        "fact_archive_manifests": {"canonical": b"original accepted"},
        "cold_fact_pages": {"canonical": b"original accepted", "cold": b"candidate accepted"},
    }
    with fresh_migration_database("candidate_archive", install_extensions=False) as dsn:
        engine = create_engine(dsn, isolation_level="REPEATABLE READ")
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql("CREATE SCHEMA market")
                conn.exec_driver_sql("""
                    CREATE TABLE market.storage_lifecycle_events (
                        action text, event_type text, target_kind text, target_id text)
                """)
                conn.exec_driver_sql("""
                    INSERT INTO market.storage_lifecycle_events VALUES
                        ('archive_expire','completed','raw_manifest','expired')
                """)
                for table, objects in buckets.items():
                    conn.exec_driver_sql(f"""
                        CREATE TABLE market.{table} (
                            id text PRIMARY KEY, object_key text, object_sha256 text, byte_count bigint)
                    """)
                    for key, data in objects.items():
                        conn.execute(text(f"INSERT INTO market.{table} VALUES (:key,:key,:sha,:size)"),
                                     {"key": key, "sha": sha256(data).hexdigest(), "size": len(data)})
                        if key != "expired":
                            (tmp_path / key).write_bytes(data)

            (tmp_path / "retained-unreferenced").write_bytes(b"outside selected catalog")

            def verify(*, max_objects=4, max_bytes=8192):
                with engine.begin() as conn:
                    conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                    return _verify_catalog(conn, tmp_path, max_objects=max_objects, max_bytes=max_bytes,
                                           check=lambda: None, bound=lambda: None)

            first = verify()
            assert first == verify()
            assert first["objects"] == 4
            assert first["bytes"] == sum(len((tmp_path / key).read_bytes())
                                         for key in ("raw", "checkpoint", "canonical", "cold"))
            with pytest.raises(RuntimeError, match="catalog_exceeds_budget"):
                verify(max_bytes=10)
            with pytest.raises(RuntimeError, match="recovery_object_budget_exceeded"):
                verify(max_objects=3)
            raw = tmp_path / "raw"
            raw.write_bytes(b"x" * raw.stat().st_size)
            with pytest.raises(RuntimeError, match="selected_object_mismatch: raw"):
                verify()
            raw.write_bytes(buckets["raw_archive_manifests"]["raw"])
            (tmp_path / "checkpoint").unlink()
            with pytest.raises(RuntimeError, match="missing_selected_object: checkpoint"):
                verify()
        finally:
            engine.dispose()
