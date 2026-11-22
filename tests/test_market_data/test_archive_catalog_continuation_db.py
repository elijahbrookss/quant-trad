"""Scoped continuation against competing global indexes, on disposable PostgreSQL."""
import json

import pytest
from sqlalchemy import create_engine, text

from portal.backend.service.storage.repos.fact_archival import (
    ARCHIVE_PAGE_CATALOG_SPECS, archive_page_catalog_statement,
)
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


def _nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from _nodes(child)


def test_continuation_uses_scope_index_and_preserves_rows_and_physical_witnesses():
    with fresh_migration_database("catalog_cursor", install_extensions=False) as dsn:
        engine = create_engine(dsn)
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA market"))
                conn.execute(text("CREATE TABLE market.fact_archive_dependencies ("
                    "manifest_id text NOT NULL,target_kind text NOT NULL,target_id text NOT NULL,"
                    "object_key text NOT NULL,object_sha256 text NOT NULL,"
                    "PRIMARY KEY(manifest_id,target_kind,target_id))"))
                conn.execute(text("CREATE INDEX global_target ON market.fact_archive_dependencies(target_kind,target_id)"))
                # The leading target kind is the same across scopes. The old
                # tuple predicate can estimate a nearly empty global range and
                # choose the target-only index, filtering the scope afterward.
                conn.execute(text("INSERT INTO market.fact_archive_dependencies "
                    "SELECT 'page-'||lpad(p::text,3,'0'),'raw_manifest',md5(p::text||':'||n),"
                    "'object-'||p||'-'||n,repeat('a',64) "
                    "FROM generate_series(0,99) p CROSS JOIN generate_series(1,2400) n ORDER BY n,p"))
                conn.execute(text("CREATE TABLE market.fact_archive_prepared_material_aliases ("
                    "preparation_id text NOT NULL,fact_version_id text NOT NULL,series_id bigint NOT NULL,"
                    "evidence_key text NOT NULL,material_hash text NOT NULL,"
                    "PRIMARY KEY(preparation_id,fact_version_id,evidence_key))"))
                conn.execute(text("CREATE INDEX global_alias ON market.fact_archive_prepared_material_aliases(fact_version_id,evidence_key)"))
                conn.execute(text("INSERT INTO market.fact_archive_prepared_material_aliases "
                    "SELECT 'prep-'||lpad(p::text,3,'0'),md5(p::text||':'||n),p,'key-'||(n%3),repeat('b',64) "
                    "FROM generate_series(0,99) p CROSS JOIN generate_series(1,2400) n ORDER BY n,p"))
                conn.execute(text("ANALYZE market.fact_archive_dependencies"))
                conn.execute(text("ANALYZE market.fact_archive_prepared_material_aliases"))
            for table, scope, identity, spec in [
                ("fact_archive_dependencies", "manifest_id", "page-050", ARCHIVE_PAGE_CATALOG_SPECS[2]),
                ("fact_archive_prepared_material_aliases", "preparation_id", "prep-050", ARCHIVE_PAGE_CATALOG_SPECS[1]),
            ]:
                _, _, columns, order = spec
                keys = order.split(", ")
                with engine.begin() as conn:
                    conn.execute(text("SET LOCAL statement_timeout='1s'"))
                    conn.execute(text("SET LOCAL lock_timeout='100ms'"))
                    expected = [dict(row) for row in conn.execute(text(
                        f"SELECT {columns},ctid::text AS _handoff_tid,tableoid AS _handoff_oid "
                        f"FROM market.{table} WHERE {scope}=:id ORDER BY {order}"), {"id":identity}).mappings()]
                    assert len(expected) == 2400
                    # Start beyond a real first page. The identity must remain
                    # in the index condition, not merely a post-scan filter.
                    params = {"id":identity,"limit":1000,
                              **{f"after_{i}":expected[999][key] for i,key in enumerate(keys)}}
                    statement = archive_page_catalog_statement(table=table,columns=columns,order=order,handoff=True,after=True)
                    plan = conn.execute(text("EXPLAIN (FORMAT JSON) "+str(statement)),params).scalar_one()[0]["Plan"]
                    indexes = [node for node in _nodes(plan) if "Index Name" in node]
                    print("catalog_cursor_plan "+json.dumps({"table":table,"plan":plan},sort_keys=True))
                    assert indexes and all(scope in node.get("Index Cond", "") for node in indexes), plan
                    assert all(scope not in node.get("Filter", "") for node in indexes), plan
                    actual = expected[:1000]
                    while True:
                        chunk = [dict(row) for row in conn.execute(statement,params).mappings()]
                        assert len(chunk) <= 1000
                        actual.extend(chunk)
                        if len(chunk) < 1000:
                            break
                        params.update({f"after_{i}":chunk[-1][key] for i,key in enumerate(keys)})
                    # Includes every projected value, physical tuple and table
                    # identity; no duplicate, omitted or cross-scope rows.
                    assert actual == expected
                    assert len({tuple(row[key] for key in keys) for row in actual}) == len(actual)
        finally:
            engine.dispose()
