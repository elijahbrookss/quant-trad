"""Disposable indexed catalogs: exact keyset coverage and fresh proof bounds.

This isolates query shape; existing handoff/reclamation database tests own
transaction authority, immutable guards, source lineage and DROP refusal.
"""
from time import monotonic

import pytest
from sqlalchemy import create_engine, text

from portal.backend.service.storage.repos import fact_handoff as handoff
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


def test_representative_catalogs_keep_full_proofs_under_original_deadlines(tmp_path):
    with fresh_migration_database("catalog_bounds", install_extensions=False) as dsn:
        engine = create_engine(dsn)
        try:
            with engine.begin() as session:
                session.execute(text("CREATE SCHEMA market"))
                session.execute(text("CREATE TABLE market.fact_archive_active_generations "
                    "(manifest_id text PRIMARY KEY, preparation_id text, generation bigint, catalog_commitment text)"))
                for name, (legacy, columns) in handoff._CATALOGS.items():
                    tables = [legacy]
                    if name in {"aliases", "canonical_dependencies"}:
                        tables.append({"aliases": "fact_archive_prepared_material_aliases",
                                       "canonical_dependencies": "fact_archive_prepared_canonical_dependencies"}[name])
                    for table in tables:
                        identity = "preparation_id" if "prepared_" in table else "manifest_id"
                        keys = {"series": "series_id", "aliases": "fact_version_id,evidence_key",
                                "dependencies": "target_kind,target_id", "canonical_dependencies": "fact_version_id"}[name]
                        fields = ",".join(f"{column} {handoff._SQL_TYPES[kind]} NOT NULL" for column, kind in columns)
                        session.execute(text(f"CREATE TABLE market.{table} ({identity} text NOT NULL,{fields},"
                                             f"PRIMARY KEY({identity},{keys}))"))
                session.execute(text("INSERT INTO market.fact_archive_active_generations "
                    "SELECT 'page-'||p,'prep-'||p,1,repeat('a',64) FROM generate_series(4,372) p"))
                session.execute(text("INSERT INTO market.fact_archive_series SELECT 'page-'||p,p,1140,"
                    "now(),now(),now(),now(),now(),now() FROM generate_series(0,372) p"))
                # Header-only production metadata: 369 prepared pages, 22 alias
                # catalogs above4096, max4843, total1262279; dependency total418943.
                # The conservative synthetic shape also includes four legacy
                # pages. Physically interleave preparations to avoid clustering.
                for prepared in (False, True):
                    prefix, low, high = ("prep-",4,372) if prepared else ("page-",0,3)
                    aliases = "fact_archive_prepared_material_aliases" if prepared else "fact_archive_material_aliases"
                    dependencies = "fact_archive_prepared_canonical_dependencies" if prepared else "fact_archive_canonical_dependencies"
                    session.execute(text(f"INSERT INTO market.{aliases} "
                        f"SELECT '{prefix}'||p,'fact-'||r,p,'key',repeat('b',64) "
                        f"FROM generate_series({low},{high}) p CROSS JOIN LATERAL "
                        "generate_series(1,CASE WHEN p BETWEEN 4 AND 25 THEN 4843 ELSE 3333 END) r ORDER BY r,p"))
                    session.execute(text(f"INSERT INTO market.{dependencies} "
                        f"SELECT '{prefix}'||p,'fact-'||r,repeat('c',64) "
                        f"FROM generate_series({low},{high}) p CROSS JOIN generate_series(1,1140) r ORDER BY r,p"))
                    session.execute(text(f"ANALYZE market.{aliases}"))
                    session.execute(text(f"ANALYZE market.{dependencies}"))
            archive = PostgresCanonicalFactArchiveRepository(database=None,
                object_store=None, temporary_directory=tmp_path)
            proof = handoff.FactHandoffProof(check_budget=lambda: None)
            pages = []
            with engine.begin() as session:
                session.execute(text("SET LOCAL statement_timeout='5s'"))
                started = monotonic()
                for p in range(373):
                    identity = f"page-{p}"
                    catalogs, physical, generation = archive._page_catalogs(session, identity, handoff=proof)
                    alias_count = 4843 if 4 <= p <= 25 else 3333
                    assert len(catalogs["aliases"]) == alias_count
                    assert len(catalogs["canonical_dependencies"]) == 1140
                    assert (generation is None) == (p < 4)
                    assert [row["fact_version_id"] for row in catalogs["aliases"]] == sorted(
                        f"fact-{r}" for r in range(1,alias_count+1))
                    page, receipt = {"id": identity}, {"verification_hash": identity}
                    proof.capture_page(page, receipt, catalogs, physical, generation)
                    pages.append((page, receipt))
                capture_seconds = monotonic() - started
            # Catalog-only qualification uses the same locked final snapshot and
            # SQL deadline; real authority admission is covered separately.
            with engine.begin() as session:
                session.execute(text("SET LOCAL statement_timeout='1s'"))
                for table in ("fact_archive_active_generations", "fact_archive_series",
                              "fact_archive_dependencies", "fact_archive_material_aliases",
                              "fact_archive_canonical_dependencies", "fact_archive_prepared_material_aliases",
                              "fact_archive_prepared_canonical_dependencies"):
                    session.execute(text(f"LOCK TABLE market.{table} IN SHARE MODE NOWAIT"))
                proof.admitted = True
                started = monotonic()
                def deadline():
                    assert monotonic() - started < 10, "unchanged exclusive handoff budget"
                for offset in range(0,373,100):
                    proof.verify_catalog_batch(session, pages[offset:offset+100], deadline)
                seconds = monotonic()-started
                assert len(proof.fresh_catalogs) == 373
                print(f"catalog_query_qualification pages=373 prepared_pages=369 legacy_pages=4 alias_rows=1276429 dependency_rows=425220 "
                      f"capture_seconds={capture_seconds:.6f} final_seconds={seconds:.6f} "
                      f"witness_bytes={proof.retained_bytes}")
            # A late extra row remains an exact-count refusal, even after a
            # complete prior proof. No previous fresh mark is deletion authority.
            proof.fresh_catalogs.clear()
            with engine.begin() as session:
                session.execute(text("INSERT INTO market.fact_archive_prepared_canonical_dependencies "
                    "VALUES ('prep-372','late-extra',repeat('d',64))"))
                with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
                    proof.verify_catalog_batch(session, pages[300:], lambda: None)
                assert not proof.fresh_catalogs
        finally:
            engine.dispose()
