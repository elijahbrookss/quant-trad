from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.db.fact_storage_schema import (
    COLD_FACT_READ_BODY, FACT_ARCHIVE_GENERATION_TABLES,
    ensure_fact_payload_partition,
)
from portal.backend.service.storage.repos import fact_archival
from scripts.db.manual_migration_fact_archive_generations_v1 import migrate
from portal.backend.service.storage.repos.fact_archive_generation import (
    archive_preparation_id,
    catalog_batch_hash,
    catalog_commitment,
    canonical_dependency_projection,
    material_alias_projection,
)
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, _ingest


def test_generation_hashes_are_stable_and_scope_every_identity():
    identity = archive_preparation_id("cfa_" + "a" * 64, 2)
    assert identity == archive_preparation_id("cfa_" + "a" * 64, 2)
    assert identity != archive_preparation_id("cfa_" + "a" * 64, 3)
    rows = [{"fact_version_id": "f1", "row_hash": "1" * 64}]
    digest = catalog_batch_hash(
        preparation_id=identity, catalog_kind="canonical_dependencies", ordinal=0, rows=rows,
    )
    assert digest != catalog_batch_hash(
        preparation_id=identity, catalog_kind="canonical_dependencies", ordinal=1, rows=rows,
    )
    assert catalog_commitment(
        manifest_id="manifest", preparation_id=identity, generation=2,
        alias_count=0, canonical_dependency_count=1,
        alias_batches=[], canonical_dependency_batches=[digest],
    ) != catalog_commitment(
        manifest_id="manifest", preparation_id=identity, generation=2,
        alias_count=0, canonical_dependency_count=2,
        alias_batches=[], canonical_dependency_batches=[digest],
    )


def test_generation_projections_exclude_legacy_rows_only_when_pointer_exists():
    aliases = material_alias_projection("candidate_aliases")
    dependencies = canonical_dependency_projection("candidate_dependencies")
    assert "NOT EXISTS" in aliases and "fact_archive_active_generations" in aliases
    assert "fact_archive_prepared_material_aliases" in aliases
    assert "NOT EXISTS" in dependencies and "fact_archive_prepared_canonical_dependencies" in dependencies
    with pytest.raises(ValueError, match="projection_alias_invalid"):
        material_alias_projection("aliases;DROP TABLE x")


def _unfinished_generation(storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    _ingest(storage)
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database,
        object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
        temporary_directory=tmp_path / "staging",
    )
    archive.seal_partition(day)
    prepared = archive._prepare_next_page_plan(day)
    plan = prepared["_generation_plan"]
    if not plan["aliases"]:
        with storage.database.session() as session:
            fact_version_id = session.execute(text(
                "SELECT id FROM market.fact_identities ORDER BY id LIMIT 1"
            )).scalar_one()
        plan["aliases"] = ({
            "fact_version_id": fact_version_id,
            "evidence_key": "coordinator-fixture",
            "series_id": storage.series_id,
            "material_hash": "a" * 64,
        },)
    claim = archive._generation.claim(
        day=day, page_ordinal=plan["page_ordinal"], manifest=plan["manifest"],
        series_catalog=plan["series"], dependencies=plan["dependencies"],
        aliases=plan["aliases"],
        canonical_dependencies=plan["canonical_dependencies"],
    )
    return archive, day, plan, claim


@pytest.mark.db
def test_generation_claim_takeover_fences_stale_worker_and_wrong_day(
        storage, tmp_path, monkeypatch):
    archive, day, plan, original = _unfinished_generation(
        storage, tmp_path, monkeypatch,
    )
    other_day = day - timedelta(days=1)
    with storage.database.session() as session:
        ensure_fact_payload_partition(session.connection(), other_day)
    with pytest.raises(RuntimeError, match="preparation_day_changed"):
        archive._generation.abandon(
            day=other_day, preparation_id=original["preparation"]["id"],
            fence=original["preparation"]["fence"],
        )

    contender = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database,
        object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
        temporary_directory=tmp_path / "contender",
    )
    inputs = {
        "day": day, "page_ordinal": plan["page_ordinal"],
        "manifest": plan["manifest"], "series_catalog": plan["series"],
        "dependencies": plan["dependencies"], "aliases": plan["aliases"],
        "canonical_dependencies": plan["canonical_dependencies"],
    }
    with pytest.raises(RuntimeError, match="preparation_busy"):
        contender._generation.claim(**inputs)
    with storage.database.session() as session:
        session.execute(text("""
            UPDATE market.fact_archive_preparations
            SET lease_expires_at=clock_timestamp()-interval '1 second',
                updated_at=clock_timestamp()
            WHERE id=:id
        """), {"id": original["preparation"]["id"]})
    if original["aliases"]:
        expired_kind, expired_rows = "aliases", original["aliases"]
    else:
        expired_kind = "canonical_dependencies"
        expired_rows = original["canonical_dependencies"]
    with pytest.raises(RuntimeError, match="preparation_fence_lost"):
        archive._generation._stage_batch(
            day=day, preparation=original["preparation"],
            catalog_kind=expired_kind, ordinal=0, rows=expired_rows,
        )
    taken = contender._generation.claim(**inputs)
    assert taken["preparation"]["fence"] == original["preparation"]["fence"] + 1
    if original["aliases"]:
        kind, rows = "aliases", original["aliases"]
    else:
        kind, rows = "canonical_dependencies", original["canonical_dependencies"]
    with pytest.raises(RuntimeError, match="preparation_fence_lost"):
        archive._generation._stage_batch(
            day=day, preparation=original["preparation"], catalog_kind=kind,
            ordinal=0, rows=rows,
        )


@pytest.mark.db
def test_expired_preparation_from_older_verifier_is_abandoned_not_reused(
        storage, tmp_path, monkeypatch):
    archive, day, plan, old = _unfinished_generation(
        storage, tmp_path, monkeypatch,
    )
    old_version = archive._generation.verifier_version
    archive._generation.verifier_version = "fixture.verifier.next"
    inputs = {
        "day": day, "page_ordinal": plan["page_ordinal"],
        "manifest": plan["manifest"], "series_catalog": plan["series"],
        "dependencies": plan["dependencies"], "aliases": plan["aliases"],
        "canonical_dependencies": plan["canonical_dependencies"],
    }
    with pytest.raises(RuntimeError, match="preparation_busy"):
        archive._generation.claim(**inputs)
    with storage.database.session() as session:
        session.execute(text("""
            UPDATE market.fact_archive_preparations
            SET lease_expires_at=clock_timestamp()-interval '1 second',
                updated_at=clock_timestamp()
            WHERE id=:id
        """), {"id": old["preparation"]["id"]})
    replacement = archive._generation.claim(**inputs)
    assert replacement["preparation"]["generation"] == 2
    assert replacement["preparation"]["verifier_version"] == "fixture.verifier.next"
    assert replacement["preparation"]["source_binding_hash"] != old["preparation"]["source_binding_hash"]
    with storage.database.session() as session:
        rows = session.execute(text("""
            SELECT generation,state,verifier_version
            FROM market.fact_archive_preparations
            WHERE storage_day=:day AND page_ordinal=:page ORDER BY generation
        """), {"day": day, "page": plan["page_ordinal"]}).all()
        assert rows == [
            (1, "abandoned", old_version),
            (2, "building", "fixture.verifier.next"),
        ]


@pytest.mark.db
def test_ready_validation_rejects_count_preserving_catalog_change(
        storage, tmp_path, monkeypatch):
    archive, day, _plan, claim = _unfinished_generation(
        storage, tmp_path, monkeypatch,
    )
    assert claim["aliases"]
    archive._generation._stage_batch(
        day=day, preparation=claim["preparation"], catalog_kind="aliases",
        ordinal=0, rows=claim["aliases"],
    )
    if claim["canonical_dependencies"]:
        archive._generation._stage_batch(
            day=day, preparation=claim["preparation"],
            catalog_kind="canonical_dependencies", ordinal=0,
            rows=claim["canonical_dependencies"],
        )
    with storage.database.session() as session:
        session.execute(text(
            "ALTER TABLE market.fact_archive_prepared_material_aliases "
            "DISABLE TRIGGER trg_guard_fact_archive_prepared_material_aliases"
        ))
        session.execute(text("""
            UPDATE market.fact_archive_prepared_material_aliases
            SET material_hash=:changed WHERE preparation_id=:id
        """), {"id": claim["preparation"]["id"], "changed": "0" * 64})
        session.execute(text(
            "ALTER TABLE market.fact_archive_prepared_material_aliases "
            "ENABLE ALWAYS TRIGGER trg_guard_fact_archive_prepared_material_aliases"
        ))
    with pytest.raises(RuntimeError, match="catalog_batch_receipt_changed|root_mismatch"):
        archive._generation.mark_ready(
            day=day, preparation=claim["preparation"],
        )


@pytest.mark.db
def test_generation_migration_from_legacy_layout_is_idempotent_and_sets_reader_floor(storage):
    old_reader_body = COLD_FACT_READ_BODY.replace(
        "-- archive-generation-reader-floor-v1: older binaries must refuse this layout.\n",
        "",
    )
    with storage.database.session() as session:
        for name in reversed(FACT_ARCHIVE_GENERATION_TABLES):
            session.execute(text(f"DROP TABLE market.{name}"))
        session.execute(text(
            "CREATE OR REPLACE FUNCTION market.cold_fact_read_required"
            "(fact_id text, field_name text) RETURNS jsonb LANGUAGE plpgsql AS $qt$"
            + old_reader_body + "$qt$"
        ))
        assert session.execute(text("""
            SELECT p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='market' AND p.proname='cold_fact_read_required'
        """)).scalar_one() == old_reader_body
        assert all(session.execute(text("SELECT to_regclass(:name)"), {
            "name": "market." + name,
        }).scalar_one_or_none() is None for name in FACT_ARCHIVE_GENERATION_TABLES)

    first = migrate(storage.database)
    second = migrate(storage.database)
    assert first == second
    assert first["historical_rows_rewritten"] is False
    placements = first["catalog_tablespaces"]
    assert placements["fact_archive_prepared_material_aliases"]
    assert placements["fact_archive_prepared_canonical_dependencies"]
    with storage.database.session() as session:
        body = session.execute(text("""
            SELECT p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='market' AND p.proname='cold_fact_read_required'
        """)).scalar_one()
        assert body == COLD_FACT_READ_BODY
        assert body != old_reader_body
        assert "archive-generation-reader-floor-v1" in body
        assert all(session.execute(text("SELECT to_regclass(:name)"), {
            "name": "market." + name,
        }).scalar_one_or_none() is not None for name in FACT_ARCHIVE_GENERATION_TABLES)


@pytest.mark.db
def test_interrupted_catalog_batch_is_resumed_then_activated_without_legacy_copy(
        storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    _ingest(storage)
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database,
        object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
        temporary_directory=tmp_path / "staging",
    )
    archive.seal_partition(day)
    monkeypatch.setattr(fact_archival, "legacy_material_alias", lambda row: {
        "fact_version_id": row["id"],
        "evidence_key": "generation-interruption-fixture",
        "series_id": row["series_id"],
        "material_hash": row["material_hash"],
    })
    original = archive._generation._stage_batch
    failed = False

    def fail_after_committed_batch(**kwargs):
        nonlocal failed
        result = original(**kwargs)
        if not failed:
            failed = True
            raise RuntimeError("fixture_interrupt_after_commit")
        return result

    monkeypatch.setattr(archive._generation, "_stage_batch", fail_after_committed_batch)
    with pytest.raises(RuntimeError, match="fixture_interrupt_after_commit"):
        archive.stage_next_page(day)
    with storage.database.session() as session:
        preparation = session.execute(text(
            "SELECT * FROM market.fact_archive_preparations WHERE storage_day=:day"
        ), {"day": day}).mappings().one()
        assert preparation["state"] == "building"
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_catalog_batches WHERE preparation_id=:id"
        ), {"id": preparation["id"]}).scalar_one() == 1
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_manifests WHERE storage_day=:day"
        ), {"day": day}).scalar_one() == 0

    monkeypatch.setattr(archive._generation, "_stage_batch", original)
    result = archive.stage_next_page(day)
    assert result["status"] == "page_acknowledged" and result["catalog_generation"] == 1
    with storage.database.session() as session:
        active = session.execute(text(
            "SELECT * FROM market.fact_archive_active_generations WHERE manifest_id=:id"
        ), {"id": result["manifest_id"]}).mappings().one()
        assert active["preparation_id"] == preparation["id"]
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_material_aliases WHERE manifest_id=:id"
        ), {"id": result["manifest_id"]}).scalar_one() == 0
        catalogs = archive._page_catalogs(session, result["manifest_id"])
        assert catalogs["series"] and len(catalogs["aliases"]) == preparation["alias_count"]
        fact_version_id = session.execute(text(
            "SELECT id FROM market.fact_identities ORDER BY id LIMIT 1"
        )).scalar_one()
        with pytest.raises(DBAPIError) as refused:
            session.execute(text(
                "INSERT INTO market.fact_archive_prepared_material_aliases "
                "(preparation_id,fact_version_id,evidence_key,batch_ordinal,series_id,material_hash) "
                "VALUES (:id,:fact,'late-valid-fk',99,:series,:hash)"
            ), {"id": preparation["id"], "fact": fact_version_id,
                 "series": storage.series_id, "hash": "f" * 64})
        assert "prepared archive insert refused" in str(refused.value.orig)
