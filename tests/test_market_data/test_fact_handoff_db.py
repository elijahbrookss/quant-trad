"""Disposable PostgreSQL parity and transaction-gap refusal regressions."""
from datetime import date, datetime, timedelta, timezone
import hashlib

import pytest
from sqlalchemy import text

from portal.backend.service.storage.repos.fact_handoff import _record
from tests.test_market_data.test_fact_reclamation_db import _prepare, _physical
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


@pytest.mark.parametrize("values", [
    {"s": "猫🥚: \n", "i": 2**63 - 1, "t": datetime(1999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
     "d": date(1999, 12, 31), "b": False},
    {"s": "", "i": -(2**63), "t": datetime(2026, 1, 1, 0, 0, 0, 123456,
        tzinfo=timezone(timedelta(hours=5, minutes=45))), "d": date(2000, 1, 1), "b": True},
    {"s": None, "i": None, "t": None, "d": None, "b": None},
])
def test_typed_record_fingerprint_matches_postgresql_exact_bytes(storage, values):
    columns = (("s", "text"), ("i", "int"), ("t", "time"), ("d", "date"), ("b", "bool"))
    with storage.database.session() as session:
        actual = session.execute(text("""
            SELECT encode(sha256(pg_catalog.record_send(ROW(
                CAST(:s AS text),CAST(:i AS bigint),CAST(:t AS timestamptz),
                CAST(:d AS date),CAST(:b AS boolean)))),'hex')
        """), values).scalar_one()
    assert actual == hashlib.sha256(_record(values, columns)).hexdigest()


@pytest.mark.parametrize("change", ["remove", "replace", "rewrite"])
def test_transaction_gap_mutation_or_rewrite_refuses_and_rolls_back(storage, tmp_path, monkeypatch, change):
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch)
    initial, physical = archive.inspect_partition(day), _physical(storage, day)
    timeouts = reclaimer._timeouts
    calls = 0
    def inject(session):
        nonlocal calls
        timeouts(session)
        calls += 1
        if calls != 2:
            return
        if change == "rewrite":
            # Exact same logical rows/count and same relation OID, new filenode.
            # This is an isolated fixture; no CASCADE or production input.
            session.execute(text("CREATE TEMP TABLE handoff_series_copy ON COMMIT DROP AS TABLE market.fact_archive_series"))
            session.execute(text("TRUNCATE market.fact_archive_series"))
            session.execute(text("INSERT INTO market.fact_archive_series SELECT * FROM handoff_series_copy"))
        else:
            session.execute(text("ALTER TABLE market.fact_archive_series DISABLE TRIGGER trg_reject_mutation_fact_archive_series"))
            if change == "remove":
                changed = session.execute(text("""
                    DELETE FROM market.fact_archive_series WHERE ctid IN (
                        SELECT ctid FROM market.fact_archive_series LIMIT 1)
                """))
            else:
                changed = session.execute(text("""
                    UPDATE market.fact_archive_series SET row_count=row_count+1 WHERE ctid IN (
                        SELECT ctid FROM market.fact_archive_series LIMIT 1)
                """))
            assert changed.rowcount == 1
            session.execute(text("ALTER TABLE market.fact_archive_series ENABLE TRIGGER trg_reject_mutation_fact_archive_series"))
    monkeypatch.setattr(reclaimer, "_timeouts", inject)
    with pytest.raises(RuntimeError, match="verification_missing_or_stale|handoff_relation_changed"):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert calls == 2
    assert archive.inspect_partition(day) == initial
    after = _physical(storage, day)
    assert (after["relation"], after["hot_rows"], after["header_rows"]) == (
        physical["relation"], physical["hot_rows"], physical["header_rows"])
    with storage.database.session() as session:
        assert session.execute(text("""
            SELECT tgenabled FROM pg_trigger WHERE tgrelid='market.fact_archive_series'::regclass
              AND tgname='trg_reject_mutation_fact_archive_series'
        """)).scalar_one() == "O"


@pytest.mark.parametrize("change", ["disabled_fk", "hot_removed", "cold_overlap"])
def test_source_handoff_requires_current_binding_placement_and_unique_cold_coverage(storage, tmp_path, monkeypatch, change):
    from portal.backend.db.fact_identity_schema import fact_header_partition_name
    from portal.backend.db.fact_storage_schema import fact_partition_name
    from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
    from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
    from tests.test_market_data.test_fact_flow_retention_db import _flow_fixture
    from tests.test_market_data.test_fact_derived_retention_db import _archive_day

    flow = _flow_fixture(storage, tmp_path, monkeypatch)
    archive = PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=flow.store,
        temporary_directory=tmp_path / "canonical-staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    if change == "cold_overlap":
        _archive_day(archive, flow.source_day)
        assert reclaimer.reclaim_partition(flow.source_day, eligible_before=storage.today,
                                           execute=True)["status"] == "partition_reclaimed"
    _archive_day(archive, flow.flow_day)
    initial, physical = archive.inspect_partition(flow.flow_day), _physical(storage, flow.flow_day)
    timeouts = reclaimer._timeouts
    calls = 0
    def inject(session):
        nonlocal calls
        timeouts(session)
        calls += 1
        if calls != 2:
            return
        if change == "disabled_fk":
            leaf = fact_header_partition_name(flow.source_day)
            trigger = session.execute(text("""
                SELECT t.tgname FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
                JOIN pg_constraint c ON c.oid=t.tgconstraint
                WHERE t.tgrelid=to_regclass(:relation) AND p.proname='RI_FKey_check_ins'
                  AND c.confrelid='market.fact_identities'::regclass
            """), {"relation": "market." + leaf}).scalar_one()
            quoted = trigger.replace('"', '""')
            session.execute(text(f'ALTER TABLE market."{leaf}" DISABLE TRIGGER "{quoted}"'))
        elif change == "hot_removed":
            leaf = fact_partition_name(flow.source_day)
            session.execute(text(f'TRUNCATE market."{leaf}"'))
        else:
            # Respect descriptor binding and unique object keys. This must reach
            # the fresh cold-coverage refusal, not fail during fixture INSERT.
            session.execute(text("""
                INSERT INTO market.fact_archive_manifests
                    (id,storage_day,page_ordinal,object_key,object_sha256,manifest_hash,row_count,
                     byte_count,first_commit_seq,first_id,last_commit_seq,last_id,descriptor,acknowledged_at)
                SELECT :id,storage_day,(SELECT max(page_ordinal)+1 FROM market.fact_archive_manifests
                    WHERE storage_day=:day),object_key||'.handoff-overlap',object_sha256,manifest_hash,row_count,
                    byte_count,first_commit_seq,first_id,last_commit_seq,last_id,
                    descriptor||jsonb_build_object('manifest_id',CAST(:id AS text),
                        'object_key',object_key||'.handoff-overlap'),acknowledged_at
                FROM market.fact_archive_manifests WHERE storage_day=:day ORDER BY page_ordinal LIMIT 1
            """), {"id": "fam_" + "f" * 64, "day": flow.source_day})
    monkeypatch.setattr(reclaimer, "_timeouts", inject)
    with pytest.raises(RuntimeError, match="identity_enforcement_invalid|source_placement_changed|reference_binding_invalid"):
        reclaimer.reclaim_partition(flow.flow_day, eligible_before=storage.today, execute=True)
    assert calls == 2
    assert archive.inspect_partition(flow.flow_day) == initial
    after = _physical(storage, flow.flow_day)
    assert (after["relation"], after["hot_rows"], after["header_rows"]) == (
        physical["relation"], physical["hot_rows"], physical["header_rows"])


@pytest.mark.parametrize("mutate_last_page", [False, True])
def test_batched_final_catalogs_keep_multipage_history_and_late_mutation_refusal(
        storage, tmp_path, monkeypatch, mutate_last_page):
    from sqlalchemy import event
    from market_data.contracts import DatasetSeriesRequest
    from tests.test_market_data.test_fact_storage_tiers_db import BASE
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch, rows=257)
    frozen = storage.repo.freeze_dataset([
        DatasetSeriesRequest(storage.series_id, BASE - timedelta(days=1), BASE + timedelta(days=1))])
    revisions = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    initial, physical = archive.inspect_partition(day), _physical(storage, day)
    original_timeouts = reclaimer._timeouts
    phases = 0
    statements = []
    def timeouts(session):
        nonlocal phases
        original_timeouts(session)
        phases += 1
        if phases == 2 and mutate_last_page:
            # The last page crosses the16-manifest batch boundary. It must not
            # inherit another page's empty-alias proof or first-pass authority.
            session.execute(text(
                "ALTER TABLE market.fact_archive_verification_generations DISABLE TRIGGER "
                "trg_reject_mutation_fact_archive_verification_generations"
            ))
            session.execute(text("""
                DELETE FROM market.fact_archive_verification_generations
                WHERE manifest_id=(
                    SELECT id FROM market.fact_archive_manifests WHERE storage_day=:day
                    ORDER BY page_ordinal DESC LIMIT 1
                )
            """), {"day":day})
            session.execute(text(
                "ALTER TABLE market.fact_archive_verification_generations ENABLE ALWAYS TRIGGER "
                "trg_reject_mutation_fact_archive_verification_generations"
            ))
    def query(_connection, _cursor, statement, *_):
        if phases == 2:
            statements.append(statement)
    monkeypatch.setattr(reclaimer, "_timeouts", timeouts)
    event.listen(storage.database._engine, "before_cursor_execute", query)
    try:
        if mutate_last_page:
            with pytest.raises(RuntimeError, match="verification_generation_changed"):
                reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
            assert archive.inspect_partition(day) == initial
            after = _physical(storage, day)
            assert (after["relation"],after["hot_rows"],after["header_rows"]) == (
                physical["relation"],physical["hot_rows"],physical["header_rows"])
        else:
            result = reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
            assert result["status"] == "partition_reclaimed" and result["page_count"] == 17
            assert _physical(storage, day)["relation"] is None
            assert result["reclaimed_bytes"] > 0
    finally:
        event.remove(storage.database._engine, "before_cursor_execute", query)
    catalogs = [q for q in statements if "AS logical_count" in q and "AS hinted_count" in q]
    receipts = [q for q in statements if "FROM market.fact_archive_verifications" in q]
    assert len(receipts) == 1
    assert len(catalogs) == (4 if mutate_last_page else 8)
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == revisions
