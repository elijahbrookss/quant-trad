"""Physical publication handoff for a verified canonical storage day."""
from __future__ import annotations

from datetime import date
from enum import Enum

from sqlalchemy import text

from market_data.cold_history import ColdHistoryReader
from portal.backend.db.fact_identity_schema import _partition_bound, fact_header_partition_name
from .cold_history import PostgresColdHistory
from .fact_storage import _catalog_manifest, ensure_payload_contracts


class HeaderRegistry(Enum):
    DAILY = "fact_header_partitions"
    LEGACY = "fact_header_legacy"


def lock_header_namespace(executor):
    executor.execute(text("LOCK TABLE ONLY market.fact_versions IN ACCESS EXCLUSIVE MODE NOWAIT"))
    for table in ("fact_header_partitions", "fact_header_series_days", "fact_header_legacy"):
        executor.execute(text(f'LOCK TABLE market."{table}" IN ACCESS EXCLUSIVE MODE NOWAIT'))


def require_oldest_header_day(session, day):
    if session.scalar(text("""
        SELECT EXISTS (SELECT FROM market.fact_header_partitions WHERE storage_day<:day)
            OR market.fact_header_legacy_end_day() IS NOT NULL
    """), {"day": day}):
        raise RuntimeError(f"cold_fact_retirement_requires_oldest_day: storage_day={day}")


def stage_retirement_pages(session, archive, day, *, admission=None):
    """Admit a bounded slice of pages while SQL authority stays live."""
    sources = session.execute(text("""
        SELECT old.* FROM market.fact_archive_manifests AS old
        WHERE old.storage_day=:day AND NOT EXISTS (
            SELECT FROM market.cold_fact_pages AS cold WHERE cold.id=old.id
        )
        ORDER BY old.page_ordinal LIMIT 8
    """), {"day": day}).mappings().all()
    if not sources:
        return None
    check_resources = admission(session) if admission is not None else (lambda: None)
    repository = PostgresColdHistory(ColdHistoryReader(
        archive.object_store, limits=archive.limits,
        ensure_contracts=lambda pairs: ensure_payload_contracts(session, pairs),
    ))
    rows = 0
    for source in sources:
        archive._check_budget()
        page = _catalog_manifest(source)
        repository.admit(session, page, staged_storage_day=day)
        rows += page.row_count
        check_resources()
    archive._check_budget()
    return {"status": "cold_page_admitted", "storage_day": day,
            "manifest_id": page.manifest_id, "page_count": len(sources), "row_count": rows}


def verify_cold_day_locators(session, day, expected_rows, *, check_budget):
    """Match published pages and bound ordinal endpoints using indexed lookups."""
    rows = session.execute(text("""
        SELECT old.id,old.row_count,cold.id AS matched,
               EXISTS (
                   SELECT FROM market.cold_fact_locators loc
                   WHERE loc.page_id=cold.id AND loc.ordinal=cold.row_count-1
               ) AS last_present,
               EXISTS (
                   SELECT FROM market.cold_fact_locators loc
                   WHERE loc.page_id=cold.id AND loc.ordinal>=cold.row_count
               ) AS overflow
        FROM market.fact_archive_manifests old
        LEFT JOIN market.cold_fact_pages cold
          ON (cold.id,cold.object_key,cold.object_sha256,cold.manifest_hash,cold.row_count,cold.staged_storage_day)
           = (old.id,old.object_key,old.object_sha256,old.manifest_hash,old.row_count,old.storage_day)
        WHERE old.storage_day=:day ORDER BY old.page_ordinal
    """), {"day": day}).mappings().all()
    staged_count = session.scalar(text(
        "SELECT count(*) FROM market.cold_fact_pages WHERE staged_storage_day=:day"
    ), {"day": day})
    if len(rows) != staged_count or sum(row["row_count"] for row in rows) != expected_rows:
        raise RuntimeError(f"cold_fact_retirement_incomplete_coverage: storage_day={day}")
    # Check each count via that page's (page_id,ordinal) index before the
    # time-limited exclusive handoff. Recheck the compact proof under the locks.
    for row in rows:
        check_budget()
        if row["matched"] is None or not row["last_present"] or row["overflow"]:
            raise RuntimeError(f"cold_fact_retirement_incomplete_coverage: storage_day={day}")
    return tuple(row["id"] for row in rows)


def verify_staged_locator_counts(session, pages, *, check_budget):
    for offset in range(0, len(pages), 16):
        check_budget()
        mismatch = session.scalar(text("""
            SELECT page.id FROM market.cold_fact_pages AS page
            CROSS JOIN LATERAL (
                SELECT count(*) AS rows FROM market.cold_fact_locators loc WHERE loc.page_id=page.id
            ) AS coverage
            WHERE page.id=ANY(:ids) AND coverage.rows<>page.row_count LIMIT 1
        """), {"ids": list(pages[offset:offset + 16])})
        if mismatch is not None:
            raise RuntimeError(f"cold_fact_retirement_locator_count_changed: manifest_id={mismatch}")


def _remove_registry_rows(executor, table, *, day):
    if table is HeaderRegistry.LEGACY:
        where = "id=1"
        allowed = {"trg_guard_fact_header_legacy": "A"}
    else:
        where = "storage_day=:day"
        allowed = {"trg_guard_fact_header_partitions": "A", "trg_reject_mutation_fact_header_partitions": "O"}
    statuses = _matching_delete_guards(executor, table.value, allowed)
    result = _delete_with_guards(executor, table.value, statuses, where, {"day": day})
    if result.rowcount != 1:
        raise RuntimeError(f"cold_fact_retirement_header_registry_changed: relation={table.value} day={day}")


def _matching_delete_guards(executor, table, allowed):
    states = dict(executor.execute(text("""
        SELECT tgname,tgenabled FROM pg_trigger WHERE tgrelid=to_regclass(:table)
          AND NOT tgisinternal AND (tgtype & 8)<>0
    """), {"table": f"market.{table}"}).all())
    required = next(iter(allowed))
    if states.get(required) != allowed[required] or any(
        name not in allowed or state != allowed[name] for name, state in states.items()
    ):
        raise RuntimeError(f"cold_fact_retirement_mutation_guards_changed: relation={table}")
    return states


def _delete_with_guards(executor, table, statuses, condition, values, *, sql=None):
    for name in sorted(statuses):
        executor.execute(text(f'ALTER TABLE market."{table}" DISABLE TRIGGER "{name}"'))
    result = executor.execute(text(sql or f'DELETE FROM market."{table}" WHERE {condition}'), values)
    for name, status in sorted(statuses.items()):
        mode = "ALWAYS " if status == "A" else ""
        executor.execute(text(f'ALTER TABLE market."{table}" ENABLE {mode}TRIGGER "{name}"'))
    return result


def drop_verified_header(executor, *, name, oid, storage_day, legacy_end_day=None):
    """Retire the pinned header child and its routing, without cascading."""
    legacy = legacy_end_day is not None
    bound = (f"FOR VALUES FROM (MINVALUE) TO ('{legacy_end_day.isoformat()}')" if legacy
             else _partition_bound(storage_day))
    expected_name = "fact_versions_legacy" if legacy else fact_header_partition_name(storage_day)
    if name != expected_name:
        raise RuntimeError(f"cold_fact_retirement_header_name_invalid: relation={name}")
    _assert_partition(executor, name, oid, bound, "fact_versions")
    size = executor.scalar(text("SELECT pg_total_relation_size(:oid)"), {"oid": oid})
    executor.execute(text(f'DROP TABLE market."{name}"'))
    if legacy:
        _remove_registry_rows(executor, HeaderRegistry.LEGACY, day=storage_day)
    else:
        table = "fact_header_series_days"
        states = _matching_delete_guards(executor, table, {"trg_guard_fact_header_series_day": "A"})
        _delete_with_guards(executor, table, states, "storage_day=:day", {"day": storage_day})
        _remove_registry_rows(executor, HeaderRegistry.DAILY, day=storage_day)
    return int(size)


def _assert_partition(executor, name, oid, bound, parent):
    executor.execute(text(f'LOCK TABLE ONLY market."{name}" IN ACCESS EXCLUSIVE MODE NOWAIT'))
    actual = executor.execute(text("""
        SELECT c.oid::bigint,pg_get_expr(c.relpartbound,0),i.inhparent=to_regclass(:parent)
        FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid
        WHERE c.oid=to_regclass(:relation) AND c.relkind='r' AND c.relpersistence='p'
          AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
    """), {"relation": f"market.{name}", "parent": f"market.{parent}"}).one_or_none()
    if actual is None or tuple(actual) != (oid, bound, True):
        raise RuntimeError(f"cold_fact_retirement_partition_binding_invalid: relation={name}")


def prune_cold_identities(session, day, *, max_rows):
    """Delete exact matching obsolete registry rows in a bounded transaction."""
    if type(day) is not date or type(max_rows) is not int or not 1 <= max_rows <= 100_000:
        raise ValueError("cold_fact_identity_prune_scope_invalid")
    session.execute(text("LOCK TABLE market.fact_identities IN ACCESS EXCLUSIVE MODE NOWAIT"))
    if not session.scalar(text("""
        SELECT EXISTS (SELECT FROM market.cold_fact_retired_days WHERE storage_day=:day)
          AND NOT EXISTS (SELECT FROM market.fact_header_partitions WHERE storage_day=:day)
    """), {"day": day}):
        raise RuntimeError(f"cold_fact_identity_prune_day_not_retired: storage_day={day}")
    table = "fact_identities"
    states = _matching_delete_guards(session, table, {
        "trg_guard_fact_identities": "A", "trg_reject_mutation_fact_identities": "O",
    })
    count = _delete_with_guards(session, table, states, "", {"day": day, "limit": max_rows}, sql="""
        WITH candidates AS MATERIALIZED (
            SELECT id,series_id,revision,observation_key FROM market.fact_identities
            WHERE storage_day=:day ORDER BY id LIMIT :limit
        )
        DELETE FROM market.fact_identities AS hot
        USING candidates AS chosen, market.cold_fact_locators AS loc, market.cold_fact_pages AS page
        WHERE hot.id=chosen.id AND loc.id=chosen.id AND page.id=loc.page_id
          AND page.staged_storage_day=:day AND loc.series_id=chosen.series_id AND loc.revision=chosen.revision
          AND loc.key_digest=sha256(convert_to(chosen.observation_key,'UTF8'))
    """).rowcount
    if count == 0 and session.scalar(text(
        "SELECT EXISTS (SELECT FROM market.fact_identities WHERE storage_day=:day)"
    ), {"day": day}):
        raise RuntimeError(f"cold_fact_identity_prune_missing_locator: storage_day={day}")
    return count


def prune_oldest_retired_identities(database, *, deadline):
    """Own one bounded cleanup batch with a short, retryable writer fence."""
    from .cold_history import cold_layout_active
    from .market_lifecycle import _LIFECYCLE_LOCK_NAME, MarketStorageLifecycleBusyError

    deadline()
    with database.session() as session:
        if not cold_layout_active(session):
            return None
        session.execute(text("SET LOCAL statement_timeout='1000ms'"))
        session.execute(text("SET LOCAL lock_timeout='250ms'"))
        day = session.scalar(text("""
            SELECT retired.storage_day FROM market.cold_fact_retired_days AS retired
            LEFT JOIN market.cold_fact_pruned_days AS pruned USING(storage_day)
            WHERE retired.handoff_kind='recurring' AND pruned.storage_day IS NULL
            ORDER BY retired.storage_day LIMIT 1
        """))
        if day is None:
            return None
        if not session.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                              {"name": _LIFECYCLE_LOCK_NAME}):
            raise MarketStorageLifecycleBusyError("cold_fact_identity_prune_lifecycle_busy")
        if session.scalar(text(
            "SELECT EXISTS (SELECT FROM market.cold_fact_pruned_days WHERE storage_day=:day)"
        ), {"day": day}):
            return None
        removed = prune_cold_identities(session, day, max_rows=10_000)
        finished = not session.scalar(text(
            "SELECT EXISTS (SELECT FROM market.fact_identities WHERE storage_day=:day)"
        ), {"day": day})
        if finished:
            session.execute(text("INSERT INTO market.cold_fact_pruned_days VALUES (:day)"), {"day": day})
        deadline()
        return {"status": "cold_identity_pruned", "storage_day": day.isoformat(), "rows_removed": removed,
                "day_complete": finished}
