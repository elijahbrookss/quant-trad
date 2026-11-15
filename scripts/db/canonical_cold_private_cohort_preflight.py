"""Plan one historical cohort on a promoted private clone using SQL metadata only.

Use the resulting window to plan the existing stage/load/audit sequence in
docs/engineering/canonical-cold-staging.md. Raw and frozen custody need the
separately scoped scripts.db.canonical_cold_custody_audit.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
import re
from time import monotonic

from sqlalchemy import create_engine, text

from scripts.db.canonical_cold_migration import INITIAL


SCHEMA = "qt.canonical_cold_private_cohort_preflight.v1"
TAIL_BATCH_ROWS = 4096
_TAIL = """
    WITH wanted AS MATERIALIZED (
        SELECT storage_day, market_commit_seq, id FROM market.fact_versions
        WHERE storage_day=:day AND (market_commit_seq,id) > (:after_seq,:after_id)
          {before_clause}
        ORDER BY market_commit_seq,id LIMIT :limit
    )
    SELECT f.market_commit_seq, f.id, hot.id IS NOT NULL AS has_payload
    FROM wanted f LEFT JOIN LATERAL (
        SELECT h.id FROM market.fact_hot_payloads h
        WHERE h.storage_day=f.storage_day AND h.id=f.id OFFSET 0
    ) hot ON true
    ORDER BY f.market_commit_seq,f.id
"""


def _tail_query(conn, day, after, before, limit):
    params = {"day": day, "after_seq": after[0], "after_id": after[1], "limit": limit}
    before_clause = ""
    if before is not None:
        params.update(before_seq=before[0], before_id=before[1])
        before_clause = "AND (market_commit_seq,id) < (:before_seq,:before_id)"
    return conn.execute(text(_TAIL.format(before_clause=before_clause)), params).mappings().all()


def _manifest_prefix(pages, limit):
    prefix = {"rows": 0, "pages": 0, "pages_with_active_generation_pointer": 0, "last_cursor": INITIAL}
    for ordinal, page in enumerate(pages):
        if page["page_ordinal"] != ordinal:
            return prefix, {"state": "archive_ordinal_gap", "missing_ordinal": ordinal,
                            "next_published_ordinal": page["page_ordinal"]}
        first = (page["first_commit_seq"], page["first_id"])
        last = (page["last_commit_seq"], page["last_id"])
        if not prefix["last_cursor"] < first <= last or page["row_count"] < 1:
            return prefix, {"state": "archive_cursor_invalid", "ordinal": ordinal, "manifest_id": page["id"]}
        if ordinal == limit:
            return prefix, {"state": "archive_window_partial", "next_published_ordinal": ordinal}
        prefix.update(rows=prefix["rows"] + page["row_count"], pages=ordinal + 1, last_cursor=last)
        prefix["pages_with_active_generation_pointer"] += int(page["has_active_generation"])
    return prefix, None


def inspect_private_cohort(conn, *, day, max_pages, max_tail_rows, check, after=None, before=None):
    """Inspect one exact day in an already-guarded private read-only snapshot."""
    if (type(day) is not date or type(max_pages) is not int or not 1 <= max_pages <= 4096
            or type(max_tail_rows) is not int or not 1 <= max_tail_rows <= 1_000_000):
        raise ValueError("private cohort preflight requires an exact day and bounded page and row limits")
    check()
    params = {"day": day}
    retention = conn.execute(text("""
        SELECT state,sealed_at,expected_rows FROM market.fact_retention_partitions WHERE storage_day=:day
    """), params).mappings().one_or_none()
    report = {"schema": SCHEMA, "storage_day": day, "retention": dict(retention) if retention else None,
              "requested": {"after_exclusive": after, "before_exclusive": before,
                            "max_pages": max_pages, "max_tail_rows": max_tail_rows},
              "published_prefix": None, "tail": None}
    if retention is None:
        return report | {"state": "retention_day_missing"}
    if retention["state"] not in {"sealed", "verified", "reclaimed"}:
        return report | {"state": "retention_day_unsealed"}
    expected = retention["expected_rows"]
    if expected is None or expected < 0 or retention["sealed_at"] is None:
        return report | {"state": "retention_seal_invalid"}

    check()
    pages = conn.execute(text("""
        SELECT m.id,m.page_ordinal,m.row_count,m.first_commit_seq,m.first_id,m.last_commit_seq,m.last_id,
               EXISTS (SELECT 1 FROM market.fact_archive_active_generations g WHERE g.manifest_id=m.id)
                    AS has_active_generation
        FROM market.fact_archive_manifests m WHERE m.storage_day=:day ORDER BY m.page_ordinal LIMIT :limit
    """), params | {"limit": max_pages + 1}).mappings().all()
    prefix, problem = _manifest_prefix(pages, max_pages)
    report["published_prefix"] = prefix
    if problem is not None:
        return report | problem
    expected_tail = expected - prefix["rows"]
    report["expected_uncovered_rows"] = expected_tail
    if expected_tail < 0:
        return report | {"state": "sealed_row_count_mismatch"}
    if retention["state"] == "reclaimed" and expected_tail:
        return report | {"state": "reclaimed_archive_missing"}
    if retention["state"] == "verified" and expected_tail:
        return report | {"state": "verified_archive_missing"}

    lower = prefix["last_cursor"]
    after = after if after is not None else lower
    if after < lower or (before is not None and before <= after):
        return report | {"state": "requested_cursor_outside_tail"}
    if after > lower:
        check()
        if not conn.execute(text("""
            SELECT EXISTS (SELECT 1 FROM market.fact_versions WHERE storage_day=:day
                           AND market_commit_seq=:seq AND id=:id)
        """), params | {"seq": after[0], "id": after[1]}).scalar_one():
            return report | {"state": "requested_cursor_missing"}

    tail = {"after_exclusive": after, "before_exclusive": before, "last_inclusive": None,
            "next_unmeasured_cursor": None, "rows": 0, "missing_sql_payload_rows": 0,
            "first_missing_sql_payload_cursor": None, "window_exhausted": False}
    report["tail"] = tail
    cursor = after
    while tail["rows"] < max_tail_rows:
        check()
        remaining = max_tail_rows - tail["rows"]
        size = min(TAIL_BATCH_ROWS, remaining)
        final_batch = size == remaining
        rows = _tail_query(conn, day, cursor, before, size + int(final_batch))
        for row in rows[:size]:
            cursor = (row["market_commit_seq"], row["id"])
            if not row["has_payload"]:
                tail["missing_sql_payload_rows"] += 1
                if tail["first_missing_sql_payload_cursor"] is None:
                    tail["first_missing_sql_payload_cursor"] = cursor
        if rows:
            tail["last_inclusive"] = cursor
        tail["rows"] += min(len(rows), size)
        if len(rows) < size or (final_batch and len(rows) == size):
            tail["window_exhausted"] = True
            break
        if final_batch:
            edge = rows[-1]
            tail["next_unmeasured_cursor"] = (edge["market_commit_seq"], edge["id"])
    check()

    if tail["rows"] + int(tail["next_unmeasured_cursor"] is not None) > expected_tail:
        state = "sealed_row_count_mismatch"
    elif tail["missing_sql_payload_rows"]:
        state = "hot_payload_missing"
    elif not tail["window_exhausted"]:
        state = "tail_window_partial"
    elif after > lower or before is not None:
        state = "tail_slice_complete"
    elif tail["rows"] != expected_tail:
        state = "sealed_row_count_mismatch"
    elif not expected_tail:
        state = "published_complete"
    else:
        state = "mixed_hot_tail_present" if prefix["pages"] else "hot_only_present"
    return report | {"state": state}


def _arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument("--proof-sha256", required=True)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--max-pages", type=int, choices=range(1, 4097), metavar="1..4096", required=True)
    parser.add_argument("--max-tail-rows", type=int, choices=range(1, 1_000_001), metavar="1..1000000", required=True)
    parser.add_argument("--max-seconds", type=int, choices=range(1, 601), metavar="1..600", required=True)
    for side in ("after", "before"):
        parser.add_argument(f"--{side}-commit-seq", type=int)
        parser.add_argument(f"--{side}-fact-id")
    args = parser.parse_args(argv)
    cursors = {}
    for side in ("after", "before"):
        seq, fact_id = getattr(args, f"{side}_commit_seq"), getattr(args, f"{side}_fact_id")
        if seq is None and fact_id is None:
            cursors[side] = None
        elif seq is None or not 0 < seq < 2**63 or not re.fullmatch(r"mfv_[0-9a-f]{40}", fact_id or ""):
            parser.error(f"--{side}-commit-seq and --{side}-fact-id must be a positive bigint and a Fact version ID")
        else:
            cursors[side] = (seq, fact_id)
    return args, cursors


def main():
    from scripts.db.canonical_cold_private_seal import assert_promoted_clone, load_private_clone

    args, cursors = _arguments()
    clone = load_private_clone(args.proof, args.proof_sha256, os.environ.get("PG_DSN"))
    deadline = monotonic() + args.max_seconds
    engine = create_engine(clone.url, connect_args={"connect_timeout": min(5, args.max_seconds),
        "application_name": "qt_private_cohort_preflight",
        "options": ("-c default_transaction_read_only=on -c lock_timeout=2000 "
                    f"-c statement_timeout={args.max_seconds * 1000}")})
    try:
        with engine.connect() as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            conn.exec_driver_sql("SET LOCAL enable_seqscan=off")

            def check():
                remaining_ms = int((deadline - monotonic()) * 1000)
                if remaining_ms <= 0:
                    raise TimeoutError("private cohort preflight elapsed budget exceeded")
                conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(remaining_ms)})

            check()
            assert_promoted_clone(conn, clone)
            result = inspect_private_cohort(conn, day=args.day, max_pages=args.max_pages,
                                            max_tail_rows=args.max_tail_rows, check=check, **cursors)
            result.update(database_identity=clone.proof["database_identity"], database=clone.url.database,
                          clone_proof_sha256=args.proof_sha256, accepted_commit_seq=clone.proof["accepted_commit_seq"],
                          max_seconds=args.max_seconds)
    finally:
        engine.dispose()
    print(json.dumps(result, default=lambda value: value.isoformat(), sort_keys=True))


if __name__ == "__main__":
    main()
