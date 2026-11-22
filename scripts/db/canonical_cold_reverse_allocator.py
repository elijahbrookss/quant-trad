"""Preserve the exact next commit allocation on a disposable v2 restore."""
from __future__ import annotations

from dataclasses import asdict, dataclass

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.db.canonical_cold_database_fence import DatabaseFence
from scripts.db.canonical_cold_migration import PostgresMigrationSource


VERSION = "qt.canonical_cold_reverse_allocator_fixture.v1"
SEQUENCE = "market.fact_commit_seq"
MAX_COMMIT = 2**63 - 1
_SEQ_SHAPE = ("bigint", 1, 1, MAX_COMMIT, 1, 1, False, True)


@dataclass(frozen=True)
class AllocatorBoundary:
    database: dict
    layout: str
    visible_commit: int
    sequence_oid: int
    last_value: int
    is_called: bool

    @property
    def next_value(self):
        return self.last_value + int(self.is_called)

    @property
    def sha256(self):
        return archive_evidence_hash({"version": VERSION, "boundary": asdict(self)})


def _guard(conn, fence, timeout_ms):
    if (not isinstance(fence, DatabaseFence) or not conn.in_transaction()
            or conn.get_isolation_level() != "READ COMMITTED"
            or conn.get_execution_options().get("isolation_level") == "AUTOCOMMIT"
            or type(timeout_ms) is not int or not 1 <= timeout_ms <= 120_000):
        raise ValueError("reverse allocator requires a bounded read-committed live database fence")
    url = conn.engine.url
    if (not str(url.database).startswith(("qt_bench_", "qt_migration_"))
            or url.host not in {None, "timescaledb", "127.0.0.1", "::1"}
            or (url.query.get("host") is not None and not str(url.query["host"]).startswith("/"))):
        raise ValueError("reverse allocator requires a disposable local clone")
    conn.exec_driver_sql("SET LOCAL lock_timeout='500ms'")
    conn.execute(text("SELECT set_config('statement_timeout',:value,true)"), {"value": str(timeout_ms)})
    fence.assert_quiet(conn)


def _boundary(conn, fence, *, before_restart=False):
    identity = PostgresMigrationSource(conn.engine, expected_system_identifier=fence.database["system_identifier"]).identity(conn)
    if any(identity[name] != fence.database[name] for name in ("system_identifier", "database", "database_oid")):
        raise RuntimeError("reverse allocator fenced database identity differs")
    layout = conn.execute(text("""
        SELECT layout_version FROM market.fact_storage_state WHERE state='ready' AND completed_at IS NOT NULL
    """)).scalar_one()
    oid, *shape = conn.execute(text("""
        SELECT settings.seqrelid::bigint,settings.seqtypid::regtype::text,settings.seqstart,settings.seqmin,
           settings.seqmax,settings.seqincrement,settings.seqcache,settings.seqcycle,relation.relpersistence='p'
        FROM pg_sequence AS settings JOIN pg_class AS relation ON relation.oid=settings.seqrelid
        WHERE settings.seqrelid='market.fact_commit_seq'::regclass
    """)).one()
    if tuple(shape) != _SEQ_SHAPE:
        raise RuntimeError("reverse allocator requires the native ascending, non-cycling, cache-one sequence")
    last_value, is_called = conn.exec_driver_sql(f"SELECT last_value,is_called FROM {SEQUENCE}").one()
    visible = PostgresMarketDataRepository._current_commit_seq_with_session(conn)
    result = AllocatorBoundary(identity, layout, visible, oid, last_value, is_called)
    if (not 1 <= result.next_value <= MAX_COMMIT or visible < 0
            or (not before_restart and visible >= result.next_value)):
        raise RuntimeError("reverse allocator checkpoint is exhausted or precedes visible accepted commits")
    return result


def capture_allocator(conn, *, fence, layout, timeout_ms=30_000):
    """Read allocation separately from visibility on the fenced snapshot owner."""
    if layout not in {FACT_STORAGE_LAYOUT_VERSION, COLD_HISTORY_LAYOUT_VERSION}:
        raise ValueError("reverse allocator capture layout unsupported")
    _guard(conn, fence, timeout_ms)
    observed = _boundary(conn, fence)
    if observed.layout != layout:
        raise RuntimeError("reverse allocator layout differs from the expected snapshot")
    fence.assert_quiet(conn)
    return observed


def synchronize_allocator(conn, *, fence, candidate, baseline, expected_candidate_hash, expected_baseline_hash,
                          timeout_ms=30_000):
    """Restart transactionally after the caller's complete exact row replay.

    The destination's visible commit must match the captured candidate. This
    is a watermark check; the caller still supplies full inventory parity.
    A retry after abort sees the baseline allocator. An identical already
    synchronized boundary returns without changing the sequence. The normalized
    restart tuple also distinguishes a naturally consumed `next_value - 1`.
    """
    if (baseline.sha256 != expected_baseline_hash or candidate.sha256 != expected_candidate_hash
            or baseline.layout != FACT_STORAGE_LAYOUT_VERSION or candidate.layout != COLD_HISTORY_LAYOUT_VERSION
            or (baseline.database["system_identifier"], baseline.database["database_oid"])
                == (candidate.database["system_identifier"], candidate.database["database_oid"])
            or not 0 <= baseline.visible_commit <= candidate.visible_commit < candidate.next_value <= MAX_COMMIT
            or not baseline.next_value <= candidate.next_value):
        raise RuntimeError("reverse allocator plan has mismatched identities or nonmonotonic boundary")
    _guard(conn, fence, timeout_ms)
    # The destination still has the baseline sequence until this final step.
    # Its projected row watermark can therefore exceed its next value here.
    observed = _boundary(conn, fence, before_restart=True)
    if (observed.database != baseline.database or observed.layout != baseline.layout
            or observed.sequence_oid != baseline.sequence_oid):
        raise RuntimeError("reverse allocator destination identity or shape changed")
    if observed.visible_commit != candidate.visible_commit:
        raise RuntimeError("reverse allocator destination accepted watermark differs from candidate")
    observed_tuple = observed.last_value, observed.is_called
    baseline_tuple = baseline.last_value, baseline.is_called
    if observed_tuple == (candidate.next_value, False) or (
        baseline.next_value == candidate.next_value and observed_tuple == baseline_tuple
    ):
        changed = False
    elif observed_tuple == baseline_tuple:
        conn.exec_driver_sql(f"ALTER SEQUENCE {SEQUENCE} RESTART WITH {candidate.next_value:d}")
        changed = True
    else:
        raise RuntimeError("reverse allocator destination has advanced independently")
    final = _boundary(conn, fence)
    if final.next_value != candidate.next_value or final.visible_commit != candidate.visible_commit:
        raise RuntimeError("reverse allocator transactional result differs from candidate")
    fence.assert_quiet(conn)
    receipt = {"version": VERSION, "candidate_sha256": candidate.sha256, "baseline_sha256": baseline.sha256,
               "visible_commit": final.visible_commit, "next_value": final.next_value,
               "target_boundary_sha256": final.sha256, "changed": changed}
    return receipt | {"receipt_sha256": archive_evidence_hash(receipt)}
