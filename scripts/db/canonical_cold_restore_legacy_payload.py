"""Restore the original native payload leaf for one legacy day on an isolated clone."""

from __future__ import annotations

from datetime import date, timedelta
import hashlib
import tempfile
from time import monotonic

from sqlalchemy import text

from market_data.archive_namespace import archive_namespace
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_header_legacy_schema import assert_fact_header_legacy_contract
from portal.backend.db.fact_identity_schema import _partition_bound
from portal.backend.db.fact_storage_schema import (
    FACT_STORAGE_LAYOUT_VERSION, HOT_PAYLOAD_VALIDATION_BODY, fact_partition_name,
)
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from portal.backend.service.storage.repos.fact_archival import _partition_manifest_set_hash
from portal.backend.service.storage.repos.fact_storage import _catalog_manifest
from scripts.db.canonical_cold_migration import PostgresMigrationSource
from scripts.db.canonical_cold_restore_legacy import VERSION as LEGACY_HEADER_VERSION
from scripts.db.canonical_cold_restore_references import _ready, _require_disposable_fence
from scripts.db.canonical_cold_rollback_cohort import _require_snapshot
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_restore_legacy_payload.v1"
_HEADER = "market.fact_versions_legacy"
_PAYLOAD_PARENT = "market.fact_hot_payloads"
_COLUMNS = "storage_day,id,series_id,payload_schema_id,observation_time,payload,provenance,quality"
_GUARD = "trg_assert_fact_hot_payload_valid"


class _CopyBuffer:
    def __init__(self, *, max_bytes, deadline, output=None):
        self.max_bytes = max_bytes
        self.deadline = deadline
        self.output = output
        self.size = 0
        self.digest = hashlib.sha256()

    def _check(self):
        _check_deadline(self.deadline)

    def write(self, chunk):
        self._check()
        self.size += len(chunk)
        if self.size > self.max_bytes:
            raise RuntimeError("canonical_cold_recovery_payload_copy_budget_exceeded")
        self.digest.update(chunk)
        if self.output is not None:
            self.output.write(chunk)

    def read(self, size):
        self._check()
        return self.output.read(size)

    def evidence(self):
        return {"bytes": self.size, "sha256": self.digest.hexdigest()}


def _native_copy(conn, sql, buffer):
    with conn.connection.driver_connection.cursor() as cursor:
        remaining = int((buffer.deadline - monotonic()) * 1000)
        if remaining < 1:
            raise TimeoutError("canonical_cold_recovery_payload_timeout")
        cursor.execute("SELECT set_config('statement_timeout',%s,true), set_config('lock_timeout','500ms',true)",
                       (str(remaining),))
        cursor.copy_expert(sql, buffer)


def _check_deadline(deadline):
    if monotonic() >= deadline:
        raise TimeoutError("canonical_cold_recovery_payload_timeout")


def _digest(conn, query, *, max_bytes, deadline, output=None):
    sink = _CopyBuffer(max_bytes=max_bytes, deadline=deadline, output=output)
    _native_copy(conn, f"COPY ({query}) TO STDOUT WITH (FORMAT binary)", sink)
    return sink.evidence()


def _limits(max_rows, max_bytes):
    if (type(max_rows) is not int or not 1 <= max_rows <= 50_000_000
            or type(max_bytes) is not int or not 1 <= max_bytes <= 1024**4):
        raise ValueError("canonical_cold_recovery_payload_scope_invalid")


def _relation(conn, name):
    return conn.execute(text("""
        SELECT c.oid::bigint, parent.oid::regclass::text, pg_get_expr(c.relpartbound,c.oid)
        FROM pg_class c LEFT JOIN pg_inherits i ON i.inhrelid=c.oid
        LEFT JOIN pg_class parent ON parent.oid=i.inhparent
        WHERE c.oid=to_regclass(:name) AND c.relkind='r' AND c.relpersistence='p'
          AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
    """), {"name": name}).one_or_none()


def _header(conn, day, *, oid, end_day, max_rows, max_bytes, deadline):
    if day >= end_day or _relation(conn, _HEADER) != (
        oid, "market.fact_versions", f"FOR VALUES FROM (MINVALUE) TO ('{end_day.isoformat()}')",
    ):
        raise RuntimeError("canonical_cold_recovery_payload_legacy_header_changed")
    if assert_fact_header_legacy_contract(conn) != end_day:
        raise RuntimeError("canonical_cold_recovery_payload_legacy_registry_changed")
    count = conn.scalar(text(f"""
        SELECT count(*) FROM (SELECT 1 FROM ONLY {_HEADER} WHERE storage_day=:day LIMIT :limit) rows
    """), {"day": day, "limit": max_rows + 1})
    if count > max_rows:
        raise RuntimeError("canonical_cold_recovery_payload_row_budget_exceeded")
    query = f"SELECT h.* FROM ONLY {_HEADER} h WHERE storage_day=DATE '{day.isoformat()}' ORDER BY id"
    return count, _digest(conn, query, max_bytes=max_bytes, deadline=deadline)


def _retention(conn, day):
    marker = conn.scalar(text("""
        SELECT to_jsonb(p) FROM market.fact_retention_partitions p WHERE storage_day=:day
    """), {"day": day})
    if not marker or marker["state"] not in {"sealed", "verified", "reclaimed"}:
        raise RuntimeError("canonical_cold_recovery_payload_retention_not_sealed")
    return marker


def _verify_reclaimed(conn, donor, day, marker, *, store, limits, check_budget):
    if store is None or not isinstance(limits, ArchiveVerificationLimits):
        raise ValueError("canonical_cold_recovery_payload_reclaimed_archive_required")
    budget = ArchiveVerificationBatch(store, limits=limits, check_budget=check_budget)
    query = text("""
        SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day
        ORDER BY page_ordinal LIMIT :limit
    """)
    params = {"day": day, "limit": limits.max_pages + 1}
    target_rows = conn.execute(query, params).mappings().all()
    donor_rows = donor.execute(query, params).mappings().all()
    if (target_rows != donor_rows or len(target_rows) > limits.max_pages
            or sum(page["row_count"] for page in target_rows) != marker["expected_rows"]):
        raise RuntimeError("canonical_cold_recovery_payload_reclaimed_manifests_changed")
    with archive_namespace(store.root, exclusive=True) as check_archive:
        proofs = []
        for ordinal, page in enumerate(target_rows):
            check_budget()
            authority = PostgresMigrationSource.source_authority(conn, page)
            if (page["page_ordinal"] != ordinal or not authority["verification_hash"]
                    or PostgresMigrationSource.source_authority(donor, page) != authority):
                raise RuntimeError("canonical_cold_recovery_payload_reclaimed_authority_changed")
            manifest = _catalog_manifest(page)
            budget.verify(manifest.object_key, manifest.object_sha256, expected_bytes=manifest.byte_count)
            proofs.append(authority["verification_hash"])
        if _partition_manifest_set_hash(day, marker["expected_rows"], proofs) != marker["manifest_set_hash"]:
            raise RuntimeError("canonical_cold_recovery_payload_reclaimed_commitment_changed")
        budget.assert_unchanged()
        check_archive()


def _assert_native_guard(conn, relation, *, cloned):
    rows = conn.execute(text("""
        SELECT t.tgtype,t.tgenabled,t.tgqual IS NULL,t.tgnargs,t.tgdeferrable,t.tginitdeferred,
               t.tgfoid=to_regprocedure('market.assert_fact_hot_payload_valid()'),p.prosrc,
               parent.tgrelid=to_regclass(:parent) AND parent.tgname=:trigger
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        LEFT JOIN pg_trigger parent ON parent.oid=t.tgparentid
        WHERE t.tgrelid=to_regclass(:relation) AND t.tgname=:trigger AND NOT t.tgisinternal
    """), {"relation": relation, "parent": _PAYLOAD_PARENT, "trigger": _GUARD}).all()
    if rows != [(7, "A", True, 0, False, False, True, HOT_PAYLOAD_VALIDATION_BODY, True if cloned else None)]:
        raise RuntimeError("canonical_cold_recovery_payload_guard_changed")


def _assert_exact_payload_keys(conn, relation, day, count):
    if conn.scalar(text(f"""
        SELECT EXISTS (
            SELECT 1 FROM ONLY {relation} p LEFT JOIN ONLY {_HEADER} h
              ON h.storage_day=:day AND h.id=p.id
            WHERE h.id IS NULL OR
              (p.storage_day,p.series_id,p.payload_schema_id,p.observation_time)
              IS DISTINCT FROM (h.storage_day,h.series_id,h.payload_schema_id,h.observation_time)
        )
    """), {"day": day}):
        raise RuntimeError("canonical_cold_recovery_payload_header_values_changed")
    if conn.scalar(text(f"SELECT count(*) FROM ONLY {relation}")) != count:
        raise RuntimeError("canonical_cold_recovery_payload_row_count_changed")


def _effective_tablespace(conn, relation):
    return conn.scalar(text("""
        SELECT s.spcname FROM pg_class c JOIN pg_database d ON d.datname=current_database()
        JOIN pg_tablespace s ON s.oid=COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)
        WHERE c.oid=to_regclass(:relation)
    """), {"relation": relation})


def _donor_budget(conn, deadline):
    remaining = int((deadline - monotonic()) * 1000)
    if remaining < 1:
        raise TimeoutError("canonical_cold_recovery_payload_timeout")
    conn.execute(text("SELECT set_config('statement_timeout',:timeout,true), set_config('lock_timeout','500ms',true)"),
                 {"timeout": str(remaining)})
    conn.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")


def _receipt_matches(receipt, expected):
    return receipt.get("receipt_sha256") == expected == archive_evidence_hash(
        {key: value for key, value in receipt.items() if key != "receipt_sha256"})


def capture_original_payload_day(donor, *, donor_identity, day, expected_header_oid,
                                 expected_source_commit_seq, max_rows, max_bytes, timeout_seconds=120,
                                 paused_donor=None):
    """Pin the old day in a disposable read-only, repeatable-read donor snapshot."""
    _limits(max_rows, max_bytes)
    if (type(day) is not date or type(expected_header_oid) is not int or expected_header_oid <= 0
            or type(expected_source_commit_seq) is not int or expected_source_commit_seq < 0):
        raise ValueError("canonical_cold_recovery_payload_scope_invalid")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
        raise ValueError("canonical_cold_recovery_payload_timeout_invalid")
    deadline = monotonic() + timeout_seconds
    if not donor.in_transaction():
        raise ValueError("canonical_cold_recovery_payload_donor_transaction_required")
    with donor.begin_nested():
        _require_snapshot(donor, donor_identity, paused_donor=paused_donor)
        _donor_budget(donor, deadline)
        _ready(donor, FACT_STORAGE_LAYOUT_VERSION)
        if (PostgresMarketDataRepository._current_commit_seq_with_session(donor) != expected_source_commit_seq):
            raise RuntimeError("canonical_cold_recovery_payload_donor_watermark_changed")
        end_day = donor.scalar(text("SELECT market.fact_header_legacy_end_day()"))
        if end_day is None:
            raise RuntimeError("canonical_cold_recovery_payload_legacy_registry_missing")
        count, header = _header(donor, day, oid=expected_header_oid, end_day=end_day, max_rows=max_rows,
                                max_bytes=max_bytes, deadline=deadline)
        marker = _retention(donor, day)
        if marker["expected_rows"] != count:
            raise RuntimeError("canonical_cold_recovery_payload_retention_count_changed")
        relation = "market." + fact_partition_name(day)
        exists = donor.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": relation})
        native = None
        if marker["state"] == "reclaimed":
            if exists is not None:
                raise RuntimeError("canonical_cold_recovery_payload_reclaimed_leaf_present")
        else:
            if exists is None or _relation(donor, relation) != (exists, _PAYLOAD_PARENT, _partition_bound(day)):
                raise RuntimeError("canonical_cold_recovery_payload_donor_leaf_changed")
            _assert_native_guard(donor, relation, cloned=True)
            _assert_exact_payload_keys(donor, relation, day, count)
            native = {"oid": exists, "tablespace": _effective_tablespace(donor, relation), **_digest(
                donor, f"SELECT {_COLUMNS} FROM ONLY {relation} ORDER BY id", max_bytes=max_bytes, deadline=deadline,
            )}
    evidence = {"version": VERSION, "donor": donor_identity, "storage_day": day.isoformat(),
                "legacy_oid": expected_header_oid, "legacy_end_day": end_day.isoformat(),
                "source_commit_seq": expected_source_commit_seq, "row_count": count, "retention": marker,
                "header": header, "payload": native}
    return evidence | {"receipt_sha256": archive_evidence_hash(evidence)}


def restore_legacy_payload_day(target, donor, *, fence, original, expected_original_hash,
                                legacy_header_receipt, expected_legacy_header_receipt_hash,
                                max_rows, max_bytes, store=None, object_limits=None, timeout_seconds=120,
                                paused_donor=None):
    """COPY the old bytes while detached, attach with the original guards, then recheck."""
    _require_disposable_fence(target, fence)
    _limits(max_rows, max_bytes)
    if (not donor.in_transaction() or type(timeout_seconds) is not int
            or not 1 <= timeout_seconds <= 3600):
        raise ValueError("canonical_cold_recovery_payload_restore_scope_invalid")
    expected_target = legacy_header_receipt["target_database"]
    if (target is donor or original["version"] != VERSION
            or legacy_header_receipt["schema"] != LEGACY_HEADER_VERSION
            or not _receipt_matches(original, expected_original_hash)
            or not _receipt_matches(legacy_header_receipt, expected_legacy_header_receipt_hash)
            or any(expected_target[key] != fence.database[key]
                   for key in ("system_identifier", "database", "database_oid"))
            or legacy_header_receipt["donor_identity"] != original["donor"]
            or expected_target == original["donor"]
            or legacy_header_receipt["original_oid"] != original["legacy_oid"]
            or legacy_header_receipt["end_day"] != original["legacy_end_day"]):
        raise ValueError("canonical_cold_recovery_payload_receipt_changed")
    deadline = min(monotonic() + timeout_seconds, fence.deadline)
    def check_budget():
        _check_deadline(deadline)
    day, end_day = date.fromisoformat(original["storage_day"]), date.fromisoformat(original["legacy_end_day"])
    count, marker, native = (original[key] for key in ("row_count", "retention", "payload"))
    if native is not None and (marker["state"] not in {"sealed", "verified"} or native["bytes"] > max_bytes):
        raise ValueError("canonical_cold_recovery_payload_native_scope_invalid")
    name = fact_partition_name(day)
    relation = f'market."{name}"'
    params = {"day": day, "relation": f"market.{name}"}
    with _bounded_step(target, timeout_seconds) as bound, donor.begin_nested():
        bound(max(0, deadline - monotonic()))
        check_budget()
        fence.assert_quiet(target)
        _require_snapshot(donor, original["donor"], paused_donor=paused_donor)
        _donor_budget(donor, deadline)
        if PostgresMigrationSource(target.engine, expected_system_identifier=expected_target["system_identifier"]).identity(target) != expected_target:
            raise RuntimeError("canonical_cold_recovery_payload_target_database_changed")
        _ready(donor, FACT_STORAGE_LAYOUT_VERSION)
        if (PostgresMarketDataRepository._current_commit_seq_with_session(donor) != original["source_commit_seq"]
                or _retention(donor, day) != marker):
            raise RuntimeError("canonical_cold_recovery_payload_donor_changed")
        donor_rows, donor_header = _header(
            donor, day, oid=original["legacy_oid"], end_day=end_day, max_rows=max_rows,
            max_bytes=max_bytes, deadline=deadline,
        )
        if (donor_rows, donor_header) != (count, original["header"]):
            raise RuntimeError("canonical_cold_recovery_payload_donor_headers_changed")
        target.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
        if not target.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                             {"name": _LIFECYCLE_LOCK_NAME}):
            raise RuntimeError("canonical_cold_recovery_payload_lifecycle_busy")
        target.exec_driver_sql("LOCK TABLE market.fact_storage_state IN SHARE MODE NOWAIT")
        target.exec_driver_sql("LOCK TABLE ONLY market.fact_hot_payloads IN SHARE UPDATE EXCLUSIVE MODE NOWAIT")
        target.exec_driver_sql(f"LOCK TABLE ONLY {_HEADER} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        _ready(target, COLD_HISTORY_LAYOUT_VERSION)
        retirement = target.execute(text("""
            SELECT handoff_kind,manifest_set_hash,row_count FROM market.cold_fact_retired_days WHERE storage_day=:day
        """), params).one_or_none()
        if (retirement != ("initial", legacy_header_receipt["audit_set_hash"], count)
                or _retention(target, day) != marker):
            raise RuntimeError("canonical_cold_recovery_payload_target_cohort_changed")
        rows, header = _header(target, day, oid=legacy_header_receipt["legacy_oid"], end_day=end_day,
                               max_rows=max_rows, max_bytes=max_bytes, deadline=deadline)
        if (rows, header) != (count, original["header"]):
            raise RuntimeError("canonical_cold_recovery_payload_target_headers_changed")
        donor_oid = donor.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), params)
        target_oid = target.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), params)
        if native is None:
            if marker["state"] != "reclaimed" or donor_oid is not None or target_oid is not None:
                raise RuntimeError("canonical_cold_recovery_payload_reclaimed_leaf_present")
            _verify_reclaimed(target, donor, day, marker, store=store, limits=object_limits,
                              check_budget=check_budget)
        else:
            if _relation(donor, params["relation"]) != (native["oid"], _PAYLOAD_PARENT, _partition_bound(day)):
                raise RuntimeError("canonical_cold_recovery_payload_donor_leaf_changed")
            if _effective_tablespace(donor, relation) != native["tablespace"]:
                raise RuntimeError("canonical_cold_recovery_payload_donor_placement_changed")
            _assert_native_guard(donor, relation, cloned=True)
            _assert_native_guard(target, _PAYLOAD_PARENT, cloned=False)
            _assert_exact_payload_keys(donor, relation, day, count)
            query = f"SELECT {_COLUMNS} FROM ONLY {relation} ORDER BY id"
            expected_bytes = {key: native[key] for key in ("bytes", "sha256")}
            # Bounded binary staging is private to this worker. The target
            # starts enforcing its native sealed-day trigger at ATTACH.
            if target_oid is None:
                until = day + timedelta(days=1)
                quote = target.dialect.identifier_preparer.quote_identifier
                tablespace = quote(native["tablespace"])
                target.exec_driver_sql(f"CREATE TABLE {relation} (LIKE {_PAYLOAD_PARENT} INCLUDING ALL) TABLESPACE {tablespace}")
                target.exec_driver_sql(
                    f'ALTER TABLE {relation} ADD CONSTRAINT "{name}_day" '
                    f"CHECK (storage_day >= DATE '{day}' AND storage_day < DATE '{until}')"
                )
                with tempfile.TemporaryFile() as spool:
                    if _digest(donor, query, max_bytes=max_bytes, deadline=deadline, output=spool) != expected_bytes:
                        raise RuntimeError("canonical_cold_recovery_payload_donor_bytes_changed")
                    spool.seek(0)
                    _native_copy(target, f"COPY {relation} ({_COLUMNS}) FROM STDIN WITH (FORMAT binary)",
                                 _CopyBuffer(max_bytes=max_bytes, deadline=deadline, output=spool))
                _assert_exact_payload_keys(target, relation, day, count)
                target.exec_driver_sql(f"ALTER TABLE {_PAYLOAD_PARENT} ATTACH PARTITION {relation} {_partition_bound(day)}")
                target_oid = target.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), params)
            else:
                if _digest(donor, query, max_bytes=max_bytes, deadline=deadline) != expected_bytes:
                    raise RuntimeError("canonical_cold_recovery_payload_donor_bytes_changed")
            if _relation(target, params["relation"]) != (target_oid, _PAYLOAD_PARENT, _partition_bound(day)):
                raise RuntimeError("canonical_cold_recovery_payload_target_leaf_changed")
            if _effective_tablespace(target, relation) != native["tablespace"]:
                raise RuntimeError("canonical_cold_recovery_payload_target_placement_changed")
            _assert_native_guard(target, relation, cloned=True)
            _assert_exact_payload_keys(target, relation, day, count)
            if _digest(target, query, max_bytes=max_bytes, deadline=deadline) != expected_bytes:
                raise RuntimeError("canonical_cold_recovery_payload_target_bytes_changed")
        if _retention(target, day) != marker:
            raise RuntimeError("canonical_cold_recovery_payload_target_retention_changed")
        check_budget()
        fence.assert_quiet(target)
    receipt = {"version": VERSION, "target_database": expected_target, "storage_day": day.isoformat(),
               "row_count": count, "header_sha256": header["sha256"],
               "retention_state": marker["state"], "retention_sha256": archive_evidence_hash(marker),
               "payload_oid": target_oid, "payload_sha256": native["sha256"] if native else None,
               "payload_tablespace": native["tablespace"] if native else None,
               "reused_archive_manifest_set_hash": marker["manifest_set_hash"] if native is None else None,
               "original_receipt_sha256": expected_original_hash,
               "legacy_header_receipt_sha256": expected_legacy_header_receipt_hash}
    return receipt | {"receipt_sha256": archive_evidence_hash(receipt)}
