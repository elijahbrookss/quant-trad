"""Bound exact canonical reads during the final precommit acceptance check."""

from __future__ import annotations

from contextlib import closing
from datetime import datetime
from hashlib import sha256
import json
import re

from sqlalchemy import text
from sqlalchemy.orm import Session

from core.execution_control import ExecutionControl, controlled_execution, execution_checkpoint
from market_data.canonical_storage import record_from_storage_row
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from portal.backend.service.storage.repos import market_data


VERSION = "qt.canonical_cold_acceptance_witnesses.v1"
MAX_WITNESS_BYTES = 524_288
_DIGEST = re.compile(r"[0-9a-f]{64}")


def _hash(value):
    return isinstance(value, str) and _DIGEST.fullmatch(value) is not None


def _time(value):
    if not isinstance(value, str):
        raise ValueError("canonical_cold_witness_requires_aware_time")
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError("canonical_cold_witness_requires_aware_time")
    return result


def _fact(record):
    return {
        "id": record.fact_version_id, "series": record.series_id, "source": record.source_id,
        "revision": record.revision, "commit": record.market_commit_seq, "run": record.ingestion_run_id,
        "hash": record.row_hash, "fact": record.fact.to_dict(),
    }


def _frozen(session, case):
    entry = session.execute(text("""
        SELECT ds.range_start,ds.range_end,ds.max_commit_seq,ds.source_summary,d.dataset_hash,s.contract_version
        FROM market.dataset_series ds JOIN market.datasets d ON d.id=ds.dataset_id
        JOIN market.series s ON s.id=ds.series_id
        WHERE ds.dataset_id=:dataset_id AND ds.series_id=:series_id
          AND ds.range_start=:start AND ds.range_end=:end
    """), {"dataset_id": case["dataset_id"], "series_id": case["series_id"],
            "start": _time(case["start"]), "end": _time(case["end"])}).mappings().one_or_none()
    if entry is None or str(entry["dataset_hash"]) != case["dataset_hash"]:
        raise RuntimeError("canonical_cold_witness_frozen_dataset_binding_changed")
    if (dict(entry["source_summary"] or {}).get("record_selection") != market_data._ALL_CANONICAL_REVISIONS_SELECTION
            or not market_data._preserves_canonical_revision_history(str(entry["contract_version"]))):
        raise RuntimeError("canonical_cold_witness_frozen_revision_scope_unpinned")
    return dict(start=entry["range_start"], end=entry["range_end"], as_of_commit_seq=entry["max_commit_seq"])


def _selection(session, case, storage_repository):
    mode = case["mode"]
    if mode == "exact":
        found = storage_repository.read_rows_by_ids(session, case["ids"])
        if set(found) != set(case["ids"]):
            raise RuntimeError("canonical_cold_witness_exact_identity_missing")
        return (found[identity] for identity in case["ids"])
    if mode == "frozen_revisions":
        scope = _frozen(session, case)
    else:
        scope = {
            "start": _time(case["start"]), "end": _time(case["end"]),
            "as_of_commit_seq": case["as_of_commit_seq"],
        }
    return market_data.PostgresMarketDataRepository._iter_canonical_rows_with_session(
        session, series_id=case["series_id"], **scope,
        known_at_lte=_time(case["known_at_lte"]) if case["known_at_lte"] is not None else None,
        latest_only=mode == "as_of", include_invalidated=mode != "as_of",
        storage_repository=storage_repository,
    )


def _validate(cases, *, require_expected=False):
    if not isinstance(cases, list) or not 1 <= len(cases) <= 64:
        raise ValueError("canonical_cold_witness_invalid_case_count")
    seen = set()
    modes = set()
    proven_modes = set()
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("canonical_cold_witness_invalid_case")
        name, mode = case.get("name"), case.get("mode")
        if not isinstance(name, str) or not name or len(name) > 96 or name in seen:
            raise ValueError("canonical_cold_witness_invalid_case_identifier")
        seen.add(name)
        fields = {"name", "mode", "max_rows"}
        if type(case.get("max_rows")) is not int or not 1 <= case["max_rows"] <= 10_000:
            raise ValueError("canonical_cold_witness_invalid_max_rows")
        if require_expected:
            fields |= {"expected_rows", "expected_hash"}
            if (type(case.get("expected_rows")) is not int
                    or not 0 <= case["expected_rows"] <= case["max_rows"]
                    or not _hash(case.get("expected_hash"))):
                raise ValueError("canonical_cold_witness_invalid_expected_evidence")
            if case["expected_rows"] > 0 and isinstance(mode, str):
                proven_modes.add(mode)
        if mode == "exact":
            fields.add("ids")
            ids = case.get("ids")
            if (not isinstance(ids, list) or not 1 <= len(ids) <= min(case["max_rows"], 256)
                    or any(not isinstance(value, str) or not 1 <= len(value) <= 64 for value in ids)
                    or len(set(ids)) != len(ids)):
                raise ValueError("canonical_cold_witness_invalid_exact_ids")
        elif mode in ("frozen_revisions", "as_of"):
            fields |= {"series_id", "known_at_lte", "start", "end"}
            if type(case.get("series_id")) is not int or case["series_id"] <= 0:
                raise ValueError("canonical_cold_witness_invalid_series_id")
            if _time(case.get("end")) <= _time(case.get("start")):
                raise ValueError("canonical_cold_witness_invalid_range")
            if case.get("known_at_lte") is not None:
                _time(case["known_at_lte"])
            if mode == "frozen_revisions":
                fields |= {"dataset_id", "dataset_hash"}
                if not isinstance(case.get("dataset_id"), str) or not case["dataset_id"] or not _hash(case.get("dataset_hash")):
                    raise ValueError("canonical_cold_witness_invalid_dataset_identity")
            else:
                fields.add("as_of_commit_seq")
                if (type(case.get("as_of_commit_seq")) is not int
                        or case["as_of_commit_seq"] < 1):
                    raise ValueError("canonical_cold_witness_invalid_as_of_scope")
        else:
            raise ValueError("canonical_cold_witness_invalid_mode")
        if case.keys() != fields:
            raise ValueError("canonical_cold_witness_unexpected_scope")
        modes.add(mode)
    if modes != {"exact", "frozen_revisions", "as_of"}:
        raise ValueError("canonical_cold_witness_required_modes_missing")
    if require_expected and proven_modes != modes:
        raise ValueError("canonical_cold_witness_nonempty_required_modes_missing")


def measure_witnesses(conn, cases, *, store, timeout_seconds, max_archive_bytes):
    """Return ordered exact evidence from the supplied transaction and tier."""
    _validate(cases)
    if (type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 600
            or type(max_archive_bytes) is not int or not 1 <= max_archive_bytes <= 8 * 1024**3):
        raise ValueError("canonical_cold_witness_invalid_execution_bounds")
    if not conn.in_transaction():
        raise RuntimeError("canonical_cold_witness_requires_bound_transaction")
    repository = PostgresCanonicalFactStorageRepository(
        object_store_factory=lambda: store, read_cache_factory=lambda: None,
    )
    limits = ExecutionControl()
    limits.limit(seconds=timeout_seconds, input_rows=sum(case["max_rows"] for case in cases) * 4,
                 archive_read_bytes=max_archive_bytes)
    results = []
    with controlled_execution(limits), Session(bind=conn, join_transaction_mode="rollback_only", autoflush=False) as session:
        for case in cases:
            content_hash = archive_evidence_hash({"name": case["name"], "mode": case["mode"]})
            count = 0
            with closing(_selection(session, case, repository)) as rows:
                for raw in rows:
                    execution_checkpoint()
                    count += 1
                    if count > case["max_rows"]:
                        raise RuntimeError(f"canonical_cold_witness_budget_exhausted: {case['name']}")
                    content_hash = archive_evidence_hash({"before": content_hash, "row": _fact(record_from_storage_row(raw))})
            results.append({**case, "expected_rows": count, "expected_hash": content_hash})
    _validate(results, require_expected=True)
    return {"schema": VERSION, "max_archive_bytes": max_archive_bytes, "cases": results}


def load_approved_witnesses(content, *, expected_sha256):
    if (not isinstance(content, bytes) or not 1 <= len(content) <= MAX_WITNESS_BYTES
            or sha256(content).hexdigest() != expected_sha256):
        raise ValueError("canonical_cold_witness_approved_digest_changed")
    witness = json.loads(content)
    if (not isinstance(witness, dict) or witness.keys() != {"schema", "max_archive_bytes", "cases"}
            or witness["schema"] != VERSION or type(witness["max_archive_bytes"]) is not int
            or not 1 <= witness["max_archive_bytes"] <= 8 * 1024**3):
        raise ValueError("canonical_cold_witness_invalid")
    _validate(witness["cases"], require_expected=True)
    return witness


def check_witnesses(conn, witness, *, store, timeout_seconds):
    expected = witness["cases"]
    observed = measure_witnesses(
        conn, [{key: value for key, value in case.items() if key not in ("expected_rows", "expected_hash")}
                  for case in expected],
        store=store, timeout_seconds=timeout_seconds, max_archive_bytes=witness["max_archive_bytes"],
    )
    for actual, wanted in zip(observed["cases"], expected, strict=True):
        if (actual["expected_hash"], actual["expected_rows"]) != (wanted["expected_hash"], wanted["expected_rows"]):
            raise RuntimeError(f"canonical_cold_witness_content_changed: {wanted['name']}")
    return archive_evidence_hash(observed)
