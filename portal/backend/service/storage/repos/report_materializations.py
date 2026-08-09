"""Report materialization artifact/status persistence."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from ._shared import (
    BotRunEventRecord,
    BotRunRecord,
    BotTradeRecord,
    ReportMaterializationRecord,
    SQLAlchemyError,
    func,
    _json_safe,
    _parse_optional_timestamp,
    _utcnow,
    db,
    logger,
    select,
    text,
)
from ...provenance import (
    REPORT_CONTRACT_VERSION,
    REPORT_DATASET_SCHEMA_VERSION,
    REPORT_INPUT_FINGERPRINT_SCHEMA_VERSION,
    REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION,
    REPORT_SCHEMA_VERSION,
    source_revision,
)


REPORT_STATUS_NOT_STARTED = "not_started"
REPORT_STATUS_BUILDING = "building"
REPORT_STATUS_READY = "ready"
REPORT_STATUS_FAILED = "failed"
REPORT_STATUS_STALE = "stale"
_REPORT_MATERIALIZATION_LOCK_PERSON = b"qt_report"


def _empty_status(run_id: str, *, contract_version: str = REPORT_CONTRACT_VERSION) -> Dict[str, Any]:
    return {
        "run_id": str(run_id or ""),
        "status": REPORT_STATUS_NOT_STARTED,
        "contract_version": contract_version,
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "dataset_schema_version": REPORT_DATASET_SCHEMA_VERSION,
        "builder_source_revision": None,
        "storage_schema_version": REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION,
        "artifact_id": None,
        "artifact_path": None,
        "built_at": None,
        "started_at": None,
        "duration_ms": None,
        "error": None,
        "stale_reason": None,
        "cache_key": None,
        "input_fingerprint": None,
        "input_fingerprint_payload": {},
        "source_event_count": 0,
        "source_event_high_water_run_seq": 0,
        "source_trade_count": 0,
        "source_run_updated_at": None,
        "can_view": False,
        "can_build": True,
        "can_retry": False,
    }


def _dt_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat() + "Z"
    return str(value)


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(_json_safe(dict(payload)), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _source_run_updated_at(payload: Optional[Mapping[str, Any]]) -> Optional[datetime]:
    if not payload:
        return None
    return _parse_optional_timestamp(payload.get("run_updated_at"))


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key, default)
    return getattr(row, key, default)


def _input_fingerprint_status_payload(
    *,
    run: Any,
    event_count: int,
    event_high_water_run_seq: int,
    event_high_water_id: int,
    event_updated_at: Any,
    trade_count: int,
    trade_updated_at: Any,
) -> Dict[str, Any]:
    raw_config_snapshot = _row_value(run, "config_snapshot")
    config_snapshot = (
        dict(raw_config_snapshot)
        if isinstance(raw_config_snapshot, Mapping)
        else {}
    )
    dataset_binding = (
        dict(config_snapshot.get("dataset_binding"))
        if isinstance(config_snapshot.get("dataset_binding"), Mapping)
        else {}
    )
    payload = {
        "schema_version": REPORT_INPUT_FINGERPRINT_SCHEMA_VERSION,
        "run_id": str(_row_value(run, "run_id") or ""),
        "bot_id": str(_row_value(run, "bot_id") or ""),
        "status": str(_row_value(run, "status") or ""),
        "run_type": str(_row_value(run, "run_type") or ""),
        "started_at": _dt_iso(_row_value(run, "started_at")),
        "ended_at": _dt_iso(_row_value(run, "ended_at")),
        "run_updated_at": _dt_iso(_row_value(run, "updated_at")),
        "config_hash": str(_row_value(run, "config_hash") or ""),
        "material_config_hash": str(_row_value(run, "material_config_hash") or ""),
        "dataset_id": str(_row_value(run, "dataset_id") or dataset_binding.get("dataset_id") or ""),
        "dataset_hash": str(_row_value(run, "dataset_hash") or dataset_binding.get("dataset_hash") or ""),
        "strategy_hash": str(_row_value(run, "strategy_hash") or ""),
        "data_snapshot_hash": str(_row_value(run, "data_snapshot_hash") or ""),
        "runtime_contract_version": str(_row_value(run, "runtime_contract_version") or ""),
        "runtime_source_revision": str(_row_value(run, "runtime_source_revision") or ""),
        "runtime_image": str(_row_value(run, "runtime_image") or ""),
        "storage_schema_version": str(_row_value(run, "storage_schema_version") or ""),
        "summary_hash": _hash_payload(_row_value(run, "summary") or {}),
        "event_count": int(event_count or 0),
        "event_high_water_run_seq": int(event_high_water_run_seq or 0),
        "event_high_water_id": int(event_high_water_id or 0),
        "event_updated_at": _dt_iso(event_updated_at),
        "trade_count": int(trade_count or 0),
        "trade_updated_at": _dt_iso(trade_updated_at),
    }
    return payload


def _fingerprints_for_run_ids(session: Any, run_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    wanted = [str(run_id or "").strip() for run_id in dict.fromkeys(run_ids) if str(run_id or "").strip()]
    if not wanted:
        return {}
    config = BotRunRecord.config_snapshot
    run_rows = session.execute(
        select(
            BotRunRecord.run_id,
            BotRunRecord.bot_id,
            BotRunRecord.status,
            BotRunRecord.run_type,
            BotRunRecord.started_at,
            BotRunRecord.ended_at,
            BotRunRecord.updated_at,
            BotRunRecord.config_hash,
            BotRunRecord.material_config_hash,
            BotRunRecord.strategy_hash,
            BotRunRecord.data_snapshot_hash,
            BotRunRecord.runtime_contract_version,
            BotRunRecord.runtime_source_revision,
            BotRunRecord.runtime_image,
            BotRunRecord.storage_schema_version,
            BotRunRecord.summary,
            config["dataset_binding"]["dataset_id"].as_string().label("dataset_id"),
            config["dataset_binding"]["dataset_hash"].as_string().label("dataset_hash"),
        ).where(BotRunRecord.run_id.in_(wanted))
    ).mappings().all()
    runs_by_id = {str(row["run_id"]): row for row in run_rows}
    event_rows = session.execute(
        select(
            BotRunEventRecord.run_id,
            func.count(BotRunEventRecord.id),
            func.max(BotRunEventRecord.run_seq),
            func.max(BotRunEventRecord.id),
            func.max(BotRunEventRecord.created_at),
        )
        .where(BotRunEventRecord.run_id.in_(wanted))
        .group_by(BotRunEventRecord.run_id)
    ).all()
    events_by_run = {
        str(run_id): {
            "count": int(count or 0),
            "high_water_run_seq": int(high_water_run_seq or 0),
            "high_water_id": int(high_water_id or 0),
            "updated_at": updated_at,
        }
        for run_id, count, high_water_run_seq, high_water_id, updated_at in event_rows
    }
    trade_rows = session.execute(
        select(
            BotTradeRecord.run_id,
            func.count(BotTradeRecord.id),
            func.max(BotTradeRecord.updated_at),
        )
        .where(BotTradeRecord.run_id.in_(wanted))
        .group_by(BotTradeRecord.run_id)
    ).all()
    trades_by_run = {
        str(run_id): {"count": int(count or 0), "updated_at": updated_at}
        for run_id, count, updated_at in trade_rows
    }
    result: Dict[str, Dict[str, Any]] = {}
    for run_id in wanted:
        run = runs_by_id.get(run_id)
        if run is None:
            continue
        event_stats = events_by_run.get(run_id, {})
        trade_stats = trades_by_run.get(run_id, {})
        payload = _input_fingerprint_status_payload(
            run=run,
            event_count=int(event_stats.get("count") or 0),
            event_high_water_run_seq=int(event_stats.get("high_water_run_seq") or 0),
            event_high_water_id=int(event_stats.get("high_water_id") or 0),
            event_updated_at=event_stats.get("updated_at"),
            trade_count=int(trade_stats.get("count") or 0),
            trade_updated_at=trade_stats.get("updated_at"),
        )
        result[run_id] = {
            "input_fingerprint": _hash_payload(payload),
            "input_fingerprint_payload": payload,
            "source_event_count": int(payload["event_count"]),
            "source_event_high_water_run_seq": int(payload["event_high_water_run_seq"]),
            "source_trade_count": int(payload["trade_count"]),
            "source_run_updated_at": _row_value(run, "updated_at"),
        }
    return result


def _status_projection_query(run_ids: Sequence[str]):
    wanted = [str(run_id or "").strip() for run_id in dict.fromkeys(run_ids) if str(run_id or "").strip()]
    artifact = ReportMaterializationRecord.artifact
    return (
        select(
            ReportMaterializationRecord.run_id,
            ReportMaterializationRecord.status,
            ReportMaterializationRecord.contract_version,
            ReportMaterializationRecord.report_schema_version,
            ReportMaterializationRecord.dataset_schema_version,
            ReportMaterializationRecord.builder_source_revision,
            ReportMaterializationRecord.storage_schema_version,
            ReportMaterializationRecord.artifact_id,
            ReportMaterializationRecord.cache_key,
            ReportMaterializationRecord.input_fingerprint,
            ReportMaterializationRecord.input_fingerprint_payload,
            ReportMaterializationRecord.source_event_count,
            ReportMaterializationRecord.source_event_high_water_run_seq,
            ReportMaterializationRecord.source_trade_count,
            ReportMaterializationRecord.source_run_updated_at,
            ReportMaterializationRecord.stale_reason,
            ReportMaterializationRecord.error,
            ReportMaterializationRecord.started_at,
            ReportMaterializationRecord.built_at,
            ReportMaterializationRecord.duration_ms,
            (func.jsonb_typeof(artifact) == "object").label("artifact_valid"),
            artifact["readiness"]["dataset_ready"].as_boolean().label("artifact_dataset_ready"),
            artifact["readiness"]["results_ready"].as_boolean().label("artifact_results_ready"),
            artifact["readiness"]["safe_to_compare"].as_boolean().label("artifact_safe_to_compare"),
            artifact["readiness"]["reason"].as_string().label("artifact_readiness_reason"),
            artifact["readiness"]["dataset_status"].as_string().label("artifact_dataset_status"),
            artifact["readiness"]["results_status"].as_string().label("artifact_results_status"),
            artifact["readiness"]["comparison_status"].as_string().label("artifact_comparison_status"),
        )
        .where(ReportMaterializationRecord.run_id.in_(wanted))
    )


def _status_from_projection(
    row: Mapping[str, Any],
    *,
    contract_version: str,
    input_fingerprint: Optional[str] = None,
    freshness_verified: bool,
) -> Dict[str, Any]:
    run_id = str(row.get("run_id") or "")
    raw_status = str(row.get("status") or REPORT_STATUS_NOT_STARTED)
    artifact_valid = bool(row.get("artifact_valid"))
    effective_status = (
        REPORT_STATUS_STALE
        if raw_status == REPORT_STATUS_READY and not artifact_valid
        else raw_status
    )
    status = {
        "run_id": run_id,
        "status": effective_status,
        "contract_version": row.get("contract_version"),
        "report_schema_version": row.get("report_schema_version"),
        "dataset_schema_version": row.get("dataset_schema_version"),
        "builder_source_revision": row.get("builder_source_revision"),
        "storage_schema_version": row.get("storage_schema_version"),
        "artifact_id": row.get("artifact_id"),
        "artifact_path": None,
        "built_at": _dt_iso(row.get("built_at")),
        "started_at": _dt_iso(row.get("started_at")),
        "duration_ms": row.get("duration_ms"),
        "error": row.get("error"),
        "stale_reason": row.get("stale_reason") or (
            "missing_artifact" if effective_status == REPORT_STATUS_STALE and not artifact_valid else None
        ),
        "cache_key": row.get("cache_key"),
        "input_fingerprint": row.get("input_fingerprint"),
        "input_fingerprint_payload": dict(row.get("input_fingerprint_payload") or {}),
        "source_event_count": int(row.get("source_event_count") or 0),
        "source_event_high_water_run_seq": int(row.get("source_event_high_water_run_seq") or 0),
        "source_trade_count": int(row.get("source_trade_count") or 0),
        "source_run_updated_at": _dt_iso(row.get("source_run_updated_at")),
        "can_view": effective_status == REPORT_STATUS_READY and artifact_valid,
        "can_build": effective_status in {REPORT_STATUS_NOT_STARTED, REPORT_STATUS_FAILED, REPORT_STATUS_STALE},
        "can_retry": effective_status == REPORT_STATUS_FAILED,
        "verification": "fingerprint_verified" if freshness_verified else "stored_observation",
        "freshness_verified": bool(freshness_verified),
        "artifact_readiness": {
            "dataset_ready": bool(row.get("artifact_dataset_ready")),
            "results_ready": bool(row.get("artifact_results_ready")),
            "safe_to_compare": bool(row.get("artifact_safe_to_compare")),
            "reason": row.get("artifact_readiness_reason"),
            "dataset_status": row.get("artifact_dataset_status"),
            "results_status": row.get("artifact_results_status"),
            "comparison_status": row.get("artifact_comparison_status"),
        },
    }
    if row.get("contract_version") != contract_version:
        return _mark_status_stale(status, "contract_version_changed")
    if row.get("report_schema_version") != REPORT_SCHEMA_VERSION:
        return _mark_status_stale(status, "report_schema_version_changed")
    if row.get("dataset_schema_version") != REPORT_DATASET_SCHEMA_VERSION:
        return _mark_status_stale(status, "dataset_schema_version_changed")
    if raw_status != REPORT_STATUS_NOT_STARTED and row.get("builder_source_revision") != source_revision():
        return _mark_status_stale(status, "builder_source_revision_changed")
    if raw_status == REPORT_STATUS_READY:
        expected = str(input_fingerprint or "").strip()
        actual = str(row.get("input_fingerprint") or "").strip()
        if not actual:
            return _mark_status_stale(status, "input_fingerprint_missing")
        if expected and actual != expected:
            return _mark_status_stale(status, "input_fingerprint_changed")
    return status


def compute_report_input_fingerprint(run_id: str) -> Dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report input fingerprint")
    if not db.available:
        raise RuntimeError("Database not available for report input fingerprint")
    with db.session() as session:
        fingerprints = _fingerprints_for_run_ids(session, [normalized_run_id])
    result = fingerprints.get(normalized_run_id)
    if result is None:
        raise KeyError(normalized_run_id)
    return result


def _mark_status_stale(status: Dict[str, Any], reason: str) -> Dict[str, Any]:
    status["status"] = REPORT_STATUS_STALE
    status["stale_reason"] = reason
    status["can_view"] = False
    status["can_build"] = True
    status["can_retry"] = False
    return status


def _record_status(
    record: ReportMaterializationRecord | None,
    run_id: str,
    *,
    contract_version: str,
    input_fingerprint: Optional[str] = None,
) -> Dict[str, Any]:
    if record is None:
        return _empty_status(run_id, contract_version=contract_version)
    status = record.to_dict()
    if record.contract_version != contract_version:
        return _mark_status_stale(status, "contract_version_changed")
    if record.report_schema_version != REPORT_SCHEMA_VERSION:
        return _mark_status_stale(status, "report_schema_version_changed")
    if record.dataset_schema_version != REPORT_DATASET_SCHEMA_VERSION:
        return _mark_status_stale(status, "dataset_schema_version_changed")
    if record.status != REPORT_STATUS_NOT_STARTED and record.builder_source_revision != source_revision():
        return _mark_status_stale(status, "builder_source_revision_changed")
    if record.status == REPORT_STATUS_READY:
        expected = str(input_fingerprint or "").strip()
        actual = str(record.input_fingerprint or "").strip()
        if not actual:
            return _mark_status_stale(status, "input_fingerprint_missing")
        if expected and actual != expected:
            return _mark_status_stale(status, "input_fingerprint_changed")
    return status


def _report_materialization_lock_key(run_id: str) -> int:
    digest = hashlib.blake2b(
        str(run_id or "").strip().encode("utf-8"),
        digest_size=8,
        person=_REPORT_MATERIALIZATION_LOCK_PERSON,
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=False) & ((1 << 63) - 1)


def get_report_materialization_status(
    run_id: str,
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
) -> Dict[str, Any]:
    """Return persisted report materialization state for a run."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return _empty_status(normalized_run_id, contract_version=contract_version)
    if not db.available:
        raise RuntimeError("Database not available for report materialization status")
    with db.session() as session:
        fingerprints = _fingerprints_for_run_ids(session, [normalized_run_id])
        fingerprint = str((fingerprints.get(normalized_run_id) or {}).get("input_fingerprint") or "").strip()
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        return _record_status(
            record,
            normalized_run_id,
            contract_version=contract_version,
            input_fingerprint=fingerprint,
        )


def list_report_materialization_statuses(
    run_ids: list[str],
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
) -> Dict[str, Dict[str, Any]]:
    """Return materialization states keyed by run id."""

    normalized = [str(run_id or "").strip() for run_id in run_ids]
    wanted = [run_id for run_id in dict.fromkeys(normalized) if run_id]
    if not wanted:
        return {}
    if not db.available:
        raise RuntimeError("Database not available for report materialization status")
    with db.session() as session:
        fingerprints = _fingerprints_for_run_ids(session, wanted)
        rows = session.execute(_status_projection_query(wanted)).mappings().all()
    statuses = {run_id: _empty_status(run_id, contract_version=contract_version) for run_id in wanted}
    for row in rows:
        run_id = str(row.get("run_id") or "")
        fingerprint = str((fingerprints.get(run_id) or {}).get("input_fingerprint") or "").strip()
        statuses[run_id] = _status_from_projection(
            row,
            contract_version=contract_version,
            input_fingerprint=fingerprint,
            freshness_verified=True,
        )
    return statuses


def list_report_materialization_observations(
    run_ids: list[str],
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
) -> Dict[str, Dict[str, Any]]:
    """Return stored catalog state without rescanning run event/trade ledgers.

    This is intentionally an observed hot-read contract. Exact artifact reads
    and report-detail readiness continue to recompute the input fingerprint.
    """

    normalized = [str(run_id or "").strip() for run_id in run_ids]
    wanted = [run_id for run_id in dict.fromkeys(normalized) if run_id]
    if not wanted:
        return {}
    if not db.available:
        raise RuntimeError("Database not available for report materialization status")
    with db.session() as session:
        rows = session.execute(_status_projection_query(wanted)).mappings().all()
    observations = {
        run_id: {
            **_empty_status(run_id, contract_version=contract_version),
            "verification": "stored_observation",
            "freshness_verified": False,
        }
        for run_id in wanted
    }
    for row in rows:
        run_id = str(row.get("run_id") or "")
        observations[run_id] = _status_from_projection(
            row,
            contract_version=contract_version,
            input_fingerprint=str(row.get("input_fingerprint") or "").strip(),
            freshness_verified=False,
        )
    return observations


def get_materialized_run_report(
    run_id: str,
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
) -> Optional[Dict[str, Any]]:
    """Return a ready materialized RunReportDTO payload, if present."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not db.available:
        return None
    with db.session() as session:
        fingerprints = _fingerprints_for_run_ids(session, [normalized_run_id])
        fingerprint = str((fingerprints.get(normalized_run_id) or {}).get("input_fingerprint") or "").strip()
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        if record is None:
            return None
        if record.contract_version != contract_version:
            return None
        if record.report_schema_version != REPORT_SCHEMA_VERSION:
            return None
        if record.dataset_schema_version != REPORT_DATASET_SCHEMA_VERSION:
            return None
        if record.builder_source_revision != source_revision():
            return None
        if not fingerprint or str(record.input_fingerprint or "").strip() != fingerprint:
            return None
        if record.status != REPORT_STATUS_READY or not isinstance(record.artifact, Mapping):
            return None
        return dict(record.artifact)


def claim_report_materialization_build(
    run_id: str,
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
    cache_key: Optional[str] = None,
    input_fingerprint: Optional[str] = None,
    input_fingerprint_payload: Optional[Mapping[str, Any]] = None,
    force: bool = False,
) -> Tuple[Dict[str, Any], bool, bool]:
    """Mark a run report build as in progress.

    Returns ``(status, claimed, joined)``. ``joined`` means another builder is
    already responsible for this run, so callers must not start a duplicate.
    """

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report materialization")
    if not db.available:
        raise RuntimeError("Database not available for report materialization")
    with db.session() as session:
        session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": _report_materialization_lock_key(normalized_run_id)},
        )
        now = _utcnow()
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        expected_fingerprint = str(input_fingerprint or "").strip()
        if not expected_fingerprint:
            raise ValueError("input_fingerprint is required for report materialization")
        if record is None:
            record = ReportMaterializationRecord(run_id=normalized_run_id)
            record.created_at = now
            session.add(record)
        elif (
            record.contract_version == contract_version
            and record.report_schema_version == REPORT_SCHEMA_VERSION
            and record.dataset_schema_version == REPORT_DATASET_SCHEMA_VERSION
            and record.builder_source_revision == source_revision()
            and record.to_dict().get("can_view")
            and str(record.input_fingerprint or "").strip() == expected_fingerprint
            and not force
        ):
            return record.to_dict(), False, False
        elif (
            record.status == REPORT_STATUS_BUILDING
            and record.contract_version == contract_version
            and record.report_schema_version == REPORT_SCHEMA_VERSION
            and record.dataset_schema_version == REPORT_DATASET_SCHEMA_VERSION
            and record.builder_source_revision == source_revision()
            and str(record.input_fingerprint or "").strip() == expected_fingerprint
            and not force
        ):
            return record.to_dict(), False, True

        record.contract_version = contract_version
        record.report_schema_version = REPORT_SCHEMA_VERSION
        record.dataset_schema_version = REPORT_DATASET_SCHEMA_VERSION
        record.builder_source_revision = source_revision()
        record.storage_schema_version = REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION
        record.status = REPORT_STATUS_BUILDING
        record.cache_key = cache_key
        record.input_fingerprint = expected_fingerprint or None
        record.input_fingerprint_payload = _json_safe(dict(input_fingerprint_payload or {}))
        record.source_event_count = int((input_fingerprint_payload or {}).get("event_count") or 0)
        record.source_event_high_water_run_seq = int((input_fingerprint_payload or {}).get("event_high_water_run_seq") or 0)
        record.source_trade_count = int((input_fingerprint_payload or {}).get("trade_count") or 0)
        record.source_run_updated_at = _source_run_updated_at(input_fingerprint_payload)
        record.stale_reason = None
        record.error = None
        record.started_at = now
        record.built_at = None
        record.duration_ms = None
        record.updated_at = now
        if force:
            record.artifact = None
            record.artifact_id = None
        return record.to_dict(), True, False


def store_materialized_run_report(
    run_id: str,
    payload: Mapping[str, Any],
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
    cache_key: Optional[str] = None,
    input_fingerprint: Optional[str] = None,
    input_fingerprint_payload: Optional[Mapping[str, Any]] = None,
    duration_ms: Optional[float] = None,
) -> Dict[str, Any]:
    """Persist a completed RunReportDTO artifact."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report materialization")
    if not db.available:
        raise RuntimeError("Database not available for report materialization")
    with db.session() as session:
        now = _utcnow()
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        if record is None:
            record = ReportMaterializationRecord(run_id=normalized_run_id)
            record.created_at = now
            session.add(record)
        record.contract_version = contract_version
        record.report_schema_version = REPORT_SCHEMA_VERSION
        record.dataset_schema_version = REPORT_DATASET_SCHEMA_VERSION
        record.builder_source_revision = source_revision()
        record.storage_schema_version = REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION
        record.status = REPORT_STATUS_READY
        fingerprint = str(input_fingerprint or "").strip()
        if not fingerprint:
            raise ValueError("input_fingerprint is required for report materialization")
        record.artifact_id = f"{normalized_run_id}:{contract_version}:{fingerprint}"
        record.artifact = _json_safe(dict(payload))
        record.cache_key = cache_key
        record.input_fingerprint = fingerprint or None
        record.input_fingerprint_payload = _json_safe(dict(input_fingerprint_payload or {}))
        record.source_event_count = int((input_fingerprint_payload or {}).get("event_count") or 0)
        record.source_event_high_water_run_seq = int((input_fingerprint_payload or {}).get("event_high_water_run_seq") or 0)
        record.source_trade_count = int((input_fingerprint_payload or {}).get("trade_count") or 0)
        record.source_run_updated_at = _source_run_updated_at(input_fingerprint_payload)
        record.error = None
        record.stale_reason = None
        record.built_at = now
        record.duration_ms = duration_ms
        record.updated_at = now
        return record.to_dict()


def mark_report_materialization_failed(
    run_id: str,
    *,
    error: str,
    contract_version: str = REPORT_CONTRACT_VERSION,
    cache_key: Optional[str] = None,
    input_fingerprint: Optional[str] = None,
    input_fingerprint_payload: Optional[Mapping[str, Any]] = None,
    duration_ms: Optional[float] = None,
) -> Dict[str, Any]:
    """Persist report materialization failure without changing run lifecycle."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report materialization")
    if not db.available:
        raise RuntimeError("Database not available for report materialization")
    with db.session() as session:
        now = _utcnow()
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        if record is None:
            record = ReportMaterializationRecord(run_id=normalized_run_id)
            record.created_at = now
            session.add(record)
        record.contract_version = contract_version
        record.report_schema_version = REPORT_SCHEMA_VERSION
        record.dataset_schema_version = REPORT_DATASET_SCHEMA_VERSION
        record.builder_source_revision = source_revision()
        record.storage_schema_version = REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION
        record.status = REPORT_STATUS_FAILED
        record.cache_key = cache_key
        fingerprint = str(input_fingerprint or "").strip()
        if not fingerprint:
            raise ValueError("input_fingerprint is required for report materialization")
        record.input_fingerprint = fingerprint
        record.input_fingerprint_payload = _json_safe(dict(input_fingerprint_payload or {}))
        record.source_event_count = int((input_fingerprint_payload or {}).get("event_count") or 0)
        record.source_event_high_water_run_seq = int((input_fingerprint_payload or {}).get("event_high_water_run_seq") or 0)
        record.source_trade_count = int((input_fingerprint_payload or {}).get("trade_count") or 0)
        record.source_run_updated_at = _source_run_updated_at(input_fingerprint_payload)
        record.error = str(error or "")[:2048] or "unknown_report_materialization_failure"
        record.stale_reason = None
        record.built_at = now
        record.duration_ms = duration_ms
        record.updated_at = now
        return record.to_dict()


def reset_report_materialization(
    run_id: str,
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
) -> Dict[str, Any]:
    """Reset a run report status so it can be built again."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report materialization")
    if not db.available:
        raise RuntimeError("Database not available for report materialization")
    try:
        with db.session() as session:
            now = _utcnow()
            record = session.get(ReportMaterializationRecord, normalized_run_id)
            if record is None:
                record = ReportMaterializationRecord(run_id=normalized_run_id)
                record.created_at = now
                session.add(record)
            record.contract_version = contract_version
            record.report_schema_version = REPORT_SCHEMA_VERSION
            record.dataset_schema_version = REPORT_DATASET_SCHEMA_VERSION
            record.builder_source_revision = None
            record.storage_schema_version = REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION
            record.status = REPORT_STATUS_NOT_STARTED
            record.artifact = None
            record.artifact_id = None
            record.cache_key = None
            record.input_fingerprint = None
            record.input_fingerprint_payload = None
            record.source_event_count = 0
            record.source_event_high_water_run_seq = 0
            record.source_trade_count = 0
            record.source_run_updated_at = None
            record.error = None
            record.stale_reason = None
            record.started_at = None
            record.built_at = None
            record.duration_ms = None
            record.updated_at = now
            return record.to_dict()
    except SQLAlchemyError as exc:
        logger.error("report_materialization_reset_failed | run_id=%s | error=%s", normalized_run_id, exc)
        raise


__all__ = [
    "REPORT_CONTRACT_VERSION",
    "REPORT_STATUS_BUILDING",
    "REPORT_STATUS_FAILED",
    "REPORT_STATUS_NOT_STARTED",
    "REPORT_STATUS_READY",
    "REPORT_STATUS_STALE",
    "claim_report_materialization_build",
    "compute_report_input_fingerprint",
    "get_materialized_run_report",
    "get_report_materialization_status",
    "list_report_materialization_statuses",
    "list_report_materialization_observations",
    "mark_report_materialization_failed",
    "reset_report_materialization",
    "store_materialized_run_report",
]
