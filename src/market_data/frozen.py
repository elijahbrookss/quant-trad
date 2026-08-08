"""Strategy-independent contract for provider-free frozen market-data reads."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from .contracts import DATASET_IDENTITY_HASH_VERSION
from .fact_registry import get_fact_contract


FROZEN_MARKET_DATA_READ_BINDING_VERSION = "frozen_market_data_read_binding.v1"
CAUSAL_KNOWN_AT_SEMANTICS_VERSION = "causal_known_at_lte_decision.v1"


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(
                f"frozen_market_data_read_binding_invalid: {field} is required"
            )
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                "frozen_market_data_read_binding_invalid: "
                f"{field} must be an ISO-8601 timestamp"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: Any, *, field: str) -> str:
    return (
        _utc(value, field=field)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _semantic_json(value: Any, *, field: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                "frozen_market_data_read_binding_invalid: "
                f"{field} must be finite"
            )
        return 0.0 if value == 0.0 else value
    if isinstance(value, datetime):
        return _iso_utc(value, field=field)
    if isinstance(value, Mapping):
        return {
            str(key): _semantic_json(item, field=f"{field}.{key}")
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [
            _semantic_json(item, field=f"{field}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ValueError(
        "frozen_market_data_read_binding_invalid: "
        f"{field} has unsupported type {type(value).__name__}"
    )


def semantic_hash(payload: Mapping[str, Any]) -> str:
    """Return the stable SHA-256 used by frozen-read and Check evidence contracts."""

    encoded = json.dumps(
        _semantic_json(payload, field="payload"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalized_source_binding(raw: Mapping[str, Any], *, series_id: int) -> dict[str, Any]:
    supplied = raw.get("source_binding")
    if supplied is not None and not isinstance(supplied, Mapping):
        raise ValueError(
            "frozen_market_data_read_binding_invalid: source_binding must be an object"
        )
    source_binding = dict(supplied or {})
    source_binding.setdefault("series_id", series_id)
    if int(source_binding.get("series_id") or 0) != series_id:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: source binding series ID differs"
        )
    if raw.get("dimensions") is not None:
        source_binding.setdefault("dimensions", dict(raw.get("dimensions") or {}))
    if raw.get("source_summary") is not None:
        source_binding.setdefault("source_summary", dict(raw.get("source_summary") or {}))
    normalized = _semantic_json(source_binding, field="source_binding")
    assert isinstance(normalized, dict)
    return normalized


def _normalize_series(values: Any, *, max_commit_seq: int) -> list[dict[str, Any]]:
    if not isinstance(values, list) or not values:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: at least one resolved series is required"
        )
    normalized: list[dict[str, Any]] = []
    seen_aliases: set[str] = set()
    for raw in values:
        if not isinstance(raw, Mapping):
            raise ValueError(
                "frozen_market_data_read_binding_invalid: series entries must be objects"
            )
        alias = str(raw.get("alias") or "").strip()
        if alias:
            if alias in seen_aliases:
                raise ValueError(
                    "frozen_market_data_read_binding_invalid: duplicate series alias"
                )
            seen_aliases.add(alias)
        instrument_id = str(raw.get("instrument_id") or "").strip()
        fact_type = str(raw.get("fact_type") or "").strip().lower()
        contract_version = str(raw.get("contract_version") or "").strip()
        raw_timeframe = raw.get("timeframe_seconds")
        timeframe_seconds = int(raw_timeframe) if raw_timeframe is not None else None
        try:
            contract = get_fact_contract(fact_type)
            contract.validate(
                contract_version=contract_version,
                timeframe_seconds=timeframe_seconds,
            )
            if not contract.dataset_eligible:
                raise ValueError(f"fact type is not dataset eligible: {fact_type}")
            series_id = int(raw.get("series_id"))
            row_count = int(raw.get("row_count"))
            series_commit_seq = int(raw.get("max_commit_seq"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "frozen_market_data_read_binding_invalid: malformed resolved fact series"
            ) from exc
        if not instrument_id or series_id <= 0 or row_count <= 0 or series_commit_seq <= 0:
            raise ValueError(
                "frozen_market_data_read_binding_invalid: series identity and counts must be positive"
            )
        if series_commit_seq > max_commit_seq:
            raise ValueError(
                "frozen_market_data_read_binding_invalid: series watermark exceeds dataset watermark"
            )
        range_start = _utc(raw.get("range_start"), field="series.range_start")
        range_end = _utc(raw.get("range_end"), field="series.range_end")
        if range_end <= range_start:
            raise ValueError(
                "frozen_market_data_read_binding_invalid: series range_end must be after range_start"
            )
        hashes = {
            name: str(raw.get(name) or "").strip()
            for name in ("material_hash", "provenance_hash", "quality_hash")
        }
        if not all(hashes.values()):
            raise ValueError(
                "frozen_market_data_read_binding_invalid: series semantic hashes are required"
            )
        source_binding = _normalized_source_binding(raw, series_id=series_id)
        source_binding_hash = semantic_hash(
            {
                "schema_version": "market_data_source_binding.v1",
                "source_binding": source_binding,
            }
        )
        supplied_source_hash = str(raw.get("source_binding_hash") or "").strip()
        if supplied_source_hash and supplied_source_hash != source_binding_hash:
            raise ValueError(
                "frozen_market_data_read_binding_invalid: source binding hash disagreement"
            )
        normalized.append(
            {
                **dict(raw),
                **({"alias": alias} if alias else {}),
                "series_id": series_id,
                "instrument_id": instrument_id,
                "fact_type": fact_type,
                "contract_version": contract_version,
                "timeframe_seconds": timeframe_seconds,
                "range_start": _iso_utc(range_start, field="series.range_start"),
                "range_end": _iso_utc(range_end, field="series.range_end"),
                "row_count": row_count,
                "max_commit_seq": series_commit_seq,
                **hashes,
                "source_binding": source_binding,
                "source_binding_hash": source_binding_hash,
            }
        )
    return sorted(
        normalized,
        key=lambda row: (
            str(row.get("alias") or ""),
            str(row["instrument_id"]),
            str(row["fact_type"]),
            int(row["timeframe_seconds"] or -1),
            int(row["series_id"]),
        ),
    )


def _binding_hash_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: payload[key]
        for key in (
            "schema_version",
            "dataset_contract_version",
            "dataset_id",
            "dataset_hash",
            "max_commit_seq",
            "known_at_semantics",
            "subjects",
            "series",
            "recorded_gaps",
            "quality",
            "provider_access",
            "provider_call_performed",
        )
    }


def normalize_frozen_market_data_read_binding(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and detach an immutable, exact-series, provider-free read binding."""

    if not isinstance(payload, Mapping):
        raise ValueError(
            "frozen_market_data_read_binding_invalid: binding must be an object"
        )
    schema_version = str(payload.get("schema_version") or "").strip()
    if schema_version != FROZEN_MARKET_DATA_READ_BINDING_VERSION:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: unsupported binding schema "
            f"{schema_version or '<missing>'}"
        )
    dataset_contract = str(payload.get("dataset_contract_version") or "").strip()
    if dataset_contract != DATASET_IDENTITY_HASH_VERSION:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: unsupported dataset contract "
            f"{dataset_contract or '<missing>'}"
        )
    dataset_id = str(payload.get("dataset_id") or "").strip()
    dataset_hash = str(payload.get("dataset_hash") or "").strip()
    if not dataset_id or not dataset_hash or dataset_id != f"mds_{dataset_hash[:32]}":
        raise ValueError(
            "frozen_market_data_read_binding_invalid: dataset identity disagreement"
        )
    try:
        max_commit_seq = int(payload.get("max_commit_seq"))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: max_commit_seq must be positive"
        ) from exc
    if max_commit_seq <= 0:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: max_commit_seq must be positive"
        )
    known_at_semantics = str(payload.get("known_at_semantics") or "").strip()
    if known_at_semantics != CAUSAL_KNOWN_AT_SEMANTICS_VERSION:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: unsupported known_at semantics"
        )
    if str(payload.get("provider_access") or "").strip().lower() != "disabled":
        raise ValueError(
            "frozen_market_data_read_binding_invalid: provider access must be disabled"
        )
    if payload.get("provider_call_performed") is not False:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: execution binding must be provider-free"
        )
    series = _normalize_series(payload.get("series"), max_commit_seq=max_commit_seq)
    raw_gaps = payload.get("recorded_gaps") or []
    if not isinstance(raw_gaps, list) or any(
        not isinstance(row, Mapping) for row in raw_gaps
    ):
        raise ValueError(
            "frozen_market_data_read_binding_invalid: recorded_gaps must be a list of objects"
        )
    quality = payload.get("quality") or {}
    if not isinstance(quality, Mapping):
        raise ValueError(
            "frozen_market_data_read_binding_invalid: quality must be an object"
        )
    raw_subjects = payload.get("subjects") or []
    if not isinstance(raw_subjects, list) or any(
        not isinstance(row, Mapping) for row in raw_subjects
    ):
        raise ValueError(
            "frozen_market_data_read_binding_invalid: subjects must be a list of objects"
        )
    subjects: list[dict[str, Any]] = []
    seen_subjects: set[str] = set()
    for raw_subject in raw_subjects:
        subject = _semantic_json(dict(raw_subject), field="subjects")
        assert isinstance(subject, dict)
        instrument_id = str(
            subject.get("instrument_id") or subject.get("id") or ""
        ).strip()
        if not instrument_id or instrument_id in seen_subjects:
            raise ValueError(
                "frozen_market_data_read_binding_invalid: subject instrument IDs must be unique"
            )
        seen_subjects.add(instrument_id)
        subject["instrument_id"] = instrument_id
        subjects.append(subject)
    series_instrument_ids = {str(row["instrument_id"]) for row in series}
    if seen_subjects and not seen_subjects.issubset(series_instrument_ids):
        raise ValueError(
            "frozen_market_data_read_binding_invalid: subjects are missing resolved series"
        )
    normalized = {
        **dict(payload),
        "schema_version": FROZEN_MARKET_DATA_READ_BINDING_VERSION,
        "dataset_contract_version": DATASET_IDENTITY_HASH_VERSION,
        "dataset_id": dataset_id,
        "dataset_hash": dataset_hash,
        "max_commit_seq": max_commit_seq,
        "known_at_semantics": CAUSAL_KNOWN_AT_SEMANTICS_VERSION,
        "subjects": sorted(subjects, key=lambda row: str(row["instrument_id"])),
        "series": series,
        "recorded_gaps": [
            _semantic_json(dict(row), field="recorded_gaps") for row in raw_gaps
        ],
        "quality": _semantic_json(dict(quality), field="quality"),
        "provider_access": "disabled",
        "provider_call_performed": False,
    }
    calculated_hash = semantic_hash(_binding_hash_payload(normalized))
    supplied_hash = str(payload.get("binding_hash") or "").strip()
    if supplied_hash and supplied_hash != calculated_hash:
        raise ValueError(
            "frozen_market_data_read_binding_invalid: binding hash disagreement"
        )
    normalized["binding_hash"] = calculated_hash
    return normalized


def build_frozen_market_data_read_binding(
    *,
    dataset_id: str,
    dataset_hash: str,
    max_commit_seq: int,
    series: Sequence[Mapping[str, Any]],
    subjects: Sequence[Mapping[str, Any]] = (),
    recorded_gaps: Sequence[Mapping[str, Any]] = (),
    quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the reusable frozen read contract without Strategy semantics."""

    return normalize_frozen_market_data_read_binding(
        {
            "schema_version": FROZEN_MARKET_DATA_READ_BINDING_VERSION,
            "dataset_contract_version": DATASET_IDENTITY_HASH_VERSION,
            "dataset_id": dataset_id,
            "dataset_hash": dataset_hash,
            "max_commit_seq": max_commit_seq,
            "known_at_semantics": CAUSAL_KNOWN_AT_SEMANTICS_VERSION,
            "subjects": [dict(row) for row in subjects],
            "series": [dict(row) for row in series],
            "recorded_gaps": [dict(row) for row in recorded_gaps],
            "quality": dict(quality or {}),
            "provider_access": "disabled",
            "provider_call_performed": False,
        }
    )


def bound_frozen_series_for_request(
    binding: Mapping[str, Any],
    *,
    instrument_id: str,
    timeframe_seconds: int | None,
    start: Any,
    end: Any,
    fact_type: str,
    contract_version: str,
    alias: str | None = None,
) -> dict[str, Any]:
    """Resolve one exact frozen series and reject any range expansion."""

    normalized = normalize_frozen_market_data_read_binding(binding)
    normalized_instrument_id = str(instrument_id or "").strip()
    normalized_fact_type = str(fact_type or "").strip().lower()
    normalized_contract = str(contract_version or "").strip()
    normalized_alias = str(alias or "").strip()
    timeframe = int(timeframe_seconds) if timeframe_seconds is not None else None
    requested_start = _utc(start, field="requested_start")
    requested_end = _utc(end, field="requested_end")
    matches = [
        row
        for row in normalized["series"]
        if row["instrument_id"] == normalized_instrument_id
        and row["timeframe_seconds"] == timeframe
        and row["fact_type"] == normalized_fact_type
        and row["contract_version"] == normalized_contract
        and (not normalized_alias or row.get("alias") == normalized_alias)
    ]
    if len(matches) != 1:
        raise ValueError(
            "frozen_market_data_series_missing: binding does not contain exactly one "
            f"series for alias={normalized_alias or '<unspecified>'} "
            f"instrument_id={normalized_instrument_id} fact_type={normalized_fact_type} "
            f"contract_version={normalized_contract} timeframe_seconds={timeframe}"
        )
    entry = dict(matches[0])
    bound_start = _utc(entry["range_start"], field="bound_start")
    bound_end = _utc(entry["range_end"], field="bound_end")
    if requested_end <= requested_start:
        raise ValueError(
            "frozen_market_data_range_invalid: requested end must be after start"
        )
    if requested_start < bound_start or requested_end > bound_end:
        raise ValueError(
            "frozen_market_data_range_expansion_forbidden: requested "
            f"[{_iso_utc(requested_start, field='requested_start')}, "
            f"{_iso_utc(requested_end, field='requested_end')}) outside frozen "
            f"[{_iso_utc(bound_start, field='bound_start')}, "
            f"{_iso_utc(bound_end, field='bound_end')})"
        )
    return entry


def bound_frozen_subject_for_id(
    binding: Mapping[str, Any], instrument_id: str
) -> dict[str, Any]:
    """Return the exact subject snapshot carried by a frozen read binding."""

    normalized = normalize_frozen_market_data_read_binding(binding)
    normalized_id = str(instrument_id or "").strip()
    matches = [
        row
        for row in normalized["subjects"]
        if str(row["instrument_id"]) == normalized_id
    ]
    if len(matches) != 1:
        raise ValueError(
            "frozen_market_data_subject_unbound: binding does not contain "
            f"instrument_id={normalized_id or '<missing>'}"
        )
    subject = dict(matches[0])
    snapshot = subject.get("snapshot")
    return dict(snapshot) if isinstance(snapshot, Mapping) else subject


def bound_frozen_subject_for_symbol(
    binding: Mapping[str, Any],
    *,
    datasource: Any,
    exchange: Any,
    symbol: Any,
) -> dict[str, Any]:
    """Resolve a symbol only from exact frozen subject snapshots."""

    normalized = normalize_frozen_market_data_read_binding(binding)
    expected = (
        str(datasource or "").strip().lower(),
        str(exchange or "").strip().lower(),
        str(symbol or "").strip().upper(),
    )
    matches: list[dict[str, Any]] = []
    for subject in normalized["subjects"]:
        raw = subject.get("snapshot")
        snapshot = dict(raw) if isinstance(raw, Mapping) else dict(subject)
        actual = (
            str(snapshot.get("datasource") or "").strip().lower(),
            str(snapshot.get("exchange") or "").strip().lower(),
            str(snapshot.get("symbol") or "").strip().upper(),
        )
        if actual == expected:
            matches.append(snapshot)
    if len(matches) != 1:
        raise ValueError(
            "frozen_market_data_subject_unbound: binding does not contain exactly "
            f"one subject for {expected}"
        )
    return matches[0]


__all__ = [
    "CAUSAL_KNOWN_AT_SEMANTICS_VERSION",
    "FROZEN_MARKET_DATA_READ_BINDING_VERSION",
    "bound_frozen_subject_for_id",
    "bound_frozen_subject_for_symbol",
    "bound_frozen_series_for_request",
    "build_frozen_market_data_read_binding",
    "normalize_frozen_market_data_read_binding",
    "semantic_hash",
]
