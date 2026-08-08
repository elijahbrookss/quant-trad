"""Application service for exact Strategy-independent frozen Dataset bindings."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from market_data.contracts import (
    DATASET_IDENTITY_HASH_VERSION,
    DatasetSeriesRequest,
    build_dataset_identity_hash,
)
from market_data.fact_registry import get_fact_contract
from market_data.frozen import (
    build_frozen_market_data_read_binding,
    semantic_hash,
)
from market_data.store import MarketDataStore

from ..storage.repos.market_data import market_data_repo
from . import instrument_service
from .backtest_dataset_service import (
    dataset_manifest_hash_payload,
    validate_frozen_dataset_series,
)


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"frozen_dataset_binding_invalid: {field} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"frozen_dataset_binding_invalid: {field} must be ISO-8601"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _series_matches(entry: Mapping[str, Any], requirement: Mapping[str, Any]) -> bool:
    contract = get_fact_contract(str(requirement.get("fact_type") or ""))
    dimensions = contract.normalize_dimensions(requirement.get("dimensions"))
    return (
        str(entry.get("instrument_id") or "")
        == str(requirement.get("instrument_id") or "")
        and str(entry.get("fact_type") or "").strip().lower()
        == str(requirement.get("fact_type") or "").strip().lower()
        and str(entry.get("contract_version") or "")
        == str(requirement.get("contract_version") or "")
        and (
            int(entry["timeframe_seconds"])
            if entry.get("timeframe_seconds") is not None
            else None
        )
        == (
            int(requirement["timeframe_seconds"])
            if requirement.get("timeframe_seconds") is not None
            else None
        )
        and dict(entry.get("dimensions") or {}) == dimensions
    )


def _source_matches(details: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    for field, expected_value in expected.items():
        if field in {"mode", "series_id", "source_identity_key", "source_identity_keys"}:
            continue
        actual = details.get(field)
        if str(actual or "").strip().lower() != str(expected_value or "").strip().lower():
            return False
    return True


def _record_source(record: Any) -> tuple[str, dict[str, Any]]:
    source = getattr(record, "source", None)
    key = str(
        getattr(record, "source_identity_key", None)
        or getattr(source, "identity_key", None)
        or ""
    ).strip()
    if not key:
        return "", {}
    return key, {
        "provider": str(getattr(source, "provider", "") or ""),
        "venue": str(getattr(source, "venue", "") or ""),
        "source_kind": str(getattr(source, "source_kind", "") or ""),
        "adapter_version": str(getattr(source, "adapter_version", "") or ""),
    }


def matching_source_identity_keys(
    records: Sequence[Any], policy: Mapping[str, Any]
) -> list[str]:
    sources: dict[str, dict[str, Any]] = {}
    for record in records:
        key, details = _record_source(record)
        if key:
            sources[key] = details
    available = sorted(sources)
    mode = str(policy.get("mode") or "").strip().lower()
    if mode == "exact":
        expected_key = str(policy.get("source_identity_key") or "").strip()
        if expected_key:
            selected = [expected_key] if expected_key in sources else []
        else:
            raw_binding = policy.get("provider_binding")
            if raw_binding is not None and not isinstance(raw_binding, Mapping):
                raise ValueError(
                    "frozen_dataset_preparation_invalid: provider_binding must be an object"
                )
            binding = dict(raw_binding or {})
            selected = [
                key
                for key in available
                if _source_matches(sources[key], binding)
            ]
            if not binding and len(available) == 1:
                selected = available
        return selected if len(selected) == 1 else []
    if mode == "allowlist":
        allowed = {
            str(value).strip()
            for value in policy.get("source_identity_keys") or []
            if str(value).strip()
        }
        return sorted(set(available).intersection(allowed))
    raise ValueError(
        "frozen_dataset_preparation_invalid: source policy must be exact or allowlist"
    )


def project_frozen_dataset(dataset: Any) -> dict[str, Any]:
    """Return the canonical immutable Dataset identity projection."""

    return {
        "schema_version": "market_dataset.v1",
        "contract_version": dataset.contract_version,
        "name": dataset.name,
        "purpose": dataset.purpose,
        "metadata": dict(dataset.metadata),
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "max_commit_seq": dataset.max_commit_seq,
        "reused_existing": bool(getattr(dataset, "reused_existing", False)),
        "series": [dict(row) for row in dataset.series],
    }


def _resolve_source_binding(
    entry: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    mode = str(policy.get("mode") or "").strip().lower()
    if mode not in {"exact", "allowlist"}:
        raise ValueError(
            "frozen_dataset_binding_invalid: evidence source policy must be exact or allowlist"
        )
    sources = dict((entry.get("source_summary") or {}).get("sources") or {})
    available = sorted(str(key) for key in sources)
    if not available:
        raise ValueError(
            "frozen_dataset_binding_invalid: frozen series has no provider provenance"
        )
    if mode == "exact":
        expected_key = str(policy.get("source_identity_key") or "").strip()
        if expected_key:
            selected = [expected_key] if expected_key in sources else []
        else:
            provider_binding = policy.get("provider_binding")
            if provider_binding is not None and not isinstance(provider_binding, Mapping):
                raise ValueError(
                    "frozen_dataset_binding_invalid: provider_binding must be an object"
                )
            constraints = dict(provider_binding or {})
            selected = [
                key
                for key in available
                if _source_matches(dict(sources[key] or {}), constraints)
            ]
            if not constraints and len(available) == 1:
                selected = available
        if len(selected) != 1:
            raise ValueError(
                "frozen_dataset_binding_invalid: exact source policy did not resolve one provider binding"
            )
    else:
        allowed = sorted(
            {
                str(value).strip()
                for value in policy.get("source_identity_keys") or []
                if str(value).strip()
            }
        )
        if not allowed:
            raise ValueError(
                "frozen_dataset_binding_invalid: allowlist source policy is empty"
            )
        selected = sorted(set(available).intersection(allowed))
        if not selected:
            raise ValueError(
                "frozen_dataset_binding_invalid: no allowlisted source exists in Dataset"
            )
    return {
        "schema_version": "market_data_source_binding.v1",
        "mode": mode,
        "series_id": int(entry["series_id"]),
        "resolved_source_identity_keys": selected,
        "sources": {key: dict(sources[key] or {}) for key in selected},
        "selection_rule": "latest_known_then_commit_seq_then_source_identity.v1",
    }


def prepare_frozen_dataset_from_requirements(
    *,
    requirements: Sequence[Mapping[str, Any]],
    freeze: bool,
    name: str | None = None,
    purpose: str = "research",
    created_by: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    store: MarketDataStore = market_data_repo,
    instrument_loader: Callable[[str], Mapping[str, Any]] = instrument_service.get_instrument_record,
) -> dict[str, Any]:
    """Resolve source-bound requirements and optionally freeze known reality.

    The operation performs no provider acquisition and makes no consumer
    readiness decision. Gaps are retained as Dataset evidence for consumers to
    interpret under their own versioned policies.
    """

    if not requirements:
        raise ValueError(
            "frozen_dataset_preparation_invalid: at least one requirement is required"
        )
    watermark = int(store.current_commit_seq())
    resolved: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    aliases: set[str] = set()
    for raw in requirements:
        requirement = dict(raw)
        alias = str(requirement.get("alias") or "").strip()
        if not alias or alias in aliases:
            raise ValueError(
                "frozen_dataset_preparation_invalid: requirements need unique aliases"
            )
        aliases.add(alias)
        start = _utc(requirement.get("required_start"), field=f"{alias}.required_start")
        end = _utc(requirement.get("required_end"), field=f"{alias}.required_end")
        policy = dict(requirement.get("source_policy") or {})
        candidates = [
            dict(row)
            for row in store.list_series(
                instrument_id=str(requirement.get("instrument_id") or "")
            )
            if _series_matches(row, requirement)
        ]
        requested_series_id = policy.get("series_id")
        if requested_series_id not in (None, ""):
            candidates = [
                row
                for row in candidates
                if int(row["series_id"]) == int(requested_series_id)
            ]
        matches: list[dict[str, Any]] = []
        for candidate in candidates:
            records = list(
                store.read_series_records(
                    series_id=int(candidate["series_id"]),
                    start=start,
                    end=end,
                    as_of_commit_seq=watermark,
                )
            )
            if not records:
                continue
            source_keys = matching_source_identity_keys(records, policy)
            if source_keys:
                matches.append(
                    {
                        "series_id": int(candidate["series_id"]),
                        "series_identity_key": str(candidate.get("identity_key") or ""),
                        "required_start": _iso(start),
                        "required_end": _iso(end),
                        "resolved_source_identity_keys": source_keys,
                        "record_count_at_watermark": len(records),
                    }
                )
        if len(matches) != 1:
            unresolved.append(
                {
                    "alias": alias,
                    "reason": (
                        "series_or_source_missing"
                        if not matches
                        else "series_or_source_ambiguous"
                    ),
                    "candidate_series_ids": sorted(
                        int(row["series_id"]) for row in candidates
                    ),
                    "matching_series_ids": sorted(
                        int(row["series_id"]) for row in matches
                    ),
                    "source_policy": policy,
                }
            )
            continue
        resolved.append({"alias": alias, **matches[0]})

    base = {
        "schema_version": "frozen_dataset_preparation.v1",
        "status": "blocked" if unresolved else "ready_to_freeze",
        "provider_call_performed": False,
        "acquisition_performed": False,
        "as_of_commit_seq": watermark,
        "resolved_requirements": resolved,
        "unresolved_requirements": unresolved,
        "dataset": None,
        "binding": None,
    }
    if unresolved or not freeze:
        return base

    ranges_by_series: dict[int, tuple[datetime, datetime]] = {}
    for row in resolved:
        series_id = int(row["series_id"])
        start = _utc(row["required_start"], field="required_start")
        end = _utc(row["required_end"], field="required_end")
        current = ranges_by_series.get(series_id)
        ranges_by_series[series_id] = (
            min(start, current[0]) if current else start,
            max(end, current[1]) if current else end,
        )
    dataset = store.freeze_dataset(
        [
            DatasetSeriesRequest(series_id=series_id, start=start, end=end)
            for series_id, (start, end) in sorted(ranges_by_series.items())
        ],
        name=name,
        purpose=purpose,
        created_by=created_by,
        metadata={
            **dict(metadata or {}),
            "schema_version": "frozen_dataset_preparation_request.v1",
            "requirements": [dict(row) for row in requirements],
            "resolved_requirements": resolved,
            "planning_watermark": watermark,
            "provider_access": False,
        },
    )
    binding = resolve_frozen_dataset_read_binding(
        dataset_id=dataset.dataset_id,
        requirements=requirements,
        store=store,
        instrument_loader=instrument_loader,
    )
    return {
        **base,
        "status": "frozen",
        "dataset": project_frozen_dataset(dataset),
        "binding": binding,
    }


def resolve_frozen_dataset_read_binding(
    *,
    dataset_id: str,
    requirements: Sequence[Mapping[str, Any]],
    store: MarketDataStore = market_data_repo,
    instrument_loader: Callable[[str], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Resolve and verify exact Dataset series without consumer readiness policy."""

    instrument_loader = instrument_loader or instrument_service.get_instrument_record
    normalized_id = str(dataset_id or "").strip()
    if not normalized_id:
        raise ValueError("frozen_dataset_binding_invalid: dataset_id is required")
    dataset = store.get_dataset(normalized_id)
    if dataset.contract_version != DATASET_IDENTITY_HASH_VERSION:
        raise ValueError("frozen_dataset_binding_invalid: unsupported Dataset contract")
    if dataset.dataset_id != f"mds_{dataset.dataset_hash[:32]}":
        raise RuntimeError("frozen_dataset_binding_invalid: Dataset identity disagreement")
    reconstructed_hash = build_dataset_identity_hash(
        [dataset_manifest_hash_payload(row) for row in dataset.series]
    )
    if reconstructed_hash != dataset.dataset_hash:
        raise RuntimeError("frozen_dataset_binding_invalid: Dataset manifest hash disagreement")

    verified_by_series: dict[int, dict[str, Any]] = {}
    selected_series: list[dict[str, Any]] = []
    recorded_gaps: list[dict[str, Any]] = []
    instrument_ids: set[str] = set()
    aliases: set[str] = set()
    for raw_requirement in requirements:
        requirement = dict(raw_requirement)
        alias = str(requirement.get("alias") or "").strip()
        if not alias or alias in aliases:
            raise ValueError(
                "frozen_dataset_binding_invalid: requirements need unique aliases"
            )
        aliases.add(alias)
        candidates = [
            dict(row) for row in dataset.series if _series_matches(row, requirement)
        ]
        policy = dict(requirement.get("source_policy") or {})
        requested_series_id = policy.get("series_id")
        if requested_series_id not in (None, ""):
            candidates = [
                row
                for row in candidates
                if int(row["series_id"]) == int(requested_series_id)
            ]
        if len(candidates) != 1:
            raise ValueError(
                "frozen_dataset_binding_invalid: requirement did not resolve one frozen series "
                f"alias={alias} matches={len(candidates)}"
            )
        entry = candidates[0]
        required_start = _utc(requirement.get("required_start"), field=f"{alias}.required_start")
        required_end = _utc(requirement.get("required_end"), field=f"{alias}.required_end")
        if (
            required_start < _utc(entry.get("range_start"), field="series.range_start")
            or required_end > _utc(entry.get("range_end"), field="series.range_end")
        ):
            raise ValueError(
                f"frozen_dataset_range_missing: alias={alias} required range is not frozen"
            )
        source_binding = _resolve_source_binding(entry, policy)
        series_id = int(entry["series_id"])
        if series_id not in verified_by_series:
            verified, _quality, _records = validate_frozen_dataset_series(
                store=store,
                entry={**entry, "dataset_id": dataset.dataset_id},
                allow_any_recorded_gap=True,
            )
            verified_by_series[series_id] = verified
        selected = {
            **verified_by_series[series_id],
            "alias": alias,
            "source_binding": source_binding,
            "requirement": requirement,
        }
        selected_series.append(selected)
        instrument_ids.add(str(entry["instrument_id"]))
        for quality in selected.get("quality_evidence") or []:
            if quality.get("start") is None or quality.get("end") is None:
                continue
            recorded_gaps.append(
                {
                    "alias": alias,
                    "series_id": series_id,
                    **dict(quality),
                }
            )

    subjects = []
    for instrument_id in sorted(instrument_ids):
        snapshot = dict(instrument_loader(instrument_id))
        subjects.append(
            {
                "instrument_id": instrument_id,
                "snapshot_hash": semantic_hash(
                    {
                        key: value
                        for key, value in snapshot.items()
                        if key not in {"created_at", "updated_at"}
                    }
                ),
                "snapshot": snapshot,
            }
        )
    quality_material = {
        "status": "recorded",
        "series": [
            {
                "alias": row["alias"],
                "series_id": int(row["series_id"]),
                "quality_hash": row["quality_hash"],
                "quality_summary": dict(row.get("quality_summary") or {}),
            }
            for row in selected_series
        ],
        "recorded_gap_count": len(recorded_gaps),
    }
    return build_frozen_market_data_read_binding(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        max_commit_seq=dataset.max_commit_seq,
        series=selected_series,
        subjects=subjects,
        recorded_gaps=recorded_gaps,
        quality={
            **quality_material,
            "quality_evidence_hash": semantic_hash(quality_material),
        },
    )


__all__ = [
    "prepare_frozen_dataset_from_requirements",
    "project_frozen_dataset",
    "resolve_frozen_dataset_read_binding",
    "matching_source_identity_keys",
]
