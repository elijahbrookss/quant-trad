"""Explicit native candle acquisition for an already-declared Check plan.

No evaluator, provider fallback, gap filling, numeric acquisition authority,
or new storage path lives here. All jobs are validated before the first write.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from core.execution_control import consume_execution_resource, execution_checkpoint
from data_providers.registry import FeatureAuth, feature_contract
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION
from portal.backend.service.market import instrument_service
from portal.backend.service.market.feed_service import historical_candle_ingestor
from portal.backend.service.market.frozen_dataset_service import matching_source_keys
from portal.backend.service.storage.repos.market_data import market_data_repo


_RANGE_REASONS = {"series_not_registered", "source_binding_unresolved", "no_facts_in_required_range"}


def _utc(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("check_preparation_invalid: acquisition ranges require UTC offsets")
    return parsed.astimezone(timezone.utc)


def acquire_check_inputs(*, requirements: Sequence[Mapping[str, Any]],
                         missing_coverage: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Acquire only unrecorded native candle ranges, then let the caller replan.

    Public feature authorization excludes credential/paid integrations here.
    Providers without an explicit native interval declaration remain blocked.
    Existing numeric acquisition permissions are not implied by this flag.
    """
    jobs: dict[tuple[str, str], dict[str, Any]] = {}
    blockers: list[dict[str, Any]] = []
    required = {str(row["alias"]): dict(row) for row in requirements}
    for missing in missing_coverage:
        execution_checkpoint()
        alias = str(missing["alias"])
        row = required[alias]
        reason = str(missing.get("reason") or "")
        try:
            if (row["fact_type"] != CANDLE_FACT_TYPE or row["contract_version"] != CANDLE_FACT_VERSION
                    or row.get("dimensions")):
                raise ValueError("native_candle_acquisition_unsupported: use the owning Fact acquisition contract")
            if reason not in _RANGE_REASONS | {"source_bound_interval_unrecorded"}:
                raise ValueError(f"check_preparation_acquisition_blocked: {reason}")
            instrument = dict(instrument_service.get_instrument_record(str(row["instrument_id"])))
            contract = feature_contract(instrument.get("datasource"), instrument.get("exchange"), "historical_candles")
            if contract.auth != FeatureAuth.PUBLIC:
                raise ValueError("check_preparation_acquisition_blocked: credential or external acquisition needs separate authorization")
            seconds = int(row["timeframe_seconds"])
            if seconds <= 0:
                raise ValueError("check_preparation_invalid: positive native timeframe required")
            interval, source = historical_candle_ingestor.native_request(instrument, timeframe_seconds=seconds)
            policy = dict(row.get("source_policy") or {})
            if not matching_source_keys({source.identity_key: asdict(source)}, policy):
                raise ValueError("check_preparation_acquisition_blocked: declared source does not match native provider")
            pinned_series = policy.get("series_id")
            if pinned_series not in (None, ""):
                candidates = market_data_repo.list_series_metadata(instrument_id=str(row["instrument_id"]))
                if not any(int(item.get("series_id") or item.get("id")) == int(pinned_series)
                           and item.get("fact_type") == CANDLE_FACT_TYPE
                           and item.get("contract_version") == CANDLE_FACT_VERSION
                           and item.get("timeframe_seconds") == seconds
                           and not item.get("dimensions") for item in candidates):
                    raise ValueError("check_preparation_acquisition_blocked: pinned series does not match native request")
            start = _utc(missing.get("start", row["required_start"]))
            end = _utc(missing.get("end", row["required_end"]))
            if not (_utc(row["required_start"]) <= start < end <= _utc(row["required_end"])):
                raise ValueError("check_preparation_invalid: acquisition escapes declared range")
            if start.timestamp() % seconds or end.timestamp() % seconds:
                raise ValueError("check_preparation_acquisition_blocked: native range must be grid aligned")
            if end > datetime.now(timezone.utc):
                raise ValueError("check_preparation_acquisition_blocked: range includes unclosed candles")
            key = (str(row["instrument_id"]), interval)
            job = jobs.setdefault(key, {"instrument": instrument, "instrument_id": key[0],
                "timeframe": interval, "source_identity_key": source.identity_key,
                "aliases": set(), "ranges": []})
            job["aliases"].add(alias)
            job["ranges"].append((start, end))
        except (ValueError, KeyError) as error:
            blockers.append({"alias": alias, "reason": str(error)})
    if blockers:
        return {"status": "acquisition_blocked", "receipts": [], "blockers": blockers,
                "provider_call_performed": False, "acquisition_performed": False}

    actions = []
    for job in jobs.values():
        merged: list[list[datetime]] = []
        for start, end in sorted(job.pop("ranges")):
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        seconds = int(next(row["timeframe_seconds"] for row in requirements
                           if str(row["alias"]) in job["aliases"]))
        for start, end in merged:
            actions.append({**job, "aliases": sorted(job["aliases"]),
                            "start": start.isoformat(), "end": end.isoformat(),
                            "nominal_slots": int((end - start).total_seconds()) // seconds})
    # Existing shared input/deadline/global-ownership budgets apply before writes.
    consume_execution_resource("input_rows", sum(job["nominal_slots"] for job in actions))
    receipts = []
    for action in actions:
        execution_checkpoint()
        public = {key: value for key, value in action.items() if key != "instrument"}
        try:
            result = historical_candle_ingestor.ingest_by_instrument(action["instrument"],
                start=action["start"], end=action["end"], interval=action["timeframe"])
        except (ValueError, RuntimeError) as error:
            # Cancellation/admission/budget failures are authoritative, never a partial success.
            execution_checkpoint()
            return {"status": "acquisition_failed", "receipts": receipts,
                    "blockers": [{**public, "reason": str(error), "partial_effects_possible": True}],
                    "provider_call_performed": True if receipts else None,
                    "acquisition_performed": True if receipts else None}
        receipts.append({**public, "source_id": result.source_id, "series_id": result.series_id,
                         "gap_evidence_count": result.gap_evidence_count, "outcome": asdict(result.outcome)})
    return {"status": "acquired", "receipts": receipts, "blockers": [],
            "provider_call_performed": bool(receipts), "acquisition_performed": bool(receipts)}
