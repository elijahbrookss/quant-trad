"""Pure contracts for dataset-bound historical backtest execution."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from .contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    DATASET_IDENTITY_HASH_VERSION,
    OPEN_INTEREST_FACT_TYPE,
    OPEN_INTEREST_FACT_VERSION,
)
from .fact_registry import get_fact_contract


BACKTEST_DATASET_BINDING_VERSION = "backtest_dataset_binding.v1"
BACKTEST_DATASET_PLAN_VERSION = "backtest_dataset_plan.v1"
RUNTIME_DERIVED_ATR_WARMUP_BARS = 14
BACKTEST_EXECUTION_INSTRUMENT_VERSION = "backtest_execution_instrument.v1"
BACKTEST_EXECUTION_INSTRUMENT_SET_VERSION = "backtest_execution_instruments.v1"
BACKTEST_EXECUTION_CONFIG_VERSION = "backtest_execution_config.v1"

_NON_SEMANTIC_INSTRUMENT_FIELDS = frozenset({"created_at", "updated_at"})
_BACKTEST_EXECUTION_BOT_FIELDS = (
    "strategy_id",
    "strategy_variant_id",
    "strategy_variant_name",
    "atm_template_id",
    "risk_config",
    "risk",
    "wallet_config",
    "mode",
    "execution_mode",
    "execution_behavior",
    "run_type",
    "bot_env",
    "execution_semantics",
    "execution_book_tape_bundle",
)


def _semantic_json(value: Any, *, field: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                f"backtest_execution_config_invalid: {field} must be finite"
            )
        return 0.0 if value == 0.0 else value
    if isinstance(value, datetime):
        return iso_utc(value, field=field)
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
        "backtest_execution_config_invalid: "
        f"{field} has unsupported type {type(value).__name__}"
    )


def _semantic_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _semantic_json(payload, field="payload"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_backtest_execution_instrument(
    instrument_id: str,
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Detach and hash one exact instrument snapshot used by backtest execution."""

    normalized_id = str(instrument_id or "").strip()
    if not normalized_id:
        raise ValueError(
            "backtest_execution_instrument_invalid: instrument_id is required"
        )
    if not isinstance(snapshot, Mapping) or not snapshot:
        raise ValueError(
            "backtest_execution_instrument_invalid: snapshot is required"
        )
    detached = {
        str(key): _semantic_json(value, field=f"instrument.{key}")
        for key, value in sorted(snapshot.items(), key=lambda pair: str(pair[0]))
        if str(key) not in _NON_SEMANTIC_INSTRUMENT_FIELDS
    }
    snapshot_id = str(
        detached.get("id") or detached.get("instrument_id") or ""
    ).strip()
    if snapshot_id and snapshot_id != normalized_id:
        raise ValueError(
            "backtest_execution_instrument_invalid: snapshot instrument ID differs"
        )
    detached["id"] = normalized_id
    for field in ("symbol", "datasource", "exchange", "instrument_type"):
        if not str(detached.get(field) or "").strip():
            raise ValueError(
                "backtest_execution_instrument_invalid: "
                f"instrument_id={normalized_id} {field} is required"
            )
    snapshot_hash = _semantic_hash(
        {
            "schema_version": BACKTEST_EXECUTION_INSTRUMENT_VERSION,
            "instrument_id": normalized_id,
            "snapshot": detached,
        }
    )
    return {
        "schema_version": BACKTEST_EXECUTION_INSTRUMENT_VERSION,
        "instrument_id": normalized_id,
        "snapshot_hash": snapshot_hash,
        "snapshot": detached,
    }


def normalize_backtest_execution_instruments(
    values: Any,
) -> tuple[list[dict[str, Any]], str]:
    """Validate exact instrument snapshots and return their aggregate identity."""

    if not isinstance(values, list) or not values:
        raise ValueError(
            "backtest_dataset_binding_invalid: execution instruments are required"
        )
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in values:
        if not isinstance(raw, Mapping):
            raise ValueError(
                "backtest_dataset_binding_invalid: execution instrument entries "
                "must be objects"
            )
        if (
            str(raw.get("schema_version") or "").strip()
            != BACKTEST_EXECUTION_INSTRUMENT_VERSION
        ):
            raise ValueError(
                "backtest_dataset_binding_invalid: unsupported execution instrument schema"
            )
        instrument_id = str(raw.get("instrument_id") or "").strip()
        if instrument_id in seen:
            raise ValueError(
                "backtest_dataset_binding_invalid: duplicate execution instrument"
            )
        rebuilt = build_backtest_execution_instrument(
            instrument_id,
            raw.get("snapshot") if isinstance(raw.get("snapshot"), Mapping) else {},
        )
        if str(raw.get("snapshot_hash") or "").strip() != rebuilt["snapshot_hash"]:
            raise ValueError(
                "backtest_dataset_binding_invalid: execution instrument hash "
                f"disagreement instrument_id={instrument_id or '<missing>'}"
            )
        seen.add(instrument_id)
        normalized.append(rebuilt)
    normalized.sort(key=lambda row: str(row["instrument_id"]))
    aggregate_hash = _semantic_hash(
        {
            "schema_version": BACKTEST_EXECUTION_INSTRUMENT_SET_VERSION,
            "instruments": [
                {
                    "instrument_id": row["instrument_id"],
                    "snapshot_hash": row["snapshot_hash"],
                }
                for row in normalized
            ],
        }
    )
    return normalized, aggregate_hash


def build_backtest_execution_config_hash(
    *,
    bot: Mapping[str, Any],
    strategy_identity: Mapping[str, Any],
    instrument_config_hash: str,
) -> str:
    """Hash the run-effective configuration that can alter backtest semantics."""

    instrument_hash = str(instrument_config_hash or "").strip()
    if not instrument_hash:
        raise ValueError(
            "backtest_execution_config_invalid: instrument_config_hash is required"
        )
    identity_fields = (
        "strategy_id",
        "strategy_hash",
        "effective_strategy_config_hash",
        "indicator_config_hash",
        "execution_policy_hash",
    )
    identity = {field: strategy_identity.get(field) for field in identity_fields}
    if any(
        not str(identity.get(field) or "").strip()
        for field in (
            "strategy_id",
            "strategy_hash",
            "indicator_config_hash",
            "execution_policy_hash",
        )
    ):
        raise ValueError(
            "backtest_execution_config_invalid: strategy execution identity is incomplete"
        )
    bot_projection = {
        field: bot.get(field)
        for field in _BACKTEST_EXECUTION_BOT_FIELDS
        if field in bot
    }
    return _semantic_hash(
        {
            "schema_version": BACKTEST_EXECUTION_CONFIG_VERSION,
            "strategy": identity,
            "instrument_config_hash": instrument_hash,
            "bot": bot_projection,
        }
    )



def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"backtest_dataset_invalid: {field} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"backtest_dataset_invalid: {field} must be an ISO-8601 timestamp"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(value: Any, *, field: str = "timestamp") -> str:
    return (
        _utc(value, field=field)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def resolve_backtest_warmup_bars(
    indicator_requirements: Sequence[Mapping[str, Any]] = (),
    *,
    configured_bars: Any = None,
) -> int:
    """Resolve the actual declared warmup without a historical magic floor."""

    required = RUNTIME_DERIVED_ATR_WARMUP_BARS
    for row in indicator_requirements:
        raw = row.get("required_bars")
        if raw in (None, ""):
            continue
        if isinstance(raw, bool):
            raise ValueError(
                "backtest_warmup_invalid: indicator required_bars must be a positive integer"
            )
        try:
            bars = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "backtest_warmup_invalid: indicator required_bars must be a positive integer"
            ) from exc
        if bars <= 0:
            raise ValueError(
                "backtest_warmup_invalid: indicator required_bars must be a positive integer"
            )
        required = max(required, bars)

    if configured_bars in (None, ""):
        return required
    if isinstance(configured_bars, bool):
        raise ValueError("backtest_warmup_bars must be a positive integer")
    try:
        configured = int(configured_bars)
    except (TypeError, ValueError) as exc:
        raise ValueError("backtest_warmup_bars must be a positive integer") from exc
    if configured <= 0:
        raise ValueError("backtest_warmup_bars must be a positive integer")
    if configured < required:
        raise ValueError(
            "backtest_warmup_insufficient: configured warmup "
            f"{configured} is below required {required}"
        )
    return configured


def normalize_backtest_dataset_binding(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and detach one admitted dataset binding for runtime use."""

    if not isinstance(payload, Mapping):
        raise ValueError("backtest_dataset_binding_invalid: binding must be an object")
    schema_version = str(payload.get("schema_version") or "").strip()
    if schema_version != BACKTEST_DATASET_BINDING_VERSION:
        raise ValueError(
            "backtest_dataset_binding_invalid: unsupported binding schema "
            f"{schema_version or '<missing>'}"
        )
    dataset_contract = str(payload.get("dataset_contract_version") or "").strip()
    if dataset_contract != DATASET_IDENTITY_HASH_VERSION:
        raise ValueError(
            "backtest_dataset_binding_invalid: unsupported dataset contract "
            f"{dataset_contract or '<missing>'}"
        )
    dataset_id = str(payload.get("dataset_id") or "").strip()
    dataset_hash = str(payload.get("dataset_hash") or "").strip()
    if not dataset_id or not dataset_hash:
        raise ValueError(
            "backtest_dataset_binding_invalid: dataset_id and dataset_hash are required"
        )
    if dataset_id != f"mds_{dataset_hash[:32]}":
        raise ValueError(
            "backtest_dataset_binding_invalid: dataset ID does not match semantic hash"
        )
    strategy_id = str(payload.get("strategy_id") or "").strip()
    strategy_hash = str(payload.get("strategy_hash") or "").strip()
    indicator_config_hash = str(payload.get("indicator_config_hash") or "").strip()
    execution_policy_hash = str(payload.get("execution_policy_hash") or "").strip()
    execution_config_hash = str(payload.get("execution_config_hash") or "").strip()
    effective_strategy_config_hash = str(
        payload.get("effective_strategy_config_hash") or ""
    ).strip() or None
    if not all(
        (strategy_id, strategy_hash, indicator_config_hash, execution_policy_hash,
         execution_config_hash)
    ):
        raise ValueError(
            "backtest_dataset_binding_invalid: strategy_id, strategy_hash, and "
            "execution configuration hashes are required"
        )
    try:
        max_commit_seq = int(payload.get("max_commit_seq"))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "backtest_dataset_binding_invalid: max_commit_seq must be positive"
        ) from exc
    if max_commit_seq <= 0:
        raise ValueError(
            "backtest_dataset_binding_invalid: max_commit_seq must be positive"
        )
    if str(payload.get("validation_status") or "").strip().lower() != "ready":
        raise ValueError(
            "backtest_dataset_binding_invalid: validation_status must be ready"
        )
    if payload.get("provider_call_performed") is not False:
        raise ValueError(
            "backtest_dataset_binding_invalid: execution binding must be provider-free"
        )
    quality = payload.get("quality")
    if not isinstance(quality, Mapping) or str(
        quality.get("status") or ""
    ).strip().lower() not in {"ready", "ready_with_caveats"}:
        raise ValueError(
            "backtest_dataset_binding_invalid: dataset quality is not admitted"
        )

    raw_series = payload.get("series")
    if not isinstance(raw_series, list) or not raw_series:
        raise ValueError(
            "backtest_dataset_binding_invalid: at least one bound series is required"
        )
    series: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, int | None]] = set()
    for raw in raw_series:
        if not isinstance(raw, Mapping):
            raise ValueError(
                "backtest_dataset_binding_invalid: series entries must be objects"
            )
        instrument_id = str(raw.get("instrument_id") or "").strip()
        fact_type = str(raw.get("fact_type") or "").strip().lower()
        contract_version = str(raw.get("contract_version") or "").strip()
        try:
            contract = get_fact_contract(fact_type)
            contract.validate(contract_version=contract_version, timeframe_seconds=raw.get("timeframe_seconds"))
            if not contract.dataset_eligible:
                raise ValueError(
                    f"fact type is not dataset eligible: {fact_type}"
                )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "backtest_dataset_binding_invalid: unsupported fact contract "
                f"{fact_type or '<missing>'}/{contract_version or '<missing>'}"
            ) from exc
        try:
            series_id = int(raw.get("series_id"))
            row_count = int(raw.get("row_count"))
            series_commit_seq = int(raw.get("max_commit_seq"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "backtest_dataset_binding_invalid: series numeric fields are malformed"
            ) from exc
        raw_timeframe = raw.get("timeframe_seconds")
        if raw_timeframe is None:
            timeframe_seconds = None
        else:
            try:
                timeframe_seconds = int(raw_timeframe)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "backtest_dataset_binding_invalid: timeframe is malformed"
                ) from exc
        contract.validate(contract_version=contract_version, timeframe_seconds=timeframe_seconds)
        if (
            not instrument_id
            or series_id <= 0
            or row_count <= 0
            or series_commit_seq <= 0
        ):
            raise ValueError(
                "backtest_dataset_binding_invalid: series identity and counts must be positive"
            )
        key = (instrument_id, fact_type, contract_version, timeframe_seconds)
        if key in seen:
            raise ValueError(
                "backtest_dataset_binding_invalid: duplicate typed fact series"
            )
        seen.add(key)
        start = _utc(raw.get("range_start"), field="series.range_start")
        end = _utc(raw.get("range_end"), field="series.range_end")
        if end <= start:
            raise ValueError(
                "backtest_dataset_binding_invalid: series range_end must be after range_start"
            )
        hashes = {
            name: str(raw.get(name) or "").strip()
            for name in ("material_hash", "provenance_hash", "quality_hash")
        }
        if not all(hashes.values()):
            raise ValueError(
                "backtest_dataset_binding_invalid: series semantic hashes are required"
            )
        series.append(
            {
                **dict(raw),
                "series_id": series_id,
                "instrument_id": instrument_id,
                "fact_type": fact_type,
                "contract_version": contract_version,
                "timeframe_seconds": timeframe_seconds,
                "range_start": iso_utc(start),
                "range_end": iso_utc(end),
                "row_count": row_count,
                "max_commit_seq": series_commit_seq,
                **hashes,
            }
        )

    execution_instruments, calculated_instrument_hash = (
        normalize_backtest_execution_instruments(payload.get("instruments"))
    )
    instrument_config_hash = str(
        payload.get("instrument_config_hash") or ""
    ).strip()
    if instrument_config_hash != calculated_instrument_hash:
        raise ValueError(
            "backtest_dataset_binding_invalid: instrument configuration hash disagreement"
        )
    series_instrument_ids = {str(row["instrument_id"]) for row in series}
    bound_instrument_ids = {
        str(row["instrument_id"]) for row in execution_instruments
    }
    if not bound_instrument_ids.issubset(series_instrument_ids):
        raise ValueError(
            "backtest_dataset_binding_invalid: execution instruments are missing primary series"
        )

    windows: dict[str, tuple[datetime, datetime]] = {}
    normalized_windows: dict[str, dict[str, Any]] = {}
    for name, window in (
        ("evaluation_range", payload.get("evaluation_range")),
        ("warmup_range", payload.get("warmup_range")),
        ("materialization_range", payload.get("materialization_range")),
        ("decision_range", payload.get("decision_range")),
    ):
        if not isinstance(window, Mapping):
            raise ValueError(
                f"backtest_dataset_binding_invalid: {name} must be an object"
            )
        start = _utc(window.get("start"), field=f"{name}.start")
        end = _utc(window.get("end_exclusive"), field=f"{name}.end_exclusive")
        if end <= start:
            raise ValueError(
                f"backtest_dataset_binding_invalid: {name} end must be after start"
            )
        windows[name] = (start, end)
        normalized_windows[name] = {
            **dict(window),
            "start": iso_utc(start),
            "end_exclusive": iso_utc(end),
        }

    evaluation_start, evaluation_end = windows["evaluation_range"]
    warmup_start, warmup_end = windows["warmup_range"]
    materialization_start, materialization_end = windows["materialization_range"]
    decision_start, decision_end = windows["decision_range"]
    if warmup_end != evaluation_start or warmup_start >= warmup_end:
        raise ValueError(
            "backtest_dataset_binding_invalid: warmup must end at evaluation start"
        )
    if (
        materialization_start > warmup_start
        or materialization_end != evaluation_end
    ):
        raise ValueError(
            "backtest_dataset_binding_invalid: materialization must cover warmup and evaluation"
        )
    if (decision_start, decision_end) != (evaluation_start, evaluation_end):
        raise ValueError(
            "backtest_dataset_binding_invalid: decision range must equal evaluation range"
        )
    series_starts = [
        _utc(row["range_start"], field="series.range_start") for row in series
    ]
    series_ends = [
        _utc(row["range_end"], field="series.range_end") for row in series
    ]
    if min(series_starts) != materialization_start or any(
        end != materialization_end for end in series_ends
    ):
        raise ValueError(
            "backtest_dataset_binding_invalid: bound series do not match materialization range"
        )
    if max(int(row["max_commit_seq"]) for row in series) > max_commit_seq:
        raise ValueError(
            "backtest_dataset_binding_invalid: series watermark exceeds dataset watermark"
        )

    return {
        **dict(payload),
        "schema_version": BACKTEST_DATASET_BINDING_VERSION,
        "dataset_contract_version": DATASET_IDENTITY_HASH_VERSION,
        "dataset_id": dataset_id,
        "dataset_hash": dataset_hash,
        "max_commit_seq": max_commit_seq,
        "strategy_id": strategy_id,
        "strategy_hash": strategy_hash,
        "effective_strategy_config_hash": effective_strategy_config_hash,
        "indicator_config_hash": indicator_config_hash,
        "execution_policy_hash": execution_policy_hash,
        "execution_config_hash": execution_config_hash,
        "instrument_config_hash": instrument_config_hash,
        **normalized_windows,
        "instruments": execution_instruments,
        "series": sorted(
            series,
            key=lambda row: (
                str(row["instrument_id"]),
                str(row["fact_type"]),
                int(row["timeframe_seconds"] or -1),
                int(row["series_id"]),
            ),
        ),
    }


def bound_instrument_for_id(
    binding: Mapping[str, Any],
    instrument_id: str,
) -> dict[str, Any]:
    """Return the exact admitted instrument snapshot for one canonical ID."""

    normalized = normalize_backtest_dataset_binding(binding)
    normalized_id = str(instrument_id or "").strip()
    matches = [
        row
        for row in normalized["instruments"]
        if str(row["instrument_id"]) == normalized_id
    ]
    if len(matches) != 1:
        raise ValueError(
            "backtest_execution_instrument_unbound: dataset execution binding "
            f"does not contain instrument_id={normalized_id or '<missing>'}"
        )
    return dict(matches[0]["snapshot"])


def bound_instrument_for_symbol(
    binding: Mapping[str, Any],
    *,
    datasource: Any,
    exchange: Any,
    symbol: Any,
) -> dict[str, Any]:
    """Resolve a symbol only within exact admitted instrument snapshots."""

    normalized = normalize_backtest_dataset_binding(binding)
    expected = (
        str(datasource or "").strip().lower(),
        str(exchange or "").strip().lower(),
        str(symbol or "").strip().upper(),
    )
    matches = []
    for row in normalized["instruments"]:
        snapshot = row["snapshot"]
        actual = (
            str(snapshot.get("datasource") or "").strip().lower(),
            str(snapshot.get("exchange") or "").strip().lower(),
            str(snapshot.get("symbol") or "").strip().upper(),
        )
        if actual == expected:
            matches.append(snapshot)
    if len(matches) != 1:
        raise ValueError(
            "backtest_execution_instrument_unbound: dataset execution binding "
            f"does not contain exactly one instrument for {expected}"
        )
    return dict(matches[0])


def bound_series_for_request(
    binding: Mapping[str, Any],
    *,
    instrument_id: str,
    timeframe_seconds: int | None,
    start: Any,
    end: Any,
    fact_type: str = CANDLE_FACT_TYPE,
    contract_version: str = CANDLE_FACT_VERSION,
) -> dict[str, Any]:
    """Resolve one exact bound series and reject all range expansion."""

    normalized = normalize_backtest_dataset_binding(binding)
    instrument_id = str(instrument_id or "").strip()
    timeframe = int(timeframe_seconds) if timeframe_seconds is not None else None
    fact_type = str(fact_type or "").strip().lower()
    contract_version = str(contract_version or "").strip()
    requested_start = _utc(start, field="requested_start")
    requested_end = _utc(end, field="requested_end")
    matches = [
        row
        for row in normalized["series"]
        if row["instrument_id"] == instrument_id
        and row["timeframe_seconds"] == timeframe
        and row["fact_type"] == fact_type
        and row["contract_version"] == contract_version
    ]
    if len(matches) != 1:
        raise ValueError(
            "backtest_dataset_series_missing: dataset does not contain exactly one "
            f"series for instrument_id={instrument_id} fact_type={fact_type} "
            f"contract_version={contract_version} timeframe_seconds={timeframe}"
        )
    entry = dict(matches[0])
    bound_start = _utc(entry["range_start"], field="bound_start")
    bound_end = _utc(entry["range_end"], field="bound_end")
    if requested_end <= requested_start:
        raise ValueError(
            "backtest_dataset_range_invalid: requested end must be after start"
        )
    if requested_start < bound_start or requested_end > bound_end:
        raise ValueError(
            "backtest_dataset_range_expansion_forbidden: requested "
            f"[{iso_utc(requested_start)}, {iso_utc(requested_end)}) outside frozen "
            f"[{iso_utc(bound_start)}, {iso_utc(bound_end)})"
        )
    return entry


__all__ = [
    "BACKTEST_DATASET_BINDING_VERSION",
    "BACKTEST_DATASET_PLAN_VERSION",
    "BACKTEST_EXECUTION_CONFIG_VERSION",
    "BACKTEST_EXECUTION_INSTRUMENT_SET_VERSION",
    "BACKTEST_EXECUTION_INSTRUMENT_VERSION",
    "RUNTIME_DERIVED_ATR_WARMUP_BARS",
    "bound_instrument_for_id",
    "bound_instrument_for_symbol",
    "bound_series_for_request",
    "build_backtest_execution_config_hash",
    "build_backtest_execution_instrument",
    "iso_utc",
    "normalize_backtest_dataset_binding",
    "normalize_backtest_execution_instruments",
    "resolve_backtest_warmup_bars",
]
