from __future__ import annotations

import copy
import hashlib
import json
import logging
import threading
import time
from datetime import timezone
from typing import Any, Deque, Dict, Mapping, Optional
from collections import deque

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine import ensure_builtin_indicator_plugins_registered
from engines.indicator_engine.overlay_runtime import project_and_normalize_entries
from engines.indicator_engine.plugins import plugin_registry
from signals.overlays.schema import normalize_overlays

logger = logging.getLogger(__name__)
_PROJECTION_CACHE_LOCK = threading.Lock()
_PROJECTION_CHECKPOINTS: Dict[str, Dict[str, Any]] = {}
_PROJECTION_CHECKPOINT_ORDER: Deque[str] = deque()
_PROJECTION_CHECKPOINT_MAX = 24
_RECENT_PROJECTION_PERF: Deque[Dict[str, Any]] = deque(maxlen=80)


def build_runtime_state_overlay(
    *,
    indicator_id: str,
    meta: Mapping[str, Any],
    df: Any,
    symbol: str,
    timeframe: str,
) -> Dict[str, Any]:
    indicator_type = str(meta.get("type") or "").strip().lower()
    if not indicator_type:
        raise RuntimeError(f"overlay_runtime_state_projection_type_missing: indicator_id={indicator_id}")

    ensure_builtin_indicator_plugins_registered()
    try:
        plugin = plugin_registry().resolve(indicator_type)
    except Exception as exc:
        raise RuntimeError(
            f"overlay_runtime_state_projection_plugin_missing: indicator_id={indicator_id} indicator_type={indicator_type}"
        ) from exc

    if plugin.overlay_projector is None:
        raise RuntimeError(
            f"overlay_runtime_state_projection_projector_missing: indicator_id={indicator_id} indicator_type={indicator_type}"
        )

    started = time.perf_counter()
    logger.info(
        "event=overlay_runtime_state_projection_start indicator_id=%s indicator_type=%s symbol=%s timeframe=%s",
        indicator_id,
        indicator_type,
        symbol,
        timeframe,
    )

    candles = _candles_from_frame(df)
    if not candles:
        raise LookupError("No overlays computed for given window")

    engine = plugin.engine_factory(meta)
    window_context = {
        "symbol": symbol,
        "timeframe": timeframe,
        "indicator_id": indicator_id,
    }
    state = engine.initialize(window_context)
    projection_state: Dict[str, Any] = {"seq": 0, "revision": -1, "entries": {}}
    cache_key = _projection_cache_key(
        indicator_id=indicator_id,
        indicator_type=indicator_type,
        symbol=symbol,
        timeframe=timeframe,
        meta=meta,
    )
    reused_bars = 0
    cache_hit = False
    checkpoint = _load_projection_checkpoint(cache_key)
    if checkpoint is not None:
        checkpoint_count = int(checkpoint.get("candles_processed") or 0)
        if _checkpoint_matches(candles=candles, checkpoint=checkpoint):
            engine_state = checkpoint.get("engine_state")
            checkpoint_projection_state = checkpoint.get("projection_state")
            if isinstance(engine_state, Mapping) and isinstance(checkpoint_projection_state, Mapping):
                state = copy.deepcopy(dict(engine_state))
                projection_state = copy.deepcopy(dict(checkpoint_projection_state))
                reused_bars = checkpoint_count
                cache_hit = checkpoint_count > 0
    perf_totals = {
        "projector_ms": 0.0,
        "delta_ms": 0.0,
        "normalize_ms": 0.0,
        "fingerprint_ms": 0.0,
        "normalize_cache_hits": 0.0,
        "normalize_cache_misses": 0.0,
        "entries_total": 0.0,
        "entries_changed": 0.0,
        "ops_count": 0.0,
        "projection_total_ms": 0.0,
    }
    for candle in candles[reused_bars:]:
        engine.apply_bar(state, candle)
        snapshot = engine.snapshot(state)
        projection = project_and_normalize_entries(
            indicator_type=indicator_type,
            snapshot=snapshot,
            projection_state=projection_state,
            entry_projector=plugin.overlay_projector,
            invalid_projection_error=f"overlay_runtime_state_projection_invalid: indicator_type={indicator_type}",
            normalize_failed_error=lambda entry_key: (
                f"overlay_runtime_state_projection_normalize_failed: "
                f"indicator_type={indicator_type} entry_key={entry_key}"
            ),
            normalize_entries=False,
            compute_delta=False,
        )
        if not projection.delta.ops:
            projection_state["seq"] = projection.delta.seq
            projection_state["revision"] = snapshot.revision
            projection_state["_normalize_cache"] = projection.normalize_cache
            for metric_key in perf_totals:
                perf_totals[metric_key] += float(projection.perf.get(metric_key, 0.0) or 0.0)
            continue
        projection_state["seq"] = projection.delta.seq
        projection_state["revision"] = snapshot.revision
        projection_state["entries"] = projection.entries
        projection_state["_normalize_cache"] = projection.normalize_cache
        for metric_key in perf_totals:
            perf_totals[metric_key] += float(projection.perf.get(metric_key, 0.0) or 0.0)
    _store_projection_checkpoint(
        cache_key,
        {
            "candles_processed": len(candles),
            "first_epoch": int(candles[0].time.timestamp()) if candles else None,
            "last_epoch": int(candles[-1].time.timestamp()) if candles else None,
            "engine_state": copy.deepcopy(state),
            "projection_state": copy.deepcopy(projection_state),
            "updated_at": time.time(),
        },
    )
    projection_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
    candles_per_second = (len(candles) / (projection_ms / 1000.0)) if projection_ms > 0 else 0.0
    logger.info(
        "event=overlay_runtime_state_projection_done indicator_id=%s indicator_type=%s candles=%s reused_bars=%s cache_hit=%s duration_ms=%.3f candles_per_second=%.3f projector_ms=%.3f delta_ms=%.3f normalize_ms=%.3f fingerprint_ms=%.3f normalize_cache_hits=%s normalize_cache_misses=%s entries_total=%s entries_changed=%s ops_count=%s",
        indicator_id,
        indicator_type,
        len(candles),
        reused_bars,
        cache_hit,
        projection_ms,
        candles_per_second,
        perf_totals["projector_ms"],
        perf_totals["delta_ms"],
        perf_totals["normalize_ms"],
        perf_totals["fingerprint_ms"],
        int(perf_totals["normalize_cache_hits"]),
        int(perf_totals["normalize_cache_misses"]),
        int(perf_totals["entries_total"]),
        int(perf_totals["entries_changed"]),
        int(perf_totals["ops_count"]),
    )
    _record_recent_projection_perf(
        {
            "recorded_at": time.time(),
            "indicator_id": indicator_id,
            "indicator_type": indicator_type,
            "symbol": symbol,
            "timeframe": timeframe,
            "candles": len(candles),
            "reused_bars": reused_bars,
            "cache_hit": cache_hit,
            "duration_ms": projection_ms,
            **perf_totals,
            "candles_per_second": candles_per_second,
        }
    )
    entries = projection_state.get("entries")
    if not isinstance(entries, Mapping) or not entries:
        raise LookupError("No overlays computed for given window")

    normalized_count = 0
    for entry in entries.values():
        if not isinstance(entry, Mapping):
            continue
        normalized = normalize_overlays(indicator_type, [dict(entry)])
        if not normalized:
            continue
        normalized_count += 1
        payload = normalized[0].get("payload") if isinstance(normalized[0], Mapping) else None
        logger.info(
            "event=overlay_runtime_state_projected indicator_id=%s indicator_type=%s entries=%s profiles=%s boxes=%s markers=%s segments=%s polylines=%s payload_keys=%s",
            indicator_id,
            indicator_type,
            len(entries),
            len(payload.get("profiles", []) if isinstance(payload, Mapping) and isinstance(payload.get("profiles"), list) else []),
            len(payload.get("boxes", []) if isinstance(payload, Mapping) and isinstance(payload.get("boxes"), list) else []),
            len(payload.get("markers", []) if isinstance(payload, Mapping) and isinstance(payload.get("markers"), list) else []),
            len(payload.get("segments", []) if isinstance(payload, Mapping) and isinstance(payload.get("segments"), list) else []),
            len(payload.get("polylines", []) if isinstance(payload, Mapping) and isinstance(payload.get("polylines"), list) else []),
            list((payload or {}).keys()) if isinstance(payload, Mapping) else [],
        )
        return dict(normalized[0])
    if normalized_count == 0:
        raise LookupError("No overlays computed for given window")
    raise RuntimeError(
        f"overlay_runtime_state_projection_normalized_empty: indicator_id={indicator_id} indicator_type={indicator_type}"
    )


def _candles_from_frame(df: Any) -> list[Candle]:
    candles: list[Candle] = []
    for timestamp, row in df.iterrows():
        dt = timestamp.to_pydatetime() if hasattr(timestamp, "to_pydatetime") else timestamp
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        candles.append(
            Candle(
                time=dt,
                open=float(row.get("open")),
                high=float(row.get("high")),
                low=float(row.get("low")),
                close=float(row.get("close")),
                volume=float(row.get("volume")) if row.get("volume") is not None else None,
            )
        )
    return candles


def list_recent_runtime_projection_perf(*, limit: int = 40) -> list[Dict[str, Any]]:
    with _PROJECTION_CACHE_LOCK:
        items = list(_RECENT_PROJECTION_PERF)
    if limit <= 0:
        return items
    return items[-int(limit):]


def _record_recent_projection_perf(record: Dict[str, Any]) -> None:
    with _PROJECTION_CACHE_LOCK:
        _RECENT_PROJECTION_PERF.append(dict(record))


def _projection_cache_key(
    *,
    indicator_id: str,
    indicator_type: str,
    symbol: str,
    timeframe: str,
    meta: Mapping[str, Any],
) -> str:
    params = _stable_projection_params(meta.get("params") if isinstance(meta, Mapping) else None)
    serialized = json.dumps(
        {
            "indicator_id": indicator_id,
            "indicator_type": indicator_type,
            "symbol": symbol,
            "timeframe": timeframe,
            "params": params,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()


def _stable_projection_params(raw_params: Any) -> Dict[str, Any]:
    if not isinstance(raw_params, Mapping):
        return {}
    volatile_keys = {
        "bot_id",
        "strategy_id",
        "symbol",
        "datasource",
        "exchange",
        "instrument_id",
        "start",
        "end",
    }
    stable: Dict[str, Any] = {}
    for key, value in raw_params.items():
        key_text = str(key)
        if key_text in volatile_keys:
            continue
        if key_text.startswith("_"):
            continue
        stable[key_text] = value
    return stable


def _checkpoint_matches(*, candles: list[Candle], checkpoint: Mapping[str, Any]) -> bool:
    if not candles:
        return False
    checkpoint_count = int(checkpoint.get("candles_processed") or 0)
    if checkpoint_count <= 0 or checkpoint_count > len(candles):
        return False
    first_epoch = checkpoint.get("first_epoch")
    last_epoch = checkpoint.get("last_epoch")
    if first_epoch is None or last_epoch is None:
        return False
    if int(candles[0].time.timestamp()) != int(first_epoch):
        return False
    if int(candles[checkpoint_count - 1].time.timestamp()) != int(last_epoch):
        return False
    return True


def _load_projection_checkpoint(cache_key: str) -> Optional[Dict[str, Any]]:
    with _PROJECTION_CACHE_LOCK:
        checkpoint = _PROJECTION_CHECKPOINTS.get(cache_key)
        if checkpoint is None:
            return None
        return copy.deepcopy(checkpoint)


def _store_projection_checkpoint(cache_key: str, checkpoint: Dict[str, Any]) -> None:
    with _PROJECTION_CACHE_LOCK:
        if cache_key not in _PROJECTION_CHECKPOINTS:
            _PROJECTION_CHECKPOINT_ORDER.append(cache_key)
        _PROJECTION_CHECKPOINTS[cache_key] = dict(checkpoint)
        while len(_PROJECTION_CHECKPOINT_ORDER) > _PROJECTION_CHECKPOINT_MAX:
            oldest = _PROJECTION_CHECKPOINT_ORDER.popleft()
            if oldest:
                _PROJECTION_CHECKPOINTS.pop(oldest, None)
