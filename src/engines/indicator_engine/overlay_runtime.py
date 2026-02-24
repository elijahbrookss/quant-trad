"""Shared overlay projection runtime helpers.

This module centralizes projection + normalization flow so QuantLab overlay
requests and bot runtime walk-forward updates execute the same core mechanics.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable, Dict, Mapping

from signals.overlays.schema import normalize_overlays

from .contracts import IndicatorStateSnapshot, OverlayProjectionInput, ProjectionDelta
from .overlay_projection import OverlayEntryProjector, fingerprint_overlay_entry, project_overlay_delta


NormalizedEntryAdapter = Callable[[str, Dict[str, Any]], Dict[str, Any]]
NormalizeErrorBuilder = Callable[[str], str]


@dataclass(frozen=True)
class OverlayProjectionResult:
    delta: ProjectionDelta
    entries: Dict[str, Dict[str, Any]]
    normalize_cache: Dict[str, Dict[str, Any]]
    perf: Dict[str, float]


def project_and_normalize_entries(
    *,
    indicator_type: str,
    snapshot: IndicatorStateSnapshot,
    projection_state: Mapping[str, Any],
    entry_projector: OverlayEntryProjector,
    invalid_projection_error: str,
    normalize_failed_error: NormalizeErrorBuilder,
    entry_adapter: NormalizedEntryAdapter | None = None,
    normalize_entries: bool = True,
    compute_delta: bool = True,
) -> OverlayProjectionResult:
    started = time.perf_counter()
    projector_ms = 0.0
    delta_ms = 0.0
    normalize_ms = 0.0
    fingerprint_ms = 0.0
    normalize_cache_hits = 0.0
    normalize_cache_misses = 0.0
    entries_total = 0.0
    entries_changed = 0.0
    ops_count = 0.0
    projection_input = OverlayProjectionInput(
        snapshot=snapshot,
        previous_projection_state=projection_state,
    )
    normalize_cache_state = projection_state.get("_normalize_cache")
    normalize_cache: Dict[str, Dict[str, Any]] = {}
    if isinstance(normalize_cache_state, Mapping):
        for key, value in normalize_cache_state.items():
            if not isinstance(value, Mapping):
                continue
            raw_fingerprint = value.get("fingerprint")
            raw_entry = value.get("entry")
            if not isinstance(raw_fingerprint, str) or not isinstance(raw_entry, Mapping):
                continue
            normalize_cache[str(key)] = {
                "fingerprint": raw_fingerprint,
                "entry": dict(raw_entry),
            }
    previous_revision = int(projection_state.get("revision") or -1)
    if snapshot.revision == previous_revision:
        delta_started = time.perf_counter()
        delta = project_overlay_delta(
            projection_input=projection_input,
            entry_projector=lambda _input: {},
        )
        delta_ms = max((time.perf_counter() - delta_started) * 1000.0, 0.0)
        return OverlayProjectionResult(
            delta=delta,
            entries={},
            normalize_cache=normalize_cache,
            perf={
                "projection_total_ms": max((time.perf_counter() - started) * 1000.0, 0.0),
                "projector_ms": projector_ms,
                "delta_ms": delta_ms,
                "normalize_ms": normalize_ms,
                "fingerprint_ms": fingerprint_ms,
                "normalize_cache_hits": normalize_cache_hits,
                "normalize_cache_misses": normalize_cache_misses,
                "entries_total": entries_total,
                "entries_changed": entries_changed,
                "ops_count": ops_count,
            },
        )

    projector_started = time.perf_counter()
    projected = entry_projector(projection_input)
    projector_ms = max((time.perf_counter() - projector_started) * 1000.0, 0.0)
    if not isinstance(projected, Mapping):
        raise RuntimeError(invalid_projection_error)

    if compute_delta:
        delta_started = time.perf_counter()
        delta = project_overlay_delta(
            projection_input=projection_input,
            entry_projector=lambda _input: projected,
        )
        delta_ms = max((time.perf_counter() - delta_started) * 1000.0, 0.0)
    else:
        previous_seq = int(projection_state.get("seq") or 0)
        has_entries = bool(projected)
        delta = ProjectionDelta(
            seq=previous_seq + 1,
            base_seq=previous_seq,
            ops=[{"op": "reset", "entries": list(projected.values())}] if has_entries else [],
            authoritative_snapshot=(previous_seq == 0),
        )
        delta_ms = 0.0
    ops_count = float(len(delta.ops))
    if not delta.ops:
        return OverlayProjectionResult(
            delta=delta,
            entries={},
            normalize_cache=normalize_cache,
            perf={
                "projection_total_ms": max((time.perf_counter() - started) * 1000.0, 0.0),
                "projector_ms": projector_ms,
                "delta_ms": delta_ms,
                "normalize_ms": normalize_ms,
                "fingerprint_ms": fingerprint_ms,
                "normalize_cache_hits": normalize_cache_hits,
                "normalize_cache_misses": normalize_cache_misses,
                "entries_total": entries_total,
                "entries_changed": entries_changed,
                "ops_count": ops_count,
            },
        )

    upsert_keys: set[str] = set()
    remove_keys: set[str] = set()
    reset_all = False
    for operation in delta.ops:
        if not isinstance(operation, Mapping):
            continue
        op_name = str(operation.get("op") or "").strip().lower()
        if op_name == "reset":
            reset_all = True
            break
        if op_name == "upsert":
            op_key = str(operation.get("key") or "").strip()
            if op_key:
                upsert_keys.add(op_key)
        elif op_name == "remove":
            op_key = str(operation.get("key") or "").strip()
            if op_key:
                remove_keys.add(op_key)

    normalized_entries: Dict[str, Dict[str, Any]]
    if reset_all:
        normalized_entries = {}
    else:
        previous_entries = projection_state.get("entries")
        normalized_entries = {}
        if isinstance(previous_entries, Mapping):
            for key, entry in previous_entries.items():
                if str(key) in remove_keys:
                    continue
                if isinstance(entry, Mapping):
                    normalized_entries[str(key)] = dict(entry)
    entries_total = float(len(projected))

    normalize_started = time.perf_counter()
    target_upserts = set(projected.keys()) if reset_all else upsert_keys
    entries_changed = float(len(target_upserts))
    for entry_key in target_upserts:
        entry_value = projected.get(entry_key)
        if not isinstance(entry_value, Mapping):
            continue
        if not normalize_entries:
            normalized_entries[str(entry_key)] = dict(entry_value)
            continue
        fingerprint_started = time.perf_counter()
        fingerprint = fingerprint_overlay_entry(entry_value)
        fingerprint_ms += max((time.perf_counter() - fingerprint_started) * 1000.0, 0.0)

        cache_entry = normalize_cache.get(str(entry_key))
        if (
            isinstance(cache_entry, Mapping)
            and str(cache_entry.get("fingerprint") or "") == fingerprint
            and isinstance(cache_entry.get("entry"), Mapping)
        ):
            normalized_entry = dict(cache_entry.get("entry") or {})
            normalize_cache_hits += 1.0
        else:
            normalized = normalize_overlays(indicator_type, [dict(entry_value)])
            if not normalized:
                raise RuntimeError(normalize_failed_error(str(entry_key)))
            normalized_entry = dict(normalized[0])
            normalize_cache_misses += 1.0
        if entry_adapter is not None:
            normalized_entry = dict(entry_adapter(str(entry_key), normalized_entry))
        normalized_entries[str(entry_key)] = normalized_entry
        normalize_cache[str(entry_key)] = {
            "fingerprint": fingerprint,
            "entry": dict(normalized_entry),
        }
    normalize_ms = max((time.perf_counter() - normalize_started) * 1000.0, 0.0)

    return OverlayProjectionResult(
        delta=delta,
        entries=normalized_entries,
        normalize_cache=normalize_cache,
        perf={
            "projection_total_ms": max((time.perf_counter() - started) * 1000.0, 0.0),
            "projector_ms": projector_ms,
            "delta_ms": delta_ms,
            "normalize_ms": normalize_ms,
            "fingerprint_ms": fingerprint_ms,
            "normalize_cache_hits": normalize_cache_hits,
            "normalize_cache_misses": normalize_cache_misses,
            "entries_total": entries_total,
            "entries_changed": entries_changed,
            "ops_count": ops_count,
        },
    )


__all__ = [
    "OverlayProjectionResult",
    "project_and_normalize_entries",
]
