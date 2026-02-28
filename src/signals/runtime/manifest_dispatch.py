"""Manifest-driven runtime signal dispatch and normalization."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.plugin_registry import IndicatorPluginManifest, SignalCatalogEntry


def emit_manifest_signals(
    *,
    manifest: IndicatorPluginManifest,
    snapshot_payload: Mapping[str, Any],
    candle: Candle,
    previous_candle: Candle | None,
) -> Dict[str, Any]:
    if manifest.signal_emitter is None:
        return {"signals": []}
    emitted = manifest.signal_emitter(snapshot_payload, candle, previous_candle)
    if not isinstance(emitted, Mapping):
        raise RuntimeError(
            "indicator_signal_emit_invalid: emitter result must be mapping "
            f"| indicator_type={manifest.indicator_type}"
        )
    raw_signals = emitted.get("signals")
    diagnostics = emitted.get("diagnostics")
    if raw_signals is None:
        raw_signals = []
    if not isinstance(raw_signals, Sequence) or isinstance(raw_signals, (str, bytes)):
        raise RuntimeError(
            "indicator_signal_emit_invalid: emitter signals must be sequence "
            f"| indicator_type={manifest.indicator_type}"
        )
    if not manifest.signal_rules:
        if len(raw_signals) > 0:
            raise RuntimeError(
                "indicator_signal_emit_invalid: signal_rules required when emitter yields signals "
                f"| indicator_type={manifest.indicator_type} count={len(raw_signals)}"
            )
        return {"signals": [], "diagnostics": diagnostics if isinstance(diagnostics, Mapping) else {}}

    by_id: Dict[str, SignalCatalogEntry] = {}
    alias_to_id: Dict[str, str] = {}
    ordered_ids: list[str] = []
    for rule in manifest.signal_rules:
        rule_id = str(rule.id).strip().lower()
        by_id[rule_id] = rule
        ordered_ids.append(rule_id)
        for alias in rule.aliases:
            alias_to_id[str(alias).strip().lower()] = rule_id

    grouped: Dict[str, list[Dict[str, Any]]] = {rule_id: [] for rule_id in ordered_ids}
    for index, item in enumerate(raw_signals):
        if not isinstance(item, Mapping):
            raise RuntimeError(
                "indicator_signal_emit_invalid: signal entry must be mapping "
                f"| indicator_type={manifest.indicator_type} index={index}"
            )
        signal = dict(item)
        nested_metadata = signal.get("metadata")
        metadata = dict(nested_metadata) if isinstance(nested_metadata, Mapping) else {}
        candidate_rule_id = str(
            signal.get("rule_id")
            or metadata.get("rule_id")
            or signal.get("pattern_id")
            or metadata.get("pattern_id")
            or ""
        ).strip().lower()
        if not candidate_rule_id:
            raise RuntimeError(
                "indicator_signal_emit_invalid: signal rule_id required "
                f"| indicator_type={manifest.indicator_type} index={index}"
            )
        canonical_rule_id = alias_to_id.get(candidate_rule_id, candidate_rule_id)
        rule = by_id.get(canonical_rule_id)
        if rule is None:
            raise RuntimeError(
                "indicator_signal_emit_invalid: unknown signal rule_id "
                f"| indicator_type={manifest.indicator_type} rule_id={candidate_rule_id}"
            )
        declared_type = str(rule.signal_type or "").strip().lower()
        emitted_type = str(
            signal.get("type") or signal.get("signal_type") or metadata.get("signal_type") or ""
        ).strip().lower()
        if emitted_type and emitted_type != declared_type:
            raise RuntimeError(
                "indicator_signal_emit_invalid: signal type mismatch "
                f"| indicator_type={manifest.indicator_type} rule_id={canonical_rule_id} "
                f"declared={declared_type} emitted={emitted_type}"
            )
        signal["type"] = declared_type
        signal["signal_type"] = declared_type
        signal["rule_id"] = canonical_rule_id
        signal.setdefault("pattern_id", canonical_rule_id)
        metadata.setdefault("rule_id", canonical_rule_id)
        metadata.setdefault("pattern_id", signal.get("pattern_id"))
        metadata.setdefault("signal_type", declared_type)
        signal["metadata"] = metadata
        grouped[canonical_rule_id].append(signal)

    ordered_signals: list[Dict[str, Any]] = []
    for rule_id in ordered_ids:
        ordered_signals.extend(grouped.get(rule_id) or [])
    return {
        "signals": ordered_signals,
        "diagnostics": diagnostics if isinstance(diagnostics, Mapping) else {},
    }
