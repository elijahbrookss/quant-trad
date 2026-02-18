"""Signal evaluation and matching utilities for strategy rules."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Sequence


def _utcnow() -> datetime:
    return datetime.utcnow()


def _iso_to_epoch_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None

    try:
        return int(dt.timestamp())
    except (OverflowError, OSError, ValueError):
        return None


def _extract_signal_epoch(signal: Optional[Mapping[str, Any]]) -> Optional[int]:
    if not isinstance(signal, Mapping):
        return None

    candidates: List[Any] = []
    if "signal_time" in signal:
        candidates.append(signal.get("signal_time"))
    if "time" in signal:
        candidates.append(signal.get("time"))
    if "timestamp" in signal:
        candidates.append(signal.get("timestamp"))

    metadata = signal.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "signal_time",
            "time",
            "timestamp",
            "bar_time",
            "bar_timestamp",
            "candle_time",
            "event_time",
            "retest_time",
        ):
            if key in metadata:
                candidates.append(metadata.get(key))

    for value in candidates:
        epoch = _iso_to_epoch_seconds(value)
        if epoch is not None:
            return epoch

    return None


def _normalise_direction(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip().lower()
    if text in {"long", "buy", "bull", "bullish", "above", "up"}:
        return "long"
    if text in {"short", "sell", "bear", "bearish", "below", "down"}:
        return "short"
    return None


def _infer_signal_direction(signal: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not isinstance(signal, Mapping):
        return None

    def _iter_sources() -> Iterable[Mapping[str, Any]]:
        yield signal
        metadata = signal.get("metadata")
        if isinstance(metadata, Mapping):
            yield metadata

    for source in _iter_sources():
        direct = _normalise_direction(source.get("direction"))
        if direct:
            return direct

        bias_hint = _normalise_direction(source.get("bias"))
        if bias_hint:
            return bias_hint

        bias_label = _normalise_direction(source.get("bias_label"))
        if bias_label:
            return bias_label

        trade_direction = _normalise_direction(source.get("trade_direction"))
        if trade_direction:
            return trade_direction

        pointer_direction = _normalise_direction(source.get("pointer_direction"))
        if pointer_direction:
            return pointer_direction

        active_side = _normalise_direction(source.get("active_side"))
        if active_side:
            return active_side

        side_hint = _normalise_direction(source.get("side"))
        if side_hint:
            return side_hint

        breakout_direction = _normalise_direction(source.get("breakout_direction"))
        if breakout_direction:
            return breakout_direction

        role_value = str(source.get("retest_role", "")).strip().lower()
        if role_value == "support":
            return "long"
        if role_value == "resistance":
            return "short"

        level_kind = str(
            source.get("level_type")
            or source.get("level_kind")
            or source.get("level_role")
            or ""
        ).strip().lower()
        if level_kind in {"vah", "value_area_high", "resistance"}:
            return "short"
        if level_kind in {"val", "value_area_low", "support"}:
            return "long"

    rule_id = str(signal.get("pattern_id") or "").lower()
    if not rule_id:
        metadata = signal.get("metadata")
        if isinstance(metadata, Mapping):
            rule_id = str(metadata.get("pattern_id") or "").lower()

    if rule_id.endswith("breakout"):
        for source in _iter_sources():
            candidate = _normalise_direction(source.get("breakout_direction"))
            if candidate:
                return candidate

    if rule_id.endswith("retest"):
        for source in _iter_sources():
            role_value = str(source.get("retest_role", "")).strip().lower()
            if role_value == "support":
                return "long"
            if role_value == "resistance":
                return "short"

    return None


def _promote_signal_metadata(signal: MutableMapping[str, Any]) -> None:
    metadata = signal.get("metadata")
    if not isinstance(metadata, Mapping):
        return

    preferred_keys = (
        "rule_id",
        "pattern_id",
        "signal_id",
        "pattern",
        "id",
        "direction",
        "bias",
        "breakout_direction",
        "pointer_direction",
        "retest_role",
    )
    for key in preferred_keys:
        if signal.get(key) in (None, "", []):
            value = metadata.get(key)
            if value not in (None, "", []):
                signal[key] = value

    alias_keys = ("aliases", "rule_aliases", "pattern_aliases")
    alias_values: list[str] = []

    def _ingest(value: Any) -> None:
        if isinstance(value, str):
            normalised = value.strip()
            if normalised and normalised not in alias_values:
                alias_values.append(normalised)
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
            for item in value:
                _ingest(item)

    for key in alias_keys:
        _ingest(signal.get(key))
    for key in alias_keys:
        _ingest(metadata.get(key))

    if alias_values:
        signal["rule_aliases"] = alias_values


def _ensure_signal_direction(signal: Optional[dict[str, Any]]) -> Optional[str]:
    if not isinstance(signal, dict):
        return None

    _promote_signal_metadata(signal)

    direction = _infer_signal_direction(signal)
    if not direction:
        return None

    existing = _normalise_direction(signal.get("direction"))
    if existing is None:
        signal["direction"] = direction

    metadata = signal.get("metadata")
    if isinstance(metadata, MutableMapping):
        meta_direction = _normalise_direction(metadata.get("direction"))
        if meta_direction is None:
            metadata["direction"] = direction

    return direction


def _collect_rule_identifiers(signal: Mapping[str, Any]) -> List[str]:
    identifiers: List[str] = []

    def _append(value: Any) -> None:
        if isinstance(value, str):
            normalised = value.strip().lower()
            if normalised and normalised not in identifiers:
                identifiers.append(normalised)
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
            for item in value:
                _append(item)

    if isinstance(signal, MutableMapping):
        _promote_signal_metadata(signal)

    sources: List[Mapping[str, Any]] = [signal]
    metadata = signal.get("metadata")
    if isinstance(metadata, Mapping):
        sources.append(metadata)

    keys = ("rule_id", "pattern_id", "signal_id", "pattern", "id")
    alias_keys = ("aliases", "rule_aliases", "pattern_aliases")

    for source in sources:
        for key in keys:
            _append(source.get(key))
        for alias_key in alias_keys:
            _append(source.get(alias_key))

    return identifiers


def _summarise_signal_population(signals: Iterable[Mapping[str, Any]]) -> dict[str, Counter]:
    type_counter: Counter[str] = Counter()
    rule_counter: Counter[str] = Counter()
    direction_counter: Counter[str] = Counter()

    for candidate in signals:
        if not isinstance(candidate, Mapping):
            continue

        signal_type = str(candidate.get("type", "")).strip().lower()
        if signal_type:
            type_counter[signal_type] += 1

        for identifier in _collect_rule_identifiers(candidate):
            rule_counter[identifier] += 1

        direction = _infer_signal_direction(dict(candidate))
        if direction:
            direction_counter[direction] += 1

    return {
        "types": type_counter,
        "rules": rule_counter,
        "directions": direction_counter,
    }


def _format_counter(counter: Counter[str]) -> str:
    if not counter:
        return "-"
    parts = [f"{key}:{counter[key]}" for key in sorted(counter)]
    return ", ".join(parts)


def _normalise_match_mode(value: Any) -> str:
    if isinstance(value, str) and value.strip().lower() == "any":
        return "any"
    return "all"


def _normalise_action(value: Any) -> str:
    action_value = str(value).strip().lower()
    if action_value not in {"buy", "sell"}:
        raise ValueError("Action must be 'buy' or 'sell'")
    return action_value


def _evaluate_condition(
    condition: Any,
    indicator_payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    info: dict[str, Any] = {
        "indicator_id": condition.indicator_id,
        "signal_type": condition.signal_type,
        "rule_id": condition.rule_id,
        "direction": condition.direction,
        "matched": False,
        "signal": None,
        "signals": [],
        "reason": None,
    }

    payload = indicator_payloads.get(condition.indicator_id)
    if payload is None:
        info["reason"] = "No signals for indicator"
        return info

    signals = payload.get("signals") if isinstance(payload, Mapping) else None
    if not isinstance(signals, Sequence):
        info["reason"] = "No signals provided by indicator"
        return info

    desired_rule = str(condition.rule_id or "").strip().lower() or None
    desired_direction = _normalise_direction(condition.direction)
    total_signals = len(signals)
    type_candidates: list[Mapping[str, Any]] = []
    rule_candidates: list[Mapping[str, Any]] = []
    direction_candidates: list[Mapping[str, Any]] = []
    matched_candidates: list[Mapping[str, Any]] = []
    observed_rules: set[str] = set()
    observed_directions: set[str] = set()

    for idx, candidate in enumerate(signals):
        if not isinstance(candidate, Mapping):
            continue

        # Log first 3 signals for debugging
        if idx < 3:
            signal_type = str(candidate.get("type", "")).strip().lower()
            signal_keys = list(candidate.keys()) if isinstance(candidate, Mapping) else []
            metadata = candidate.get("metadata")
            metadata_keys = list(metadata.keys()) if isinstance(metadata, Mapping) else None

            import logging
            logger = logging.getLogger("StrategyEvaluator")
            logger.info(
                "strategy_signal_debug | idx=%d | is_mapping=%s | signal_type=%s | condition_type=%s | signal_keys=%s | has_metadata=%s | metadata_keys=%s",
                idx,
                isinstance(candidate, Mapping),
                signal_type,
                condition.signal_type.strip().lower(),
                signal_keys,
                metadata is not None,
                metadata_keys,
            )

        signal_type = str(candidate.get("type", "")).strip().lower()
        if signal_type != condition.signal_type.strip().lower():
            continue

        _promote_signal_metadata(candidate)
        identifiers = _collect_rule_identifiers(candidate)
        if identifiers:
            observed_rules.update(identifiers)

        direction = _ensure_signal_direction(candidate)
        if direction:
            observed_directions.add(direction)

        type_candidates.append(candidate)

        if desired_rule:
            candidate_rules = {rule.lower() for rule in identifiers}
            if desired_rule not in candidate_rules:
                continue
        rule_candidates.append(candidate)

        if desired_direction:
            if direction != desired_direction:
                continue
            direction_candidates.append(candidate)
        else:
            direction_candidates.append(candidate)

        matched_candidates.append(candidate)

    matched_candidates.sort(key=lambda entry: (_extract_signal_epoch(entry) or 0))

    info["observed_rules"] = sorted(observed_rules)
    info["observed_directions"] = sorted(observed_directions)
    info["stats"] = {
        "signals": total_signals,
        "type_matches": len(type_candidates),
        "rule_matches": len(rule_candidates),
        "direction_matches": len(direction_candidates) if desired_direction else len(rule_candidates),
        "final_matches": len(matched_candidates),
    }

    if matched_candidates:
        terminal_signal = matched_candidates[-1]
        info["matched"] = True
        info["signal"] = terminal_signal
        info["signals"] = matched_candidates
        info["direction_detected"] = _infer_signal_direction(terminal_signal)
        info["reason"] = None
        return info

    if not type_candidates:
        info["reason"] = "No matching signals (type mismatch)"
    elif desired_rule and not rule_candidates:
        info["reason"] = "No matching signals (rule mismatch)"
    elif desired_direction and not direction_candidates:
        info["reason"] = "No matching signals (direction mismatch)"
    else:
        info["reason"] = "No matching signals"
    return info


__all__ = [
    "_ensure_signal_direction",
    "_collect_rule_identifiers",
    "_evaluate_condition",
    "_extract_signal_epoch",
    "_format_counter",
    "_infer_signal_direction",
    "_normalise_action",
    "_normalise_direction",
    "_normalise_match_mode",
    "_promote_signal_metadata",
    "_summarise_signal_population",
]
