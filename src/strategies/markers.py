"""Chart marker and overlay builders for strategy signal results."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

from . import evaluator


def _extract_signal_price(signal: Any) -> Any:
    if not isinstance(signal, dict):
        return None
    metadata = signal.get("metadata")
    candidates = []
    if isinstance(metadata, dict):
        candidates.extend(
            metadata.get(key)
            for key in (
                "price",
                "close",
                "retest_close",
                "trigger_price",
                "level_price",
                "poc",
            )
        )
    candidates.append(signal.get("price"))
    for value in candidates:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if not (number is None or number != number):
            return number
    return None


def _build_markers_for_results(
    results: Sequence[Mapping[str, Any]],
    *,
    action: str,
) -> List[Dict[str, Any]]:
    color = "#10b981" if action == "buy" else "#f87171"
    shape = "arrowUp" if action == "buy" else "arrowDown"
    markers: List[Dict[str, Any]] = []

    for res in results:
        rule_name = str(res.get("rule_name") or res.get("rule_id") or action.title())
        seen_keys = set()
        signals = list(res.get("signals") or [])
        signals.sort(key=lambda entry: (evaluator._extract_signal_epoch(entry) or 0))
        for signal in signals:
            if not isinstance(signal, Mapping):
                continue
            epoch = evaluator._extract_signal_epoch(signal)
            price = _extract_signal_price(signal)
            if epoch is None or price is None:
                continue
            direction = evaluator._infer_signal_direction(signal) or ("long" if action == "buy" else "short")
            label = f"{rule_name} ({direction})" if direction else rule_name
            dedupe_key = (epoch, price, res.get("rule_id"), direction)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            markers.append(
                {
                    "time": epoch,
                    "price": price,
                    "color": color,
                    "shape": shape,
                    "text": label,
                    "size": 1,
                    "subtype": "strategy_signal",
                    "direction": direction,
                    "rule_id": res.get("rule_id"),
                    "position": "belowBar" if action == "buy" else "aboveBar",
                }
            )

    return markers


def build_chart_markers(buy_signals: Sequence[Mapping[str, Any]], sell_signals: Sequence[Mapping[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Return the marker payload expected by chart consumers."""

    return {
        "buy": _build_markers_for_results(buy_signals, action="buy"),
        "sell": _build_markers_for_results(sell_signals, action="sell"),
    }


__all__ = ["_build_markers_for_results", "build_chart_markers"]
