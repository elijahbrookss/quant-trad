from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence, Set


def enabled_signal_output_names_from_meta(meta: Mapping[str, Any]) -> Set[str]:
    enabled: Set[str] = set()
    for output in meta.get("typed_outputs") or []:
        if not isinstance(output, Mapping):
            continue
        if str(output.get("type") or "").strip() != "signal":
            continue
        output_name = str(output.get("name") or "").strip()
        if not output_name:
            continue
        if output.get("enabled", True) is False:
            continue
        enabled.add(output_name)
    return enabled


def normalise_enabled_event_keys(config: Mapping[str, Any]) -> Set[str]:
    enabled = config.get("enabled_event_keys")
    if enabled is None:
        return set()
    if isinstance(enabled, (str, bytes)):
        candidates: Sequence[Any] = [enabled]
    elif isinstance(enabled, Sequence):
        candidates = list(enabled)
    else:
        candidates = []
    return {
        str(item).strip().lower()
        for item in candidates
        if str(item).strip()
    }


def filter_signal_payload(
    payload: Mapping[str, Any],
    *,
    enabled_output_names: Set[str],
    enabled_event_keys: Set[str],
) -> Dict[str, Any]:
    filtered = dict(payload)
    filtered.pop("signals", None)
    filtered.pop("overlays", None)
    machine_payload = payload.get("machine")
    raw_signals = machine_payload.get("signals") if isinstance(machine_payload, Mapping) else None
    if not isinstance(raw_signals, list):
        raise RuntimeError("indicator_signal_payload_invalid: machine.signals is required")
    retained_signals: List[Dict[str, Any]] = []
    retained_signal_ids: Set[str] = set()
    for signal in raw_signals:
        if not isinstance(signal, Mapping):
            continue
        output_name = str(signal.get("output_name") or "").strip()
        event_key = str(signal.get("event_key") or "").strip().lower()
        if output_name not in enabled_output_names:
            continue
        if enabled_event_keys and event_key not in enabled_event_keys:
            continue
        copied = dict(signal)
        retained_signals.append(copied)
        signal_id = str(copied.get("signal_id") or "").strip()
        if signal_id:
            retained_signal_ids.add(signal_id)

    ui_payload = payload.get("ui")
    raw_overlays = ui_payload.get("overlays") if isinstance(ui_payload, Mapping) else None
    if not isinstance(raw_overlays, list):
        raise RuntimeError("indicator_signal_payload_invalid: ui.overlays is required")
    retained_overlays: List[Dict[str, Any]] = []
    for overlay in raw_overlays:
        if not isinstance(overlay, Mapping):
            continue
        if str(overlay.get("source") or "").strip() != "signal":
            retained_overlays.append(dict(overlay))
            continue
        overlay_name = str(overlay.get("overlay_name") or "").strip()
        if overlay_name not in enabled_output_names:
            continue
        overlay_payload = dict(overlay.get("payload") or {})
        raw_bubbles = overlay_payload.get("bubbles")
        if not isinstance(raw_bubbles, list):
            continue
        retained_bubbles = [
            dict(bubble)
            for bubble in raw_bubbles
            if isinstance(bubble, Mapping)
            and str(bubble.get("signal_id") or "").strip()
            and str(bubble.get("signal_id") or "").strip() in retained_signal_ids
        ]
        if not retained_bubbles:
            continue
        copied_overlay = dict(overlay)
        copied_overlay["payload"] = {
            **overlay_payload,
            "bubbles": retained_bubbles,
        }
        retained_overlays.append(copied_overlay)

    runtime_invariants = filtered.get("runtime_invariants")
    if isinstance(runtime_invariants, Mapping):
        runtime_invariants = {
            **dict(runtime_invariants),
            "signals_count": len(retained_signals),
            "signal_overlay_count": len(retained_overlays),
        }
        filtered["runtime_invariants"] = runtime_invariants

    filtered["machine"] = {
        "signals": retained_signals,
    }
    filtered["ui"] = {
        "overlays": retained_overlays,
    }
    return filtered


__all__ = [
    "enabled_signal_output_names_from_meta",
    "filter_signal_payload",
    "normalise_enabled_event_keys",
]
