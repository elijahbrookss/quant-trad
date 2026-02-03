from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
import logging
from signals.overlays.transformers import apply_overlay_transform

logger = logging.getLogger(__name__)

class ChartStateBuilder:
    """Compose visible candles and overlays for the bot runtime chart payload."""

    def __init__(
        self,
        *,
        normalise_epoch_fn: Callable[[Any], Optional[int]],
        log_sequence_fn: Callable[[str, Optional[str], Sequence[Any]], None],
        strategy_key_fn: Callable[[Any], str],
    ) -> None:
        self._normalise_epoch = normalise_epoch_fn
        self._log_sequence = log_sequence_fn
        self._strategy_key = strategy_key_fn

    def visible_candles(
        self,
        primary: Optional[Any],
        status: Optional[str],
        bar_index: int,
        intrabar_manager: Any,
    ) -> List[Dict[str, Any]]:
        candles: List[Dict[str, Any]] = []
        if not primary or not getattr(primary, "candles", None):
            return candles
        normalized = (status or "").lower()
        total = len(primary.candles)
        if normalized in {"idle", "initialising", "completed", "stopped"}:
            visible = total
        else:
            visible = min(max(bar_index, 0), total)
        visible = max(1, visible)
        slice_candidates = list(primary.candles[:visible])
        ordered = sorted(slice_candidates, key=lambda candle: candle.time.timestamp())
        candles = [candle.to_dict() for candle in ordered]
        key = self._strategy_key(primary)
        snapshot = intrabar_manager.snapshots.get(key)
        if snapshot and candles:
            candles[-1] = intrabar_manager.merge_snapshot_payload(candles[-1], snapshot)
        self._log_sequence("visible_payload", getattr(primary, "strategy_id", None), candles)
        return candles

    def visible_overlays(
        self,
        overlays: Iterable[Any],
        status: Optional[str],
        current_epoch: Optional[int],
    ) -> List[Dict[str, Any]]:
        overlays_list = list(overlays or [])
        if not overlays_list:
            return []
        normalized = (status or "").lower()
        if current_epoch is None:
            if normalized in {"idle", "initialising"}:
                return []
            return overlays_list
        visible: List[Dict[str, Any]] = []
        for overlay in overlays_list:
            transformed = apply_overlay_transform(overlay, current_epoch)
            overlay_type = str(overlay.get("type") if isinstance(overlay, Mapping) else "") or ""
            if transformed is None:
                if overlay_type in {"regime_overlay", "regime_markers"}:
                    logger.debug(
                        "regime_overlay_skipped_transform | epoch=%s | type=%s",
                        current_epoch,
                        overlay_type,
                    )
                continue
            trimmed = self._trim_overlay_to_epoch(transformed, current_epoch)
            if trimmed and self._overlay_is_ready(trimmed, current_epoch):
                if overlay_type in {"regime_overlay", "regime_markers"}:
                    payload = trimmed.get("payload") if isinstance(trimmed, Mapping) else {}
                    boxes = len(payload.get("boxes", []) if isinstance(payload, Mapping) else [])
                    segments = len(payload.get("segments", []) if isinstance(payload, Mapping) else [])
                    markers = len(payload.get("markers", []) if isinstance(payload, Mapping) else [])
                    instrument_id = overlay.get("instrument_id") if isinstance(overlay, Mapping) else None
                    symbol = overlay.get("symbol") if isinstance(overlay, Mapping) else None
                    known_at = None
                    if isinstance(payload, Mapping):
                        known_at = payload.get("known_at") or payload.get("knownAt")
                    logger.debug(
                        "regime_overlay_visible | epoch=%s | type=%s | boxes=%s | segments=%s | markers=%s | instrument_id=%s | symbol=%s | known_at=%s",
                        current_epoch,
                        overlay_type,
                        boxes,
                        segments,
                        markers,
                        instrument_id,
                        symbol,
                        known_at,
                    )
                visible.append(trimmed)
            else:
                if overlay_type in {"regime_overlay", "regime_markers"}:
                    payload = overlay.get("payload") if isinstance(overlay, Mapping) else {}
                    instrument_id = overlay.get("instrument_id") if isinstance(overlay, Mapping) else None
                    symbol = overlay.get("symbol") if isinstance(overlay, Mapping) else None
                    known_at = None
                    if isinstance(payload, Mapping):
                        known_at = payload.get("known_at") or payload.get("knownAt")
                    logger.debug(
                        "regime_overlay_trimmed_out | epoch=%s | type=%s | known_at=%s | payload_keys=%s | instrument_id=%s | symbol=%s",
                        current_epoch,
                        overlay_type,
                        known_at,
                        list((payload or {}).keys()) if isinstance(payload, Mapping) else [],
                        instrument_id,
                        symbol,
                    )
        # Summarize per-call visibility for regime overlays to spot look-ahead issues
        regime_visible = [ov for ov in visible if isinstance(ov, Mapping) and ov.get("type") in {"regime_overlay", "regime_markers"}]
        if regime_visible:
            box_list = []
            seg_list = []
            mark_total = 0
            inst_ids = set()
            symbols = set()
            known_ats = []
            for ov in regime_visible:
                if isinstance(ov, Mapping):
                    inst_id = ov.get("instrument_id")
                    symbol = ov.get("symbol")
                    if inst_id:
                        inst_ids.add(inst_id)
                    if symbol:
                        symbols.add(symbol)
                    payload = ov.get("payload") if isinstance(ov.get("payload"), Mapping) else {}
                    if isinstance(payload, Mapping):
                        boxes = payload.get("boxes") or []
                        segments = payload.get("segments") or []
                        markers = payload.get("markers") or []
                        box_list.extend(boxes)
                        seg_list.extend(segments)
                        mark_total += len(markers or [])
                        known = payload.get("known_at") or payload.get("knownAt")
                        if known is not None:
                            known_ats.append(known)

            def _extents(entries, a_key, b_key):
                times = []
                for item in entries or []:
                    if not isinstance(item, Mapping):
                        continue
                    a = item.get(a_key)
                    b = item.get(b_key)
                    if isinstance(a, (int, float)):
                        times.append(a)
                    if isinstance(b, (int, float)):
                        times.append(b)
                if not times:
                    return (None, None)
                return (min(times), max(times))

            box_span = _extents(box_list, "x1", "x2")
            seg_span = _extents(seg_list, "x1", "x2")
            logger.debug(
                "regime_overlay_visible_summary | epoch=%s | overlays=%s | boxes=%s | segments=%s | markers=%s | instruments=%s | symbols=%s | box_span=%s | segment_span=%s | known_at=%s",
                current_epoch,
                len(regime_visible),
                len(box_list),
                len(seg_list),
                mark_total,
                sorted(inst_ids) if inst_ids else [],
                sorted(symbols) if symbols else [],
                box_span,
                seg_span,
                known_ats if known_ats else None,
            )
        return visible

    def chart_state(
        self,
        candles: List[Dict[str, Any]],
        trades: List[Dict[str, Any]],
        stats: Mapping[str, Any],
        overlays: List[Dict[str, Any]],
        logs: List[Dict[str, Any]],
        decision_events: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        return {
            "candles": candles,
            "trades": trades,
            "stats": stats,
            "overlays": overlays,
            "logs": logs,
            "decisions": decision_events or [],
        }

    def _overlay_is_ready(self, overlay: Mapping[str, Any], current_epoch: int) -> bool:
        if not isinstance(overlay, Mapping):
            return False
        known_at = self._first_epoch_from(overlay, ("known_at", "knownAt"))
        if known_at is not None and current_epoch < known_at:
            return False
        payload = overlay.get("payload")
        if isinstance(payload, Mapping):
            payload_known_at = self._first_epoch_from(payload, ("known_at", "knownAt"))
            if payload_known_at is not None and current_epoch < payload_known_at:
                return False
        return True

    def _trim_overlay_to_epoch(self, overlay: Any, current_epoch: int) -> Optional[Dict[str, Any]]:
        if not isinstance(overlay, Mapping):
            return None
        payload = overlay.get("payload")
        if not isinstance(payload, Mapping):
            return dict(overlay)
        trimmed_payload, has_content = self._trim_overlay_payload(payload, current_epoch)
        if not has_content:
            return None
        if trimmed_payload is payload:
            return dict(overlay)
        trimmed = dict(overlay)
        trimmed["payload"] = trimmed_payload
        return trimmed

    def _trim_overlay_payload(self, payload: Mapping[str, Any], current_epoch: int) -> Tuple[Mapping[str, Any], bool]:
        if not isinstance(payload, Mapping):
            return payload, True
        trimmed: Dict[str, Any] = dict(payload)
        changed = False

        def process_list(key: str, filter_fn: Callable[[Any], Optional[Any]]) -> None:
            nonlocal changed
            entries = payload.get(key)
            if not isinstance(entries, list):
                return
            new_entries: List[Any] = []
            entry_changed = False
            for entry in entries:
                filtered = filter_fn(entry)
                if filtered is None:
                    entry_changed = True
                    continue
                new_entries.append(filtered)
                if filtered is not entry:
                    entry_changed = True
            if entry_changed or len(new_entries) != len(entries):
                trimmed[key] = new_entries
                changed = True
            else:
                trimmed[key] = entries

        process_list("price_lines", lambda entry: self._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("markers", lambda entry: self._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("touchPoints", lambda entry: self._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("touch_points", lambda entry: self._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("bubbles", lambda entry: self._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("segments", lambda entry: self._trim_segment_entry(entry, current_epoch))
        process_list("polylines", lambda entry: self._trim_polyline_entry(entry, current_epoch))
        process_list("boxes", lambda entry: self._trim_box_entry(entry, current_epoch))

        has_content = self._payload_has_content(trimmed)
        return (trimmed if changed else payload, has_content)

    def _payload_has_content(self, payload: Mapping[str, Any]) -> bool:
        if not isinstance(payload, Mapping):
            return False
        list_keys = {
            "price_lines",
            "markers",
            "touchPoints",
            "touch_points",
            "boxes",
            "segments",
            "polylines",
            "bubbles",
        }
        for key in list_keys:
            entries = payload.get(key)
            if isinstance(entries, list) and entries:
                return True
        for key, value in payload.items():
            if key in list_keys:
                continue
            if isinstance(value, list) and value:
                return True
            if isinstance(value, Mapping) and value:
                return True
            if isinstance(value, (int, float)) and value != 0:
                return True
            if isinstance(value, str) and value.strip():
                return True
        return False

    def _trim_time_entry(self, entry: Any, current_epoch: int, keys: Tuple[str, ...]) -> Optional[Any]:
        if not isinstance(entry, Mapping):
            return None
        if not self._entry_known_at_visible(entry, current_epoch):
            return None
        epoch = self._first_epoch_from(entry, keys)
        if epoch is not None and epoch > current_epoch:
            return None
        return entry

    def _trim_box_entry(self, entry: Any, current_epoch: int) -> Optional[Any]:
        if not isinstance(entry, Mapping):
            return None
        if not self._entry_known_at_visible(entry, current_epoch):
            return None
        start_epoch = self._first_epoch_from(entry, ("start", "start_date", "startDate", "x1"))
        # Filter boxes that haven't started yet
        if start_epoch is not None and start_epoch > current_epoch:
            return None

        # For boxes extending past current time, trim the end to current_epoch (like segments)
        end_epoch = self._first_epoch_from(entry, ("end", "end_date", "endDate"))
        extend_flag = bool(entry.get("extend")) if "extend" in entry else False
        if end_epoch is None and not extend_flag:
            end_epoch = self._first_epoch_from(entry, ("x2",))

        if end_epoch is not None and end_epoch > current_epoch:
            # Trim the box end to current epoch instead of filtering it out
            trimmed = dict(entry)
            if "x2" in trimmed:
                trimmed["x2"] = current_epoch
            if "end" in trimmed:
                trimmed["end"] = current_epoch
            if "end_date" in trimmed:
                trimmed["end_date"] = current_epoch
            if "endDate" in trimmed:
                trimmed["endDate"] = current_epoch
            return trimmed

        return entry

    def _trim_segment_entry(self, entry: Any, current_epoch: int) -> Optional[Any]:
        if not isinstance(entry, Mapping):
            return None
        if not self._entry_known_at_visible(entry, current_epoch):
            return None
        start_epoch = self._first_epoch_from(entry, ("x1", "start", "start_date", "startDate"))
        if start_epoch is not None and start_epoch > current_epoch:
            return None
        end_epoch = self._first_epoch_from(entry, ("x2", "end", "end_date", "endDate"))
        if end_epoch is not None and end_epoch > current_epoch:
            trimmed = dict(entry)
            trimmed["x2"] = current_epoch
            return trimmed
        return entry

    def _trim_polyline_entry(self, entry: Any, current_epoch: int) -> Optional[Any]:
        if not isinstance(entry, Mapping):
            return entry
        if not self._entry_known_at_visible(entry, current_epoch):
            return None
        points = entry.get("points")
        if not isinstance(points, list):
            return entry
        new_points: List[Any] = []
        changed = False
        for point in points:
            if not isinstance(point, Mapping):
                continue
            if not self._entry_known_at_visible(point, current_epoch):
                changed = True
                continue
            epoch = self._normalise_epoch(point.get("time"))
            if epoch is not None and epoch > current_epoch:
                changed = True
                continue
            new_points.append(point)
        if not new_points:
            return None
        if changed or len(new_points) != len(points):
            trimmed = dict(entry)
            trimmed["points"] = new_points
            return trimmed
        return entry

    def _first_epoch_from(self, entry: Mapping[str, Any], keys: Tuple[str, ...]) -> Optional[int]:
        for key in keys:
            if key not in entry:
                continue
            epoch = self._normalise_epoch(entry.get(key))
            if epoch is not None:
                return epoch
        return None

    def _entry_known_at_visible(self, entry: Mapping[str, Any], current_epoch: int) -> bool:
        known_at = self._first_epoch_from(entry, ("known_at", "knownAt"))
        if known_at is None:
            return True
        return known_at <= current_epoch
