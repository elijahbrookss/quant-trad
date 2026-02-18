from datetime import datetime, timedelta, timezone

import pytest


class _DummyFrame:
    def __init__(self, timestamps):
        self._index = tuple(timestamps)
        self.empty = len(self._index) == 0

    def copy(self):
        return _DummyFrame(self._index)

    @property
    def index(self):
        return self._index

    def __len__(self):
        return len(self._index)


def _build_frame():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return _DummyFrame([start + timedelta(hours=i) for i in range(5)])


def _configure_runtime(monkeypatch, *, to_lightweight_payload):
    from portal.backend.service.indicators import indicator_service as svc

    class _OverlayIndicator:
        NAME = "market_profile"

        @classmethod
        def from_context(cls, provider, ctx, **kwargs):  # noqa: D401 - test helper
            return cls()

        def to_lightweight(self, df):  # noqa: D401 - test helper serializer
            return dict(to_lightweight_payload)

    inst_id = "overlay-visibility-ind"
    record = {
        "id": inst_id,
        "name": "Overlay Visibility",
        "type": _OverlayIndicator.NAME,
        "params": {
            "symbol": "ES",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-01T04:00:00Z",
            "interval": "1h",
            "days_back": 180,
            "use_merged_value_areas": True,
            "merge_threshold": 0.6,
            "min_merge_sessions": 5,
        },
        "color": "#4f46e5",
        "datasource": "ALPACA",
        "exchange": None,
        "enabled": True,
        "updated_at": "2024-01-01T00:00:00Z",
    }
    entry = svc.IndicatorCacheEntry(
        meta=svc._build_meta_from_record(record),
        instance=_OverlayIndicator(),
        updated_at=record["updated_at"],
    )
    frame = _build_frame()

    class _Provider:
        def get_ohlcv(self, ctx):  # noqa: D401 - test helper
            return frame.copy()

    monkeypatch.setattr(svc, "_INSTANCE_CACHE", {inst_id: entry})
    monkeypatch.setattr(svc, "_load_indicator_record", lambda req_id: record if req_id == inst_id else None)
    monkeypatch.setattr(svc, "_resolve_data_provider", lambda *args, **kwargs: _Provider())
    monkeypatch.setitem(svc._INDICATOR_MAP, _OverlayIndicator.NAME, _OverlayIndicator)
    return svc, inst_id


def test_overlay_visibility_trims_box_to_request_end(monkeypatch):
    start_epoch = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
    payload = {
        "boxes": [
            {
                "x1": start_epoch,
                "x2": start_epoch + 4 * 3600,
                "y1": 100.0,
                "y2": 110.0,
            }
        ],
        "markers": [],
        "price_lines": [],
        "segments": [],
        "polylines": [],
        "bubbles": [],
    }
    svc, inst_id = _configure_runtime(monkeypatch, to_lightweight_payload=payload)

    response = svc.overlays_for_instance(
        inst_id=inst_id,
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T01:00:00Z",
        interval="1h",
        symbol="ES",
    )

    box = response["payload"]["boxes"][0]
    assert box["x1"] == start_epoch
    assert box["x2"] == int(datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc).timestamp())


def test_overlay_visibility_filters_future_known_at(monkeypatch):
    start_epoch = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
    payload = {
        "boxes": [
            {
                "x1": start_epoch,
                "x2": start_epoch + 2 * 3600,
                "y1": 100.0,
                "y2": 110.0,
                "known_at": start_epoch + 3 * 3600,
            }
        ],
        "markers": [],
        "price_lines": [],
        "segments": [],
        "polylines": [],
        "bubbles": [],
    }
    svc, inst_id = _configure_runtime(monkeypatch, to_lightweight_payload=payload)

    with pytest.raises(LookupError):
        svc.overlays_for_instance(
            inst_id=inst_id,
            start="2024-01-01T00:00:00Z",
            end="2024-01-01T01:00:00Z",
            interval="1h",
            symbol="ES",
        )
