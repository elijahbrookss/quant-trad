from __future__ import annotations

def test_persistence_buffer_flushes_by_count():
    calls = {"entries": 0, "events": 0}

    from engines.bot_runtime.runtime.components.persistence_buffer import TradePersistenceBuffer

    buffer = TradePersistenceBuffer(
        max_batch_size=2,
        flush_interval_s=100,
        time_fn=lambda: 0.0,
        record_trade=lambda _payload: calls.__setitem__("entries", calls["entries"] + 1),
        record_trade_event=lambda _payload: calls.__setitem__("events", calls["events"] + 1),
    )
    buffer.record_trade_entry({"trade_id": "t1"})
    assert calls == {"entries": 0, "events": 0}

    buffer.record_trade_event({"id": "e1"})
    assert calls == {"entries": 1, "events": 1}


def test_persistence_buffer_flushes_on_close_event():
    calls = {"entries": 0, "events": 0}

    from engines.bot_runtime.runtime.components.persistence_buffer import TradePersistenceBuffer

    buffer = TradePersistenceBuffer(
        max_batch_size=10,
        flush_interval_s=100,
        time_fn=lambda: 0.0,
        record_trade=lambda _payload: calls.__setitem__("entries", calls["entries"] + 1),
        record_trade_event=lambda _payload: calls.__setitem__("events", calls["events"] + 1),
    )
    buffer.record_trade_event({"id": "close-event"}, event_type="close")
    assert calls == {"entries": 0, "events": 1}
