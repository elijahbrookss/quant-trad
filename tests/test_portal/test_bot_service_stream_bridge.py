from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import bot_service


class _FakeWatchdog:
    def __init__(self) -> None:
        self.callbacks = []

    def set_orphan_callback(self, callback) -> None:
        self.callbacks.append(callback)


class _FakeComposition:
    def __init__(self, watchdog) -> None:
        self.watchdog = watchdog


def test_ensure_watchdog_stream_bridge_registers_callback_once(monkeypatch):
    watchdog = _FakeWatchdog()
    composition = _FakeComposition(watchdog)

    monkeypatch.setattr(bot_service, "_WATCHDOG_CALLBACK_SET", False)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)

    bot_service.ensure_watchdog_stream_bridge()
    bot_service.ensure_watchdog_stream_bridge()

    assert len(watchdog.callbacks) == 1
    assert callable(watchdog.callbacks[0])
