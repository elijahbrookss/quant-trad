from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots.runtime_composition import (
    RuntimeComposition,
    RuntimeMode,
    build_runtime_composition,
    clear_runtime_compositions_for_tests,
    get_runtime_composition,
)


class _FakeStream:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any]]] = []

    def broadcast(self, event: str, payload: dict[str, Any]) -> None:
        self.messages.append((event, payload))


class _FakeConfig:
    def list_bots(self):
        return []


class _FakeStorage:
    def __init__(self) -> None:
        self.saved = []

    def upsert_bot(self, payload):
        self.saved.append(dict(payload))

    def list_bot_runs(self, *, bot_id=None, limit=None):
        return []

    def get_latest_bot_run_view_state(self, *, bot_id: str, run_id=None, series_key=None):
        return None


class _FakeWatchdog:
    runner_id = "runner-test"

    def set_orphan_callback(self, _callback):
        return None

    def register_bot(self, _bot_id: str):
        return None

    def unregister_bot(self, _bot_id: str):
        return None

    def scan_stale_heartbeats(self):
        return []

    def verify_container_ownership(self):
        return []

    def status(self):
        return {"runner_id": self.runner_id}


class _FakeRunner:
    def start_bot(self, *, bot):
        return f"container-{bot['id']}"

    def stop_bot(self, *, bot_id):
        return None


def test_runtime_composition_builds_explicit_collaborators():
    composition = build_runtime_composition(
        mode=RuntimeMode.BACKTEST,
        stream_manager=_FakeStream(),
        config_service=_FakeConfig(),
        storage=_FakeStorage(),
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: _FakeRunner(),
    )

    assert isinstance(composition, RuntimeComposition)
    assert composition.mode == RuntimeMode.BACKTEST
    assert composition.runtime_control_service is not None
    assert composition.storage is not None


def test_runtime_composition_supports_mode_variants_with_same_contract():
    common = dict(
        stream_manager=_FakeStream(),
        config_service=_FakeConfig(),
        storage=_FakeStorage(),
        watchdog=_FakeWatchdog(),
        runner_factory=lambda: _FakeRunner(),
    )
    paper = build_runtime_composition(mode=RuntimeMode.PAPER, **common)
    live = build_runtime_composition(mode=RuntimeMode.LIVE, **common)

    assert paper.mode == RuntimeMode.PAPER
    assert live.mode == RuntimeMode.LIVE
    assert type(paper.runtime_control_service) is type(live.runtime_control_service)


def test_runtime_composition_caches_per_mode_singletons(monkeypatch):
    clear_runtime_compositions_for_tests()
    monkeypatch.setenv("BOT_RUNTIME_MODE", "backtest")

    first = get_runtime_composition(mode=RuntimeMode.BACKTEST)
    second = get_runtime_composition(mode=RuntimeMode.BACKTEST)

    assert first is second
