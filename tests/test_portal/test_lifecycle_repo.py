from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import startup_lifecycle
from portal.backend.service.storage.repos import lifecycle


def _db_available() -> Any:
    @contextmanager
    def _unexpected_session():
        raise AssertionError("legacy lifecycle DB fallback should not be used in this test")
        yield

    return SimpleNamespace(available=True, session=_unexpected_session)


def test_record_bot_run_lifecycle_checkpoint_persists_canonical_rows_and_returns_allocated_seq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _record(rows, *, context=None):
        captured["rows"] = [dict(row) for row in rows]
        captured["context"] = dict(context or {})
        return len(rows)

    def _sync(*, payload, seq, replace_metadata):
        captured["synced_payload"] = dict(payload)
        captured["synced_seq"] = int(seq)
        captured["replace_metadata"] = bool(replace_metadata)
        return {
            "bot_id": payload["bot_id"],
            "run_id": payload["run_id"],
            "phase": payload["phase"],
            "status": payload["status"],
            "owner": payload["owner"],
            "message": payload["message"],
            "metadata": dict(payload.get("metadata") or {}),
            "failure": dict(payload.get("failure") or {}),
            "checkpoint_at": payload["checkpoint_at"],
            "updated_at": payload["updated_at"],
        }

    monkeypatch.setattr(lifecycle, "db", _db_available())
    monkeypatch.setattr(lifecycle, "_allocate_next_canonical_seq", lambda _run_id: 5)
    monkeypatch.setattr(lifecycle, "record_bot_runtime_events_batch", _record)
    monkeypatch.setattr(lifecycle, "_sync_legacy_lifecycle_tables", _sync)

    result = lifecycle.record_bot_run_lifecycle_checkpoint(
        startup_lifecycle.lifecycle_checkpoint_payload(
            bot_id="bot-1",
            run_id="run-1",
            phase=startup_lifecycle.BotLifecyclePhase.CONTAINER_BOOTING.value,
            owner=startup_lifecycle.LifecycleOwner.CONTAINER.value,
            message="Container booting.",
        )
    )

    assert result["seq"] == 5
    assert result["live"] is False
    assert captured["synced_seq"] == 5
    assert captured["context"]["pipeline_stage"] == "botlens_canonical_lifecycle_append"
    assert captured["context"]["message_kind"] == "botlens_lifecycle_event"
    assert captured["context"]["source_reason"] == "producer"
    assert [row["payload"]["event_name"] for row in captured["rows"]] == ["RUN_PHASE_REPORTED"]


def test_record_bot_run_lifecycle_checkpoint_rejects_run_ready_without_prior_canonical_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lifecycle, "db", _db_available())
    monkeypatch.setattr(lifecycle, "_allocate_next_canonical_seq", lambda _run_id: 7)
    monkeypatch.setattr(lifecycle, "_latest_canonical_lifecycle_row", lambda _run_id: None)
    monkeypatch.setattr(
        lifecycle,
        "record_bot_runtime_events_batch",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("canonical persistence must not run")),
    )

    with pytest.raises(RuntimeError, match="requires prior durable startup truth before RUN_READY"):
        lifecycle.record_bot_run_lifecycle_checkpoint(
            startup_lifecycle.lifecycle_checkpoint_payload(
                bot_id="bot-1",
                run_id="run-1",
                phase=startup_lifecycle.BotLifecyclePhase.LIVE.value,
                owner=startup_lifecycle.LifecycleOwner.RUNTIME.value,
                message="All planned series emitted first runtime snapshot; bot is live.",
            )
        )


def test_get_bot_run_lifecycle_prefers_canonical_rows_over_legacy_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical = {"run_id": "run-1", "phase": "live", "status": "running", "seq": 8}
    legacy = {"run_id": "run-1", "phase": "stopped", "status": "stopped", "seq": 3}

    monkeypatch.setattr(lifecycle, "_latest_canonical_lifecycle_row", lambda _run_id: dict(canonical))
    monkeypatch.setattr(lifecycle, "_latest_legacy_lifecycle_row", lambda _run_id: dict(legacy))

    assert lifecycle.get_bot_run_lifecycle("run-1") == canonical


def test_list_bot_run_lifecycle_events_prefers_canonical_rows_over_legacy_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical = [{"run_id": "run-1", "phase": "container_booting", "status": "starting", "seq": 1}]
    legacy = [{"run_id": "run-1", "phase": "stopped", "status": "stopped", "seq": 9}]

    monkeypatch.setattr(lifecycle, "_list_canonical_lifecycle_rows", lambda _run_id: list(canonical))
    monkeypatch.setattr(lifecycle, "_list_legacy_lifecycle_rows", lambda _run_id: list(legacy))

    assert lifecycle.list_bot_run_lifecycle_events("run-1") == canonical


def test_lifecycle_checkpoint_payload_rejects_status_phase_mismatch() -> None:
    with pytest.raises(ValueError, match="status must match phase"):
        startup_lifecycle.lifecycle_checkpoint_payload(
            bot_id="bot-1",
            run_id="run-1",
            phase="completed",
            status="running",
            owner="runtime",
            message="done",
        )
