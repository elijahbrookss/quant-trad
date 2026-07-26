from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import startup_lifecycle
from portal.backend.service.storage.repos import lifecycle


def _db_available() -> Any:
    @contextmanager
    def _unexpected_session():
        raise AssertionError("unexpected lifecycle database session")
        yield

    return SimpleNamespace(available=True, session=_unexpected_session)


def test_record_bot_run_lifecycle_checkpoint_persists_canonical_rows_and_updates_run_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    transaction_session = object()
    latest = {
        "event_id": "canonical-event",
        "bot_id": "bot-1",
        "run_id": "run-1",
        "seq": 5,
        "run_seq": 5,
        "source_seq": 1,
        "phase": "container_booting",
        "status": "starting",
        "owner": "container",
        "message": "Container booting.",
        "metadata": {},
        "failure": {},
        "checkpoint_at": "2026-07-24T12:00:00Z",
        "created_at": "2026-07-24T12:00:00Z",
        "live": False,
    }

    def _record(rows, *, context=None, transactional_projection=None):
        captured["rows"] = [dict(row) for row in rows]
        captured["context"] = dict(context or {})
        assert transactional_projection is not None
        transactional_projection(transaction_session)
        return len(rows)

    def _latest(_run_id, *, session=None):
        assert session is transaction_session
        return dict(latest)

    def _project(session, payload):
        assert session is transaction_session
        captured["summary_payload"] = dict(payload)
        return {"run_id": payload["run_id"], "status": payload["status"]}

    monkeypatch.setattr(lifecycle, "db", _db_available())
    monkeypatch.setattr(lifecycle, "record_bot_runtime_events_batch", _record)
    monkeypatch.setattr(
        lifecycle,
        "_validated_latest_lifecycle_in_session",
        lambda session, run_id: _latest(run_id, session=session),
    )
    monkeypatch.setattr(lifecycle, "_project_bot_run_summary_in_session", _project)

    result = lifecycle.record_bot_run_lifecycle_checkpoint(
        startup_lifecycle.lifecycle_checkpoint_payload(
            bot_id="bot-1",
            run_id="run-1",
            phase=startup_lifecycle.BotLifecyclePhase.CONTAINER_BOOTING.value,
            owner=startup_lifecycle.LifecycleOwner.CONTAINER.value,
            message="Container booting.",
            checkpoint_at="2026-07-24T12:00:00Z",
        )
    )

    assert result == latest
    assert captured["summary_payload"] == latest
    assert captured["context"]["pipeline_stage"] == "botlens_canonical_lifecycle_append"
    assert captured["context"]["message_kind"] == "botlens_lifecycle_event"
    assert captured["context"]["source_reason"] == "producer"
    assert captured["rows"][0]["seq"] == 1
    assert [row["payload"]["event_name"] for row in captured["rows"]] == ["RUN_PHASE_REPORTED"]


def test_record_bot_run_lifecycle_checkpoint_rejects_run_ready_without_prior_canonical_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lifecycle, "db", _db_available())
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


def test_canonical_lifecycle_row_uses_runtime_run_seq_as_visible_seq() -> None:
    checkpoint = datetime(2026, 7, 24, 12, 0, 0)
    row = {
        "id": 11,
        "event_id": "event-1",
        "bot_id": "bot-1",
        "run_id": "run-1",
        "seq": 2,
        "run_seq": 9,
        "event_time": checkpoint,
        "created_at": checkpoint,
        "payload": {
            "context": {
                "bot_id": "bot-1",
                "run_id": "run-1",
                "phase": "live",
                "status": "running",
                "component": "runtime",
                "message": "Runtime ready.",
                "metadata": {"feed": "paper"},
                "failure": {"recoverable": True},
                "live": True,
            }
        },
    }

    result = lifecycle._canonical_lifecycle_row_from_runtime_row(row)

    assert result["event_id"] == "event-1"
    assert result["bot_id"] == "bot-1"
    assert result["run_id"] == "run-1"
    assert result["seq"] == 9
    assert result["run_seq"] == 9
    assert result["source_seq"] == 2
    assert result["phase"] == "live"
    assert result["status"] == "running"
    assert result["owner"] == "runtime"
    assert result["message"] == "Runtime ready."
    assert result["metadata"] == {"feed": "paper"}
    assert result["failure"] == {"recoverable": True}
    assert result["live"] is True
    assert result["checkpoint_at"] == "2026-07-24T12:00:00Z"
    assert result["created_at"] == "2026-07-24T12:00:00Z"


def test_lifecycle_summary_projection_failure_propagates_from_event_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction_session = object()
    latest = {
        "event_id": "canonical-event",
        "bot_id": "bot-1",
        "run_id": "run-1",
        "phase": "container_booting",
        "status": "starting",
        "checkpoint_at": "2026-07-24T12:00:00Z",
    }

    def _record(_rows, *, context=None, transactional_projection=None):
        assert context is not None
        assert transactional_projection is not None
        transactional_projection(transaction_session)
        return 1

    monkeypatch.setattr(lifecycle, "db", _db_available())
    monkeypatch.setattr(lifecycle, "record_bot_runtime_events_batch", _record)
    monkeypatch.setattr(
        lifecycle,
        "_validated_latest_lifecycle_in_session",
        lambda session, run_id: dict(latest),
    )
    monkeypatch.setattr(
        lifecycle,
        "_project_bot_run_summary_in_session",
        lambda _session, _payload: (_ for _ in ()).throw(RuntimeError("summary projection failed")),
    )

    with pytest.raises(RuntimeError, match="summary projection failed"):
        lifecycle.record_bot_run_lifecycle_checkpoint(
            startup_lifecycle.lifecycle_checkpoint_payload(
                bot_id="bot-1",
                run_id="run-1",
                phase=startup_lifecycle.BotLifecyclePhase.CONTAINER_BOOTING.value,
                owner=startup_lifecycle.LifecycleOwner.CONTAINER.value,
                message="Container booting.",
            )
        )


def test_record_bot_run_lifecycle_checkpoint_fails_when_canonical_builder_returns_no_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lifecycle, "db", _db_available())
    monkeypatch.setattr(lifecycle, "_latest_canonical_lifecycle_row", lambda _run_id: None)
    monkeypatch.setattr(lifecycle, "build_botlens_domain_events_from_lifecycle", lambda **_kwargs: [])
    monkeypatch.setattr(
        lifecycle,
        "record_bot_runtime_events_batch",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("canonical persistence must not run")),
    )

    with pytest.raises(RuntimeError, match="canonical lifecycle builder produced no events"):
        lifecycle.record_bot_run_lifecycle_checkpoint(
            startup_lifecycle.lifecycle_checkpoint_payload(
                bot_id="bot-1",
                run_id="run-1",
                phase=startup_lifecycle.BotLifecyclePhase.CONTAINER_BOOTING.value,
                owner=startup_lifecycle.LifecycleOwner.CONTAINER.value,
                message="Container booting.",
            )
        )


def test_get_and_list_lifecycle_use_only_canonical_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    canonical = {"run_id": "run-1", "phase": "live", "status": "running", "seq": 8}
    events = [{"run_id": "run-1", "phase": "container_booting", "status": "starting", "seq": 1}]

    monkeypatch.setattr(lifecycle, "_latest_canonical_lifecycle_row", lambda _run_id: dict(canonical))
    monkeypatch.setattr(lifecycle, "_list_canonical_lifecycle_rows", lambda _run_id: list(events))

    assert lifecycle.get_bot_run_lifecycle("run-1") == canonical
    assert lifecycle.list_bot_run_lifecycle_events("run-1") == events


def test_list_latest_lifecycles_resolves_missing_run_ids_from_run_summaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows_by_run = {
        "run-1": {"run_id": "run-1", "bot_id": "bot-1", "phase": "live", "status": "running"},
        "run-2": {"run_id": "run-2", "bot_id": "bot-2", "phase": "completed", "status": "completed"},
    }
    monkeypatch.setattr(lifecycle, "db", _db_available())
    monkeypatch.setattr(
        lifecycle,
        "list_latest_bot_runs_by_bot_ids",
        lambda bot_ids: {"bot-2": {"run_id": "run-2"}} if bot_ids == ["bot-2"] else {},
    )
    monkeypatch.setattr(
        lifecycle,
        "_latest_canonical_lifecycle_rows",
        lambda run_ids: {run_id: dict(rows_by_run[run_id]) for run_id in run_ids},
    )

    result = lifecycle.list_latest_bot_run_lifecycles(
        ["bot-1", "bot-2"],
        run_ids_by_bot={"bot-1": "run-1"},
    )

    assert result == {
        "bot-1": rows_by_run["run-1"],
        "bot-2": rows_by_run["run-2"],
    }


def test_run_summary_projection_updates_status_and_terminal_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint = datetime(2026, 7, 24, 12, 0, 0)
    bot = SimpleNamespace(
        name="Bot",
        strategy_id="strategy-1",
        run_type="backtest",
        backtest_start=None,
        backtest_end=None,
    )
    run = lifecycle.BotRunRecord(
        run_id="run-1",
        bot_id="bot-1",
        status="running",
        run_type="backtest",
        created_at=checkpoint,
        updated_at=checkpoint,
    )

    class _Session:
        def get(self, model, identity):
            if model is lifecycle.BotRecord:
                return bot
            if model is lifecycle.BotRunRecord:
                return run
            raise AssertionError(f"unexpected model {model}")

        def add(self, _row):
            raise AssertionError("existing run should be updated, not inserted")

    result = lifecycle._project_bot_run_summary_in_session(
        _Session(),
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "status": "completed",
            "checkpoint_at": checkpoint,
        }
    )

    assert result["status"] == "completed"
    assert result["ended_at"] == "2026-07-24T12:00:00Z"
    assert run.status == "completed"
    assert run.ended_at == checkpoint


def test_rebuild_bot_run_lifecycle_summary_restores_status_and_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_checkpoint = datetime(2026, 7, 24, 12, 0, 0)
    final_checkpoint = datetime(2026, 7, 24, 12, 5, 0)
    run = lifecycle.BotRunRecord(
        run_id="run-1",
        bot_id="bot-1",
        status="idle",
        run_type="backtest",
        created_at=first_checkpoint,
        updated_at=first_checkpoint,
    )

    class _Session:
        def get(self, model, identity):
            assert model is lifecycle.BotRunRecord
            assert identity == "run-1"
            return run

    session = _Session()

    @contextmanager
    def _session_scope():
        yield session

    fake_db = SimpleNamespace(available=True, session=_session_scope)
    rows = [
        {
            "event_id": "event-1",
            "run_id": "run-1",
            "bot_id": "bot-1",
            "phase": "start_requested",
            "status": "starting",
            "checkpoint_at": first_checkpoint.isoformat() + "Z",
        },
        {
            "event_id": "event-2",
            "run_id": "run-1",
            "bot_id": "bot-1",
            "phase": "completed",
            "status": "completed",
            "checkpoint_at": final_checkpoint.isoformat() + "Z",
        },
    ]

    def _project(_session, payload):
        run.status = payload["status"]
        return run.to_dict()

    monkeypatch.setattr(lifecycle, "db", fake_db)
    monkeypatch.setattr(
        lifecycle,
        "_canonical_lifecycle_rows_in_session",
        lambda _session, _run_id: list(rows),
    )
    monkeypatch.setattr(lifecycle, "_project_bot_run_summary_in_session", _project)

    result = lifecycle.rebuild_bot_run_lifecycle_summary("run-1")

    assert result["status"] == "completed"
    assert run.started_at == first_checkpoint
    assert run.ended_at == final_checkpoint


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


def test_canonical_lifecycle_rows_reject_unknown_phase() -> None:
    with pytest.raises(ValueError, match="unknown canonical lifecycle phase"):
        lifecycle._validate_canonical_lifecycle_rows(
            [
                {
                    "event_id": "event-1",
                    "run_id": "run-1",
                    "phase": "mystery",
                    "status": "idle",
                    "checkpoint_at": "2026-07-24T12:00:00Z",
                }
            ],
            run_id="run-1",
        )


def test_canonical_lifecycle_rows_reject_backdated_checkpoint() -> None:
    with pytest.raises(ValueError, match="chronology regression"):
        lifecycle._validate_canonical_lifecycle_rows(
            [
                {
                    "event_id": "event-1",
                    "run_id": "run-1",
                    "phase": "container_booting",
                    "status": "starting",
                    "checkpoint_at": "2026-07-24T12:00:01Z",
                },
                {
                    "event_id": "event-2",
                    "run_id": "run-1",
                    "phase": "loading_bot_config",
                    "status": "starting",
                    "checkpoint_at": "2026-07-24T12:00:00Z",
                },
            ],
            run_id="run-1",
        )


def test_canonical_lifecycle_rows_reject_post_terminal_checkpoint() -> None:
    with pytest.raises(ValueError, match="cannot append after terminal state"):
        lifecycle._validate_canonical_lifecycle_rows(
            [
                {
                    "event_id": "event-1",
                    "run_id": "run-1",
                    "phase": "completed",
                    "status": "completed",
                    "checkpoint_at": "2026-07-24T12:00:00Z",
                },
                {
                    "event_id": "event-2",
                    "run_id": "run-1",
                    "phase": "stopped",
                    "status": "stopped",
                    "checkpoint_at": "2026-07-24T12:00:01Z",
                },
            ],
            run_id="run-1",
        )
