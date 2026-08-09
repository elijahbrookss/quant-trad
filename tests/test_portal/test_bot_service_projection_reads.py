from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import bot_service


class _FakeConfigService:
    def __init__(self) -> None:
        self._bots = [
            {
                "id": "bot-1",
                "name": "Bot 1",
                "strategy_id": "strategy-1",
                "wallet_config": {"balances": {"USDC": 100.0}},
                "snapshot_interval_ms": 1000,
                "run_type": "backtest",
            }
        ]

    def list_bots(self):
        return [dict(bot) for bot in self._bots]

    def get_bot(self, bot_id: str):
        for bot in self._bots:
            if bot["id"] == bot_id:
                return dict(bot)
        raise KeyError(bot_id)


class _FakeStorage:
    def __init__(self) -> None:
        self.botlens_evidence = {}
        self.run = {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "status": "running",
            "started_at": "2026-04-09T04:21:37Z",
            "summary": {"total_trades": 4},
        }
        self.lifecycle = {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "phase": "live",
            "status": "running",
            "owner": "runtime",
            "message": "live",
            "metadata": {},
            "failure": {},
            "checkpoint_at": "2026-04-09T04:21:43Z",
            "updated_at": "2026-04-09T04:21:43Z",
        }
        self.lease = {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "runner_id": "runner-1",
            "status": "active",
            "expires_at": "2099-01-01T00:00:00Z",
            "released_at": None,
        }

    def get_latest_bot_run_lifecycle(self, bot_id: str):
        return dict(self.lifecycle) if str(bot_id) == "bot-1" else None

    def get_latest_bot_runtime_run_id(self, bot_id: str):
        return "run-1" if str(bot_id) == "bot-1" else None

    def get_bot_run_lifecycle(self, run_id: str):
        return dict(self.lifecycle) if str(run_id) == "run-1" else None

    def get_bot_run(self, run_id: str):
        return dict(self.run) if str(run_id) == "run-1" else None

    def get_bot_run_lease(self, run_id: str):
        return dict(self.lease) if str(run_id) == "run-1" else None

    def get_report_materialization_status(self, run_id: str):
        return {"run_id": run_id, "status": "not_started", "can_view": False}

    def list_latest_bot_runs_by_bot_ids(self, bot_ids):
        return {
            bot_id: dict(self.run)
            for bot_id in bot_ids
            if str(bot_id) == "bot-1"
        }

    def list_latest_bot_run_lifecycles(self, bot_ids, *, run_ids_by_bot=None):
        _ = run_ids_by_bot
        return {
            bot_id: dict(self.lifecycle)
            for bot_id in bot_ids
            if str(bot_id) == "bot-1"
        }

    def list_bot_runs_by_ids(self, run_ids):
        return {
            run_id: dict(self.run)
            for run_id in run_ids
            if str(run_id) == "run-1"
        }

    def list_bot_run_leases_by_run_ids(self, run_ids):
        return {
            run_id: dict(self.lease)
            for run_id in run_ids
            if str(run_id) == "run-1"
        }

    def list_report_materialization_statuses(self, run_ids):
        return {
            run_id: self.get_report_materialization_status(run_id)
            for run_id in run_ids
        }

    def list_bot_runs(self, bot_id: str):
        return [dict(self.run)] if str(bot_id) == "bot-1" else []

    def list_bot_runs_page(
        self, *, limit=100, before_sort_at=None, before_run_id=None
    ):
        _ = before_sort_at, before_run_id
        return [dict(self.run)][:limit]

    def list_botlens_run_evidence(self, run_ids):
        return {
            run_id: dict(self.botlens_evidence[run_id])
            for run_id in run_ids
            if run_id in self.botlens_evidence
        }


class _FakeTelemetryHub:
    def __init__(self, snapshots: dict[str, object] | None = None) -> None:
        self._snapshots = dict(snapshots or {})

    def get_run_snapshot(self, *, run_id: str):
        return self._snapshots.get(str(run_id))


class _FakeComposition:
    def __init__(self, *, config_service, storage) -> None:
        self.config_service = config_service
        self.storage = storage
        self.stream_manager = SimpleNamespace(broadcast=lambda *args, **kwargs: None)
        self.runtime_control_service = SimpleNamespace(
            start_bot=lambda bot_id: {"id": bot_id},
            stop_bot=lambda bot_id, preserve_container=False: {"id": bot_id, "preserve_container": preserve_container},
            bots_stream=lambda: None,
            watchdog_status=lambda: {},
        )
        self.watchdog = SimpleNamespace(set_orphan_callback=lambda callback: None)


def test_list_bot_runs_inventory_is_bounded_and_does_not_replay(monkeypatch):
    storage = _FakeStorage()
    storage.run["config_snapshot"] = {
        "dataset_binding": {
            "dataset_id": "dataset-1",
            "dataset_hash": "hash-1",
            "raw_rows": ["must-not-leak"] * 100,
        },
        "runtime_payload": {"large": ["must-not-leak"] * 100},
    }
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=storage)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())

    result = bot_service.list_bot_runs_inventory(limit=25)

    assert result["schema_version"] == "bot_run_inventory.v1"
    assert result["runs"][0]["run_id"] == "run-1"
    assert result["runs"][0]["is_active"] is True
    assert result["runs"][0]["botlens_available"] is False
    assert result["runs"][0]["config_snapshot"] == {
        "dataset_binding": {
            "dataset_id": "dataset-1",
            "dataset_hash": "hash-1",
        }
    }
    assert result["next_cursor"] is None
    assert result["observed_at"]


def test_active_run_inventory_keeps_sibling_runs_of_one_definition_distinct(monkeypatch):
    storage = _FakeStorage()
    runs = {
        "run-1": {
            **storage.run,
            "run_id": "run-1",
            "created_at": "2026-04-09T04:21:37Z",
            "started_at": "2026-04-09T04:21:38Z",
        },
        "run-2": {
            **storage.run,
            "run_id": "run-2",
            "created_at": "2026-04-09T04:22:37Z",
            "started_at": "2026-04-09T04:22:38Z",
        },
    }
    lifecycles = {
        run_id: {**storage.lifecycle, "run_id": run_id}
        for run_id in runs
    }
    leases = [
        {**storage.lease, "run_id": run_id}
        for run_id in runs
    ]
    storage.list_active_bot_run_leases = lambda: list(leases)
    storage.list_bot_runs_by_ids = lambda run_ids: {
        run_id: dict(runs[run_id]) for run_id in run_ids
    }
    storage.list_bot_run_lifecycles = lambda run_ids: {
        run_id: dict(lifecycles[run_id]) for run_id in run_ids
    }
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=storage)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())

    result = bot_service.list_active_run_instances()

    assert [row["run_id"] for row in result] == ["run-2", "run-1"]
    assert {row["bot_id"] for row in result} == {"bot-1"}
    assert all(row["is_active"] is True for row in result)
    assert all(row["liveness"]["state"] == "awaiting_telemetry" for row in result)


def test_list_bot_runs_for_bot_reports_snapshot_unavailable_without_replay(monkeypatch):
    def _unexpected_replay(*args, **kwargs):
        _ = args, kwargs
        raise AssertionError("run list projection must not trigger replay")

    composition = _FakeComposition(config_service=_FakeConfigService(), storage=_FakeStorage())
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(
        "portal.backend.service.bots.botlens_event_replay.rebuild_run_projection_snapshot",
        _unexpected_replay,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.bot_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "running",
            "running": True,
            "id": "container-1",
            "started_at": "2026-04-09T04:21:37Z",
            "finished_at": None,
            "exit_code": None,
            "error": None,
        },
    )

    result = bot_service.list_bot_runs_for_bot("bot-1")

    assert result["active_run_id"] == "run-1"
    assert result["runs"][0]["botlens_available"] is False
    assert result["runs"][0]["botlens_reason"] == "durable_evidence_unavailable"
    assert result["runs"][0]["seq"] is None


def test_run_inventory_exposes_durable_botlens_evidence_without_replay(monkeypatch):
    storage = _FakeStorage()
    storage.run["status"] = "completed"
    storage.lifecycle.update({"phase": "completed", "status": "completed"})
    storage.botlens_evidence["run-1"] = {
        "source": "durable_event_ledger",
        "event_count": 3254,
        "max_seq": 3254,
        "max_row_id": 9001,
        "known_at": "2026-04-09T04:21:43Z",
    }
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=storage)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())

    result = bot_service.list_bot_runs_inventory(limit=25)

    run = result["runs"][0]
    assert run["botlens_available"] is True
    assert run["botlens_source"] == "durable_event_ledger"
    assert run["seq"] == 3254
    assert run["known_at"] == "2026-04-09T04:21:43Z"


def test_run_inspection_exposes_durable_botlens_evidence_without_hot_snapshot(monkeypatch):
    storage = _FakeStorage()
    storage.botlens_evidence["run-1"] = {
        "source": "durable_event_ledger",
        "event_count": 81,
        "max_seq": 79,
        "max_row_id": 120,
        "known_at": "2026-04-09T04:21:43Z",
    }
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=storage)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())

    result = bot_service.get_bot_run_inspection("run-1")

    projection = result["run"]["projection"]
    assert projection == {
        "available": True,
        "reason": None,
        "source": "durable_event_ledger",
        "known_at": "2026-04-09T04:21:43Z",
        "seq": 79,
        "event_count": 81,
    }


def test_list_bot_runs_for_bot_keeps_persisted_terminal_status_when_snapshot_is_stale(monkeypatch):
    storage = _FakeStorage()
    storage.run["status"] = "completed"
    storage.lifecycle.update({"phase": "completed", "status": "completed"})
    snapshot = SimpleNamespace(
        health=SimpleNamespace(
            to_dict=lambda: {
                "status": "starting",
                "last_event_at": "2026-04-09T04:21:43Z",
            }
        ),
        symbol_catalog=SimpleNamespace(entries={}),
        seq=3,
    )
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=storage)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(
        bot_service,
        "_telemetry_hub",
        lambda: _FakeTelemetryHub({"run-1": snapshot}),
    )

    result = bot_service.list_bot_runs_for_bot("bot-1")

    assert result["runs"][0]["status"] == "completed"
    assert result["runs"][0]["runtime_status"] == "completed"


def test_get_bot_run_status_uses_fresh_hot_checkpoint_and_progress(monkeypatch):
    storage = _FakeStorage()
    snapshot = SimpleNamespace(
        health=SimpleNamespace(
            to_dict=lambda: {
                "status": "running",
                "progress": 0.72,
                "last_event_at": "2026-04-09T05:30:00Z",
            }
        ),
    )
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=storage)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(
        bot_service,
        "_telemetry_hub",
        lambda: _FakeTelemetryHub({"run-1": snapshot}),
    )

    result = bot_service.get_bot_run_status("bot-1", "run-1")

    assert result["status"] == "running"
    assert result["checkpoint_at"] == "2026-04-09T05:30:00Z"
    assert result["updated_at"] == "2026-04-09T05:30:00Z"
    assert result["progress"] == pytest.approx(0.72)
    assert result["progress_unit"] == "fraction"


def test_get_bot_run_status_prefers_persisted_terminal_truth_over_stale_lifecycle(monkeypatch):
    storage = _FakeStorage()
    storage.run["status"] = "completed"
    storage.run["ended_at"] = "2026-04-09T06:00:00Z"
    snapshot = SimpleNamespace(
        health=SimpleNamespace(
            to_dict=lambda: {
                "status": "running",
                "progress": 0.99,
                "last_event_at": "2026-04-09T05:59:59Z",
            }
        ),
    )
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=storage)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(
        bot_service,
        "_telemetry_hub",
        lambda: _FakeTelemetryHub({"run-1": snapshot}),
    )

    result = bot_service.get_bot_run_status("bot-1", "run-1")

    assert result["status"] == "completed"
    assert result["terminal"] is True
    assert result["completed"] is True
    assert result["active"] is False


def test_runtime_capacity_marks_estimate_incomplete_when_snapshot_missing(monkeypatch):
    composition = _FakeComposition(config_service=_FakeConfigService(), storage=_FakeStorage())
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())

    result = bot_service.runtime_capacity()

    assert result["running_bots"] == 1
    assert result["workers_in_use"] == 0
    assert result["workers_requested"] == 0
    assert result["telemetry_unavailable_bots"] == 1
    assert result["estimate_incomplete"] is True


def test_publish_projected_bot_skips_projection_without_stream_subscribers(monkeypatch):
    class _ConfigThatShouldNotLoad(_FakeConfigService):
        def get_bot(self, bot_id: str):
            raise AssertionError("projection should not load bot when nobody is subscribed")

    composition = _FakeComposition(config_service=_ConfigThatShouldNotLoad(), storage=_FakeStorage())
    composition.stream_manager = SimpleNamespace(
        has_subscribers=lambda: False,
        broadcast=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not broadcast")),
    )
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)

    bot_service.publish_projected_bot("bot-1")


def test_list_bots_uses_batched_projection_reads(monkeypatch):
    class _BatchConfig(_FakeConfigService):
        def __init__(self) -> None:
            self._bots = [
                {
                    "id": "bot-1",
                    "name": "Bot 1",
                    "strategy_id": "strategy-1",
                },
                {
                    "id": "bot-2",
                    "name": "Bot 2",
                    "strategy_id": "strategy-1",
                },
            ]

    class _BatchStorage(_FakeStorage):
        def __init__(self) -> None:
            super().__init__()
            self.single_latest_runtime_calls = 0
            self.batch_latest_run_calls = 0
            self.batch_lifecycle_calls = 0
            self.batch_status_calls = 0

        def get_latest_bot_runtime_run_id(self, bot_id: str):
            self.single_latest_runtime_calls += 1
            return super().get_latest_bot_runtime_run_id(bot_id)

        def list_latest_bot_runs_by_bot_ids(self, bot_ids):
            self.batch_latest_run_calls += 1
            return {
                bot_id: {
                    "run_id": f"run-{bot_id[-1]}",
                    "bot_id": bot_id,
                    "status": "completed",
                    "summary": {},
                }
                for bot_id in bot_ids
            }

        def list_latest_bot_run_lifecycles(self, bot_ids, *, run_ids_by_bot=None):
            self.batch_lifecycle_calls += 1
            return {
                bot_id: {
                    "bot_id": bot_id,
                    "run_id": (run_ids_by_bot or {}).get(bot_id),
                    "phase": "completed",
                    "status": "completed",
                    "metadata": {},
                    "failure": {},
                }
                for bot_id in bot_ids
            }

        def list_bot_runs_by_ids(self, run_ids):
            return {
                run_id: {
                    "run_id": run_id,
                    "bot_id": f"bot-{run_id[-1]}",
                    "status": "completed",
                    "summary": {},
                }
                for run_id in run_ids
            }

        def list_bot_run_leases_by_run_ids(self, run_ids):
            return {
                run_id: {
                    "run_id": run_id,
                    "bot_id": f"bot-{run_id[-1]}",
                    "runner_id": "runner-1",
                    "status": "released",
                    "expires_at": "2026-04-09T04:22:43Z",
                    "released_at": "2026-04-09T04:22:44Z",
                }
                for run_id in run_ids
            }

        def list_report_materialization_statuses(self, run_ids):
            self.batch_status_calls += 1
            return {
                run_id: {
                    "run_id": run_id,
                    "status": "not_started",
                    "can_view": False,
                    "can_build": True,
                    "can_retry": False,
                }
                for run_id in run_ids
            }

    storage = _BatchStorage()
    composition = _FakeComposition(config_service=_BatchConfig(), storage=storage)
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(
        "portal.backend.service.bots.bot_service.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: {"status": "missing", "running": False},
    )

    result = bot_service.list_bots()

    assert [row["id"] for row in result] == ["bot-1", "bot-2"]
    assert storage.batch_latest_run_calls == 1
    assert storage.batch_lifecycle_calls == 1
    assert storage.batch_status_calls == 1
    assert storage.single_latest_runtime_calls == 0


def test_list_bots_hot_read_does_not_inspect_docker(monkeypatch):
    bots = [
        {"id": "bot-1", "name": "Bot 1"},
        {"id": "bot-2", "name": "Bot 2"},
    ]
    inputs = {
        bot["id"]: (
            {"run_id": f"run-{bot['id'][-1]}", "status": "running"},
            {
                "run_id": f"run-{bot['id'][-1]}",
                "phase": "live",
                "status": "running",
                "metadata": {},
            },
            None,
            None,
        )
        for bot in bots
    }
    def _inspect(bot_id: str):
        raise AssertionError(f"hot fleet read inspected Docker for {bot_id}")

    monkeypatch.setattr(bot_service, "_load_projection_inputs_batch", lambda _bots: inputs)
    monkeypatch.setattr(
        "portal.backend.service.bots.bot_service.DockerBotRunner.inspect_bot_container",
        _inspect,
    )

    result = bot_service._project_bots(bots, inspect_container=False)

    assert [row["id"] for row in result] == ["bot-1", "bot-2"]
    assert [row["lifecycle"]["container"]["id"] for row in result] == [
        None,
        None,
    ]


def test_bots_stream_uses_batched_facade_snapshot(monkeypatch):
    observed = {}

    class _StreamManager:
        def subscribe_all(self, snapshot_fn):
            observed["snapshot_fn"] = snapshot_fn
            initial = {"type": "snapshot", "bots": snapshot_fn()}
            return (lambda: None), SimpleNamespace(), initial

    composition = _FakeComposition(config_service=_FakeConfigService(), storage=_FakeStorage())
    composition.stream_manager = _StreamManager()
    monkeypatch.setattr(bot_service, "_composition", lambda: composition)
    monkeypatch.setattr(bot_service, "list_bots", lambda: [{"id": "batched-bot"}])

    _release, _channel, initial = bot_service.bots_stream()

    assert observed["snapshot_fn"] is bot_service.list_bots
    assert initial["bots"] == [{"id": "batched-bot"}]
