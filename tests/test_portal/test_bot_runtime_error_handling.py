from portal.backend.service.bots.bot_runtime.runtime import BotRuntime


def _runtime() -> BotRuntime:
    return BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100.0}}})


def test_normalise_rejection_metadata_promotes_trade_id_when_missing():
    trade_id, metadata = BotRuntime._normalise_rejection_metadata(
        {"trade_id": "trade-from-meta", "reason": "ignored", "risk": "max_exposure"},
        None,
    )

    assert trade_id == "trade-from-meta"
    assert metadata == {"risk": "max_exposure"}


def test_normalise_rejection_metadata_keeps_blocking_trade_id():
    trade_id, metadata = BotRuntime._normalise_rejection_metadata(
        {"trade_id": "trade-from-meta", "active_trade_id": "trade-active"},
        "trade-blocking",
    )

    assert trade_id == "trade-blocking"
    assert metadata == {"active_trade_id": "trade-active"}


def test_execute_loop_persists_error_status_when_runner_sets_error(monkeypatch):
    runtime = _runtime()
    persisted: list[str] = []
    pushed: list[str] = []

    class ErrorRunner:
        def run(self) -> None:
            runtime._set_error_state("step failed")
            runtime._stop.set()

    monkeypatch.setattr(runtime, "_ensure_prepared", lambda: None)
    monkeypatch.setattr(runtime, "_set_phase", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_start_overlay_aggregator", lambda: None)
    monkeypatch.setattr(runtime, "_stop_overlay_aggregator", lambda: None)
    monkeypatch.setattr(runtime, "_build_series_runner", lambda: ErrorRunner())
    monkeypatch.setattr(runtime, "_push_update", lambda event: pushed.append(event))
    monkeypatch.setattr(runtime, "_persist_runtime_state", lambda status: persisted.append(status))
    monkeypatch.setattr(runtime, "_flush_persistence_buffer", lambda _reason: None)

    runtime._execute_loop()

    assert runtime.state.get("status") == "error"
    assert persisted == ["error"]
    assert pushed and pushed[-1] == "error"


def test_run_exception_forwards_error_update(monkeypatch):
    runtime = _runtime()
    persisted: list[str] = []
    pushed: list[str] = []
    flushed: list[str] = []

    def _raise() -> None:
        raise RuntimeError("thread exploded")

    monkeypatch.setattr(runtime, "_execute_loop", _raise)
    monkeypatch.setattr(runtime, "_push_update", lambda event: pushed.append(event))
    monkeypatch.setattr(runtime, "_persist_runtime_state", lambda status: persisted.append(status))
    monkeypatch.setattr(runtime, "_flush_persistence_buffer", lambda reason: flushed.append(reason))

    runtime._run()

    assert runtime.state.get("status") == "error"
    assert pushed == ["error"]
    assert persisted == ["error"]
    assert flushed == ["runtime_loop_failed"]
