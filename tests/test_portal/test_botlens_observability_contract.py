from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.observability import (
    BackendObserver,
    QueueStateMetricOwner,
    get_observability_sink,
    reset_observability_sink,
)
from portal.backend.service.bots.botlens_intake_router import IntakeRouter
from portal.backend.service.bots.botlens_mailbox import FanoutEnvelope, SymbolMailbox
from portal.backend.service.bots.botlens_projector_registry import ProjectorRegistry
from portal.backend.service.bots.botlens_symbol_projector import SymbolProjector
from portal.backend.service.storage.repos import runtime_events
import portal.backend.service.bots.botlens_symbol_projector as sym_mod


class _FakeWebSocket:
    async def accept(self) -> None:  # pragma: no cover - not used here
        return None

    async def send_text(self, _payload: str) -> None:  # pragma: no cover - not used here
        return None

    async def close(self, code: int = 1000) -> None:  # pragma: no cover - not used here
        return None


class _FakeScalarResult:
    def __init__(self, value):
        self._value = value

    def scalars(self):
        return self

    def first(self):
        return self._value

    def all(self):
        return list(self._value)


class _FakeViewStateRow:
    def __init__(self, payload):
        self._payload = dict(payload)
        self.event_id = self._payload.get("event_id")
        self.seq = self._payload.get("seq")

    def to_dict(self):
        return dict(self._payload)


class _FakeSession:
    def __init__(self, execute_values):
        self._values = list(execute_values)
        self.execute_calls = 0
        self.added = []

    def execute(self, _stmt):
        index = min(self.execute_calls, len(self._values) - 1)
        value = self._values[index]
        self.execute_calls += 1
        if isinstance(value, Exception):
            raise value
        return _FakeScalarResult(value)

    def add(self, row):
        self.added.append(row)

    def add_all(self, rows):
        self.added.extend(rows)

    def flush(self):
        return None

    def get(self, _model, _key):
        return None


class _FakeDb:
    available = True

    def __init__(self, execute_values):
        self.session_handle = _FakeSession(execute_values)

    @contextmanager
    def session(self):
        yield self.session_handle

    def reset_connection_state(self):
        return None


@pytest.fixture(autouse=True)
def _reset_sink() -> None:
    reset_observability_sink()


def _sink_names(kind: str) -> list[str]:
    snapshot = get_observability_sink().snapshot()
    return [entry["name"] for entry in snapshot[kind]]


def _queue_owner(*, key: str, depth_metric: str, utilization_metric: str, oldest_age_metric: str | None = None, **labels):
    return QueueStateMetricOwner(
        observer=BackendObserver(component="test_queue_owner"),
        key=key,
        depth_metric=depth_metric,
        utilization_metric=utilization_metric,
        oldest_age_metric=oldest_age_metric,
        labels=labels,
    )


def test_backend_observer_interval_gauge_throttles_emission() -> None:
    observer = BackendObserver(component="test_observer")

    with patch("portal.backend.service.observability.time.monotonic", side_effect=[0.0, 0.2, 1.3]):
        observer.maybe_gauge("queue:run-1", "viewer_active_count", 1.0, run_id="run-1")
        observer.maybe_gauge("queue:run-1", "viewer_active_count", 2.0, run_id="run-1")
        observer.maybe_gauge("queue:run-1", "viewer_active_count", 3.0, run_id="run-1")

    metrics = [m for m in get_observability_sink().snapshot()["metrics"] if m["name"] == "viewer_active_count"]
    assert [metric["value"] for metric in metrics] == [1.0, 3.0]


@pytest.mark.asyncio
async def test_intake_router_invalid_envelope_emits_event_and_counter() -> None:
    registry = ProjectorRegistry(run_stream=_FakeWebSocket())
    router = IntakeRouter(registry=registry)

    await router.route(["not", "a", "mapping"])

    snapshot = get_observability_sink().snapshot()
    assert any(metric["name"] == "ingest_messages_invalid_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "intake_invalid_envelope" for event in snapshot["events"])


@pytest.mark.asyncio
async def test_intake_router_bounds_message_kind_metric_labels() -> None:
    registry = ProjectorRegistry(run_stream=_FakeWebSocket())
    router = IntakeRouter(registry=registry)

    await router.route({"kind": "future-kind-v99", "bot_id": "bot-1", "run_id": "run-1"})
    await router.route({"kind": "bot_projection_refresh", "bot_id": "bot-1", "run_id": "run-1"})

    ingest_metrics = [
        metric
        for metric in get_observability_sink().snapshot()["metrics"]
        if metric["name"] in {"ingest_messages_total", "ingest_messages_unknown_kind_total", "ingest_route_ms"}
    ]

    assert any(metric["labels"].get("message_kind") == "unknown" for metric in ingest_metrics)
    assert any(metric["labels"].get("message_kind") == "deprecated" for metric in ingest_metrics)


def test_symbol_mailbox_overflow_emits_drop_metrics() -> None:
    mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
    for index in range(mailbox.fact_queue.maxsize):
        assert mailbox.enqueue_facts({"seq": index})

    assert mailbox.enqueue_facts({"seq": "overflow"}) is False

    snapshot = get_observability_sink().snapshot()
    assert any(metric["name"] == "symbol_fact_dropped_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "symbol_fact_queue_overflow" for event in snapshot["events"])


def test_bootstrap_supersede_emits_metric_and_event() -> None:
    mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
    mailbox.set_bootstrap({"seq": 1})
    mailbox.set_bootstrap({"seq": 2})

    snapshot = get_observability_sink().snapshot()
    assert any(metric["name"] == "bootstrap_superseded_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "bootstrap_superseded" for event in snapshot["events"])


@pytest.mark.asyncio
async def test_fanout_drop_emits_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sym_mod, "upsert_bot_run_view_state", lambda row: dict(row))
    monkeypatch.setattr(sym_mod, "record_bot_runtime_event", lambda row: dict(row))
    monkeypatch.setattr(sym_mod, "get_latest_bot_run_view_state", lambda **kw: None)

    fanout = __import__("asyncio").Queue(maxsize=1)
    fanout.put_nowait({"sentinel": True})
    projector = SymbolProjector(
        run_id="run-1",
        bot_id="bot-1",
        symbol_key="instrument-btc|1m",
        mailbox=SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m"),
        run_notifications=__import__("asyncio").Queue(),
        fanout_channel=fanout,
        run_notification_queue_metrics=_queue_owner(
            key="run_notification_queue:run-1",
            depth_metric="run_notification_queue_depth",
            utilization_metric="run_notification_queue_utilization",
            oldest_age_metric="run_notification_queue_oldest_age_ms",
            bot_id="bot-1",
            run_id="run-1",
            queue_name="run_notification_queue",
        ),
        fanout_queue_metrics=_queue_owner(
            key="fanout_channel:run-1",
            depth_metric="fanout_queue_depth",
            utilization_metric="fanout_queue_utilization",
            oldest_age_metric="fanout_queue_oldest_age_ms",
            bot_id="bot-1",
            run_id="run-1",
            queue_name="fanout_channel",
        ),
    )
    await projector._load_initial_state()
    await projector._apply_bootstrap(
        {
            "kind": "botlens_runtime_bootstrap_facts",
            "bot_id": "bot-1",
            "run_id": "run-1",
            "series_key": "instrument-btc|1m",
            "run_seq": 1,
            "bridge_session_id": "session-1",
            "bridge_seq": 1,
            "facts": [
                {"fact_type": "runtime_state_observed", "runtime": {"status": "running"}},
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
            ],
            "event_time": "2026-01-01T00:00:00Z",
            "known_at": "2026-01-01T00:00:00Z",
        }
    )

    snapshot = get_observability_sink().snapshot()
    assert any(metric["name"] == "fanout_dropped_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "fanout_channel_overflow" for event in snapshot["events"])
    assert all(event["name"] != "fanout_delta_drop" for event in snapshot["events"])
    fanout_gauges = [metric for metric in snapshot["metrics"] if metric["name"] == "fanout_queue_depth"]
    assert fanout_gauges
    assert all("message_kind" not in metric["labels"] for metric in fanout_gauges)
    assert all("series_key" not in metric["labels"] for metric in fanout_gauges)


def test_upsert_bot_run_view_state_emits_db_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_db = _FakeDb([None])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    runtime_events.upsert_bot_run_view_state(
        {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "series_key": "instrument-btc|1m",
            "seq": 1,
            "schema_version": 4,
            "payload": {"detail": {"symbol_key": "instrument-btc|1m"}},
            "known_at": "2026-01-01T00:00:00Z",
        }
    )

    snapshot = get_observability_sink().snapshot()
    metric_names = [metric["name"] for metric in snapshot["metrics"]]
    assert "db_write_total" in metric_names
    assert "db_write_ms" in metric_names
    assert "db_write_payload_bytes" in metric_names


def test_upsert_bot_run_view_state_duplicate_skip_does_not_emit_write_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb([_FakeViewStateRow({"seq": 1, "payload": {"detail": {}}})])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    runtime_events.upsert_bot_run_view_state(
        {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "series_key": "instrument-btc|1m",
            "seq": 1,
            "schema_version": 4,
            "payload": {"detail": {"symbol_key": "instrument-btc|1m"}},
            "known_at": "2026-01-01T00:00:00Z",
        }
    )

    snapshot = get_observability_sink().snapshot()
    metric_names = [metric["name"] for metric in snapshot["metrics"]]
    assert "db_duplicate_skip_total" in metric_names
    assert "db_write_total" not in metric_names


def test_upsert_bot_run_view_state_stale_update_emits_stale_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb([_FakeViewStateRow({"seq": 4, "payload": {"detail": {}}})])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    runtime_events.upsert_bot_run_view_state(
        {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "series_key": "instrument-btc|1m",
            "seq": 2,
            "schema_version": 4,
            "payload": {"detail": {"symbol_key": "instrument-btc|1m"}},
            "known_at": "2026-01-01T00:00:00Z",
        }
    )

    snapshot = get_observability_sink().snapshot()
    assert any(metric["name"] == "db_stale_update_total" for metric in snapshot["metrics"])
    assert all(metric["name"] != "db_write_total" for metric in snapshot["metrics"])


def test_record_bot_runtime_event_seq_collision_emits_event(monkeypatch: pytest.MonkeyPatch) -> None:
    existing = _FakeViewStateRow(
        {
            "event_id": "existing-event",
            "seq": 7,
        }
    )
    fake_db = _FakeDb([None, existing])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    with pytest.raises(ValueError):
        runtime_events.record_bot_runtime_event(
            {
                "event_id": "incoming-event",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 7,
                "event_type": "botlens.runtime_facts",
                "schema_version": 4,
                "payload": {"facts": []},
            }
        )

    snapshot = get_observability_sink().snapshot()
    assert any(event["name"] == "db_seq_collision" for event in snapshot["events"])


@pytest.mark.asyncio
async def test_symbol_projector_records_caller_side_persistence_wait_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sym_mod, "upsert_bot_run_view_state", lambda row: dict(row))
    monkeypatch.setattr(sym_mod, "record_bot_runtime_event", lambda row: dict(row))
    monkeypatch.setattr(sym_mod, "get_latest_bot_run_view_state", lambda **kw: None)

    projector = SymbolProjector(
        run_id="run-1",
        bot_id="bot-1",
        symbol_key="instrument-btc|1m",
        mailbox=SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m"),
        run_notifications=__import__("asyncio").Queue(),
        fanout_channel=__import__("asyncio").Queue(),
        run_notification_queue_metrics=_queue_owner(
            key="run_notification_queue:run-1",
            depth_metric="run_notification_queue_depth",
            utilization_metric="run_notification_queue_utilization",
            oldest_age_metric="run_notification_queue_oldest_age_ms",
            bot_id="bot-1",
            run_id="run-1",
            queue_name="run_notification_queue",
        ),
        fanout_queue_metrics=_queue_owner(
            key="fanout_channel:run-1",
            depth_metric="fanout_queue_depth",
            utilization_metric="fanout_queue_utilization",
            oldest_age_metric="fanout_queue_oldest_age_ms",
            bot_id="bot-1",
            run_id="run-1",
            queue_name="fanout_channel",
        ),
    )
    await projector._load_initial_state()
    await projector._apply_bootstrap(
        {
            "kind": "botlens_runtime_bootstrap_facts",
            "bot_id": "bot-1",
            "run_id": "run-1",
            "series_key": "instrument-btc|1m",
            "run_seq": 1,
            "bridge_session_id": "session-1",
            "bridge_seq": 1,
            "facts": [
                {"fact_type": "runtime_state_observed", "runtime": {"status": "running"}},
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
            ],
            "event_time": "2026-01-01T00:00:00Z",
            "known_at": "2026-01-01T00:00:00Z",
        }
    )

    wait_metrics = [
        metric
        for metric in get_observability_sink().snapshot()["metrics"]
        if metric["name"] == "persistence_wait_ms"
    ]
    pipeline_stages = {metric["labels"].get("pipeline_stage") for metric in wait_metrics}
    assert {"detail_state_persist", "raw_event_persist"} <= pipeline_stages


@pytest.mark.asyncio
async def test_symbol_projector_runtime_event_no_longer_persists_typed_delta_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = []
    monkeypatch.setattr(sym_mod, "upsert_bot_run_view_state", lambda row: dict(row))
    monkeypatch.setattr(sym_mod, "record_bot_runtime_event", lambda row: captured.append(dict(row)) or dict(row))
    monkeypatch.setattr(sym_mod, "get_latest_bot_run_view_state", lambda **kw: None)

    projector = SymbolProjector(
        run_id="run-1",
        bot_id="bot-1",
        symbol_key="instrument-btc|1m",
        mailbox=SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="instrument-btc|1m"),
        run_notifications=__import__("asyncio").Queue(),
        fanout_channel=__import__("asyncio").Queue(),
        run_notification_queue_metrics=_queue_owner(
            key="run_notification_queue:run-1",
            depth_metric="run_notification_queue_depth",
            utilization_metric="run_notification_queue_utilization",
            oldest_age_metric="run_notification_queue_oldest_age_ms",
            bot_id="bot-1",
            run_id="run-1",
            queue_name="run_notification_queue",
        ),
        fanout_queue_metrics=_queue_owner(
            key="fanout_channel:run-1",
            depth_metric="fanout_queue_depth",
            utilization_metric="fanout_queue_utilization",
            oldest_age_metric="fanout_queue_oldest_age_ms",
            bot_id="bot-1",
            run_id="run-1",
            queue_name="fanout_channel",
        ),
    )
    await projector._load_initial_state()
    await projector._apply_bootstrap(
        {
            "kind": "botlens_runtime_bootstrap_facts",
            "bot_id": "bot-1",
            "run_id": "run-1",
            "series_key": "instrument-btc|1m",
            "run_seq": 1,
            "bridge_session_id": "session-1",
            "bridge_seq": 1,
            "facts": [
                {"fact_type": "runtime_state_observed", "runtime": {"status": "running"}},
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
            ],
            "event_time": "2026-01-01T00:00:00Z",
            "known_at": "2026-01-01T00:00:00Z",
        }
    )

    assert captured
    assert "typed_delta_metrics" not in captured[0]["payload"]
