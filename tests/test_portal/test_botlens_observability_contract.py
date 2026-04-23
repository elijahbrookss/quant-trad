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
from portal.backend.service.bots.botlens_candle_continuity import (
    CandleContinuityAccumulator,
    continuity_summary_from_candles,
    emit_candle_continuity_summary,
)
from portal.backend.service.bots.botlens_contract import BRIDGE_BOOTSTRAP_KIND
from portal.backend.service.bots.botlens_domain_events import build_botlens_domain_events_from_fact_batch
from portal.backend.service.bots.botlens_intake_router import IntakeRouter
from portal.backend.service.bots.botlens_mailbox import RunMailbox, SymbolMailbox
from portal.backend.service.bots.botlens_projection_batches import projection_batch_from_payload
from portal.backend.service.bots.botlens_projector_registry import ProjectorRegistry
from portal.backend.service.bots.botlens_run_projector import RunProjector
from portal.backend.service.bots.botlens_symbol_projector import SymbolProjector
from portal.backend.service.storage.repos import runtime_events
import portal.backend.service.bots.botlens_run_projector as run_mod
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
        if self._value is None:
            return []
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


def _bootstrap_batch(payload: dict):
    return projection_batch_from_payload(
        batch_kind=BRIDGE_BOOTSTRAP_KIND,
        run_id=str(payload["run_id"]),
        bot_id=str(payload["bot_id"]),
        symbol_key=str(payload["series_key"]),
        payload=payload,
        events=build_botlens_domain_events_from_fact_batch(
            bot_id=str(payload["bot_id"]),
            run_id=str(payload["run_id"]),
            payload=payload,
        ),
    )


def test_backend_observer_interval_gauge_throttles_emission() -> None:
    observer = BackendObserver(component="test_observer")

    with patch("portal.backend.service.observability.time.monotonic", side_effect=[0.0, 0.2, 1.3]):
        observer.maybe_gauge("queue:run-1", "viewer_active_count", 1.0, run_id="run-1")
        observer.maybe_gauge("queue:run-1", "viewer_active_count", 2.0, run_id="run-1")
        observer.maybe_gauge("queue:run-1", "viewer_active_count", 3.0, run_id="run-1")

    metrics = [m for m in get_observability_sink().snapshot()["metrics"] if m["metric_name"] == "viewer_active_count"]
    assert [metric["value"] for metric in metrics] == [1.0, 3.0]


def test_emit_candle_continuity_summary_reuses_existing_metric_and_event_surfaces() -> None:
    observer = BackendObserver(component="test_observer")
    summary = continuity_summary_from_candles(
        [
            {"time": "2026-01-01T00:00:00Z"},
            {"time": "2026-01-01T00:01:00Z"},
            {"time": "2026-01-01T00:03:00Z"},
        ],
        series_key="instrument-btc|1m",
    )

    payload = emit_candle_continuity_summary(
        observer,
        stage="botlens_selected_symbol_snapshot",
        summary=summary,
        bot_id="bot-1",
        run_id="run-1",
        instrument_id="instrument-btc",
        series_key="instrument-btc|1m",
        message_kind="ephemeral",
        boundary_name="selected_symbol_snapshot",
        extra={"contract": "botlens_selected_symbol_snapshot"},
    )

    assert payload["boundary_name"] == "selected_symbol_snapshot"
    assert payload["detected_gap_count"] == 1
    snapshot = get_observability_sink().snapshot()
    continuity_metrics = [
        metric
        for metric in snapshot["metrics"]
        if metric["metric_name"].startswith("candle_continuity_")
    ]
    assert {
        metric["metric_name"] for metric in continuity_metrics
    } >= {
        "candle_continuity_candle_count",
        "candle_continuity_gap_count",
        "candle_continuity_missing_candle_estimate",
        "candle_continuity_max_gap_multiple",
        "candle_continuity_ratio",
    }
    assert all(metric["tags"].get("pipeline_stage") == "botlens_selected_symbol_snapshot" for metric in continuity_metrics)
    assert all(metric["tags"].get("series_key") == "instrument-btc|1m" for metric in continuity_metrics)
    event = next(event for event in snapshot["events"] if event["name"] == "candle_continuity_summary")
    assert event["context"]["boundary_name"] == "selected_symbol_snapshot"
    assert event["context"]["contract"] == "botlens_selected_symbol_snapshot"
    assert event["context"]["detected_gap_count"] == 1
    assert event["context"]["gap_count_by_type"]["unknown_gap"] == 1
    assert event["context"]["final_status"] == "defect"


def test_candle_continuity_classifies_gaps_conservatively() -> None:
    candles = [
        {"time": "2026-01-01T00:00:00Z"},
        {"time": "2026-01-01T00:02:00Z"},
    ]

    unknown = continuity_summary_from_candles(candles, series_key="instrument-btc|1m")
    expected = continuity_summary_from_candles(
        candles,
        series_key="instrument-btc|1m",
        gap_classification="expected_session_gap",
    )
    provider = continuity_summary_from_candles(
        candles,
        series_key="instrument-btc|1m",
        source_reason="provider_missing_data",
    )
    ingestion = continuity_summary_from_candles(
        candles,
        series_key="instrument-btc|1m",
        source_reason="ingestion_failure",
    )

    assert unknown.gap_count_by_type["unknown_gap"] == 1
    assert unknown.defect_gap_count == 1
    assert expected.gap_count_by_type["expected_session_gap"] == 1
    assert expected.defect_gap_count == 0
    assert expected.final_status == "expected_sparse"
    assert provider.gap_count_by_type["provider_missing_data"] == 1
    assert provider.defect_gap_count == 1
    assert ingestion.gap_count_by_type["ingestion_failure"] == 1
    assert ingestion.defect_gap_count == 1


def test_candle_continuity_accumulator_detects_cross_batch_full_run_gaps() -> None:
    accumulator = CandleContinuityAccumulator(expected_interval_seconds_value=60)

    accumulator.add(
        [
            {"time": "2026-01-01T00:00:00Z"},
            {"time": "2026-01-01T00:01:00Z"},
        ]
    )
    accumulator.add([{"time": "2026-01-01T00:03:00Z"}])

    summary = accumulator.summary()

    assert summary.candle_count == 3
    assert summary.detected_gap_count == 1
    assert summary.gap_count_by_type["unknown_gap"] == 1
    assert summary.final_status == "defect"


def test_intake_router_emits_final_per_series_continuity_summary() -> None:
    reset_observability_sink()
    router = IntakeRouter(registry=ProjectorRegistry(run_stream=_FakeWebSocket()))
    facts_a = [
        {"fact_type": "candle_upserted", "candle": {"time": "2026-01-01T00:00:00Z"}},
        {"fact_type": "candle_upserted", "candle": {"time": "2026-01-01T00:01:00Z"}},
    ]
    facts_b = [
        {"fact_type": "candle_upserted", "candle": {"time": "2026-01-01T00:03:00Z"}},
    ]

    router._accumulate_continuity(
        run_id="run-1",
        series_key="instrument-btc|1m",
        facts=facts_a,
        source_reason="ingest",
    )
    router._accumulate_continuity(
        run_id="run-1",
        series_key="instrument-btc|1m",
        facts=facts_b,
        source_reason="ingest",
    )
    router._emit_final_continuity_summaries(run_id="run-1", bot_id="bot-1", reason="completed")

    event = next(event for event in get_observability_sink().snapshot()["events"] if event["name"] == "candle_continuity_summary")
    assert event["context"]["boundary_name"] == "run_final"
    assert event["context"]["series_key"] == "instrument-btc|1m"
    assert event["context"]["detected_gap_count"] == 1
    assert event["context"]["gap_count_by_type"]["unknown_gap"] == 1


@pytest.mark.asyncio
async def test_intake_router_invalid_envelope_emits_event_and_counter() -> None:
    registry = ProjectorRegistry(run_stream=_FakeWebSocket())
    router = IntakeRouter(registry=registry)

    await router.route(["not", "a", "mapping"])

    snapshot = get_observability_sink().snapshot()
    assert any(metric["metric_name"] == "ingest_messages_invalid_total" for metric in snapshot["metrics"])
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
        if metric["metric_name"] in {"ingest_messages_total", "ingest_messages_unknown_kind_total", "ingest_route_ms"}
    ]

    assert any(metric["tags"].get("message_kind") == "unknown" for metric in ingest_metrics)
    assert any(metric["tags"].get("message_kind") == "deprecated" for metric in ingest_metrics)


def test_symbol_mailbox_overflow_emits_drop_metrics() -> None:
    mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
    for index in range(mailbox.fact_queue.maxsize):
        assert mailbox.enqueue_batch({"seq": index})

    assert mailbox.enqueue_batch({"seq": "overflow"}) is False

    snapshot = get_observability_sink().snapshot()
    assert any(metric["metric_name"] == "symbol_fact_dropped_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "symbol_fact_queue_overflow" for event in snapshot["events"])


def test_bootstrap_supersede_emits_metric_and_event() -> None:
    mailbox = SymbolMailbox(run_id="run-1", bot_id="bot-1", symbol_key="btc|1m")
    mailbox.set_bootstrap({"seq": 1})
    mailbox.set_bootstrap({"seq": 2})

    snapshot = get_observability_sink().snapshot()
    assert any(metric["metric_name"] == "bootstrap_superseded_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "bootstrap_superseded" for event in snapshot["events"])


@pytest.mark.asyncio
async def test_fanout_drop_emits_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
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
        _bootstrap_batch(
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
    )

    snapshot = get_observability_sink().snapshot()
    assert any(metric["metric_name"] == "fanout_dropped_total" for metric in snapshot["metrics"])
    assert any(event["name"] == "fanout_channel_overflow" for event in snapshot["events"])
    assert all(event["name"] != "fanout_delta_drop" for event in snapshot["events"])
    fanout_gauges = [metric for metric in snapshot["metrics"] if metric["metric_name"] == "fanout_queue_depth"]
    assert fanout_gauges
    assert all("message_kind" not in metric["tags"] for metric in fanout_gauges)
    assert all("series_key" not in metric["tags"] for metric in fanout_gauges)


@pytest.mark.asyncio
async def test_symbol_projector_rebuild_failure_marks_projection_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fail_load(**_kwargs):
        raise RuntimeError("bad persisted fact")

    monkeypatch.setattr(sym_mod, "load_domain_projection_batches", _fail_load)
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

    snapshot = projector.get_snapshot()
    assert snapshot.readiness.snapshot_ready is False
    assert snapshot.diagnostics.diagnostics[0]["type"] == "projection_error"
    event = next(event for event in get_observability_sink().snapshot()["events"] if event["name"] == "ledger_rebuild_failed")
    assert event["context"]["projection_state"] == "projection_error"


@pytest.mark.asyncio
async def test_run_projector_rebuild_failure_marks_projection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fail_load(**_kwargs):
        raise RuntimeError("bad persisted run fact")

    monkeypatch.setattr(run_mod, "load_domain_projection_batches", _fail_load)
    projector = RunProjector(
        run_id="run-1",
        bot_id="bot-1",
        mailbox=RunMailbox(run_id="run-1", bot_id="bot-1"),
        fanout_channel=__import__("asyncio").Queue(),
        fanout_queue_metrics=_queue_owner(
            key="fanout_channel:run-1",
            depth_metric="fanout_queue_depth",
            utilization_metric="fanout_queue_utilization",
            oldest_age_metric="fanout_queue_oldest_age_ms",
            bot_id="bot-1",
            run_id="run-1",
            queue_name="fanout_channel",
        ),
        on_evict=lambda _run_id: __import__("asyncio").sleep(0),
    )

    await projector._load_initial_state()

    snapshot = projector.get_snapshot()
    assert snapshot.health.status == "projection_error"
    assert snapshot.readiness.catalog_discovered is False
    assert snapshot.faults.faults[0]["fault_code"] == "projection_error"
    event = next(event for event in get_observability_sink().snapshot()["events"] if event["name"] == "ledger_rebuild_failed")
    assert event["context"]["projection_state"] == "projection_error"


def test_record_bot_runtime_event_seq_collision_emits_event(monkeypatch: pytest.MonkeyPatch) -> None:
    existing = _FakeViewStateRow(
        {
            "event_id": "existing-event",
            "seq": 7,
        }
    )
    fake_db = _FakeDb([[existing], None])
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
async def test_symbol_projector_emits_apply_metrics_without_persistence_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        _bootstrap_batch(
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
    )

    metric_names = {metric["metric_name"] for metric in get_observability_sink().snapshot()["metrics"]}
    assert "symbol_projector_apply_ms" in metric_names
    assert "persistence_wait_ms" not in metric_names


@pytest.mark.asyncio
async def test_symbol_projector_runtime_event_no_longer_persists_typed_delta_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fanout = __import__("asyncio").Queue()

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
        _bootstrap_batch(
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
    )

    envelope = fanout.get_nowait()
    assert "typed_delta_metrics" not in str(envelope.item.deltas)
