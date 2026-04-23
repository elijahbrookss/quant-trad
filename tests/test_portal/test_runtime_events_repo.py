from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import time

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy.dialects import postgresql

from portal.backend.service.storage.repos import runtime_events
from portal.backend.service.storage.repos import _shared as storage_shared
from portal.backend.service.observability import get_observability_sink, reset_observability_sink


class _FakeScalarResult:
    def __init__(self, value):
        self._value = value

    def scalars(self):
        return self

    def mappings(self):
        return self

    def first(self):
        if isinstance(self._value, list):
            return self._value[0] if self._value else None
        return self._value

    def all(self):
        if self._value is None:
            return []
        if not isinstance(self._value, list):
            return [self._value]
        return list(self._value)


class _FakeViewStateRow:
    def __init__(self, payload):
        self._payload = dict(payload)
        for key, value in self._payload.items():
            setattr(self, key, value)

    def to_dict(self):
        return dict(self._payload)


class _SequencedSession:
    def __init__(self, values):
        self._values = list(values)
        self.execute_calls = 0
        self.added = []
        self.statements = []

    def execute(self, _stmt):
        if not self._values:
            raise AssertionError("session requires at least one result")
        index = min(self.execute_calls, len(self._values) - 1)
        value = self._values[index]
        self.execute_calls += 1
        self.statements.append(_stmt)
        if isinstance(value, Exception):
            raise value
        return _FakeScalarResult(value)

    def add(self, row):
        self.added.append(row)

    def add_all(self, rows):
        self.added.extend(list(rows))

    def flush(self):
        return None


class _FakeDb:
    available = True

    def __init__(self, values):
        self.session_handle = _SequencedSession(values)

    @contextmanager
    def session(self):
        yield self.session_handle


def _statement_sql(statement) -> str:
    return str(statement.compile(dialect=postgresql.dialect()))


def _compiled_param(params: dict, prefix: str):
    matches = [value for key, value in params.items() if str(key).startswith(prefix)]
    assert matches, f"missing compiled param prefix={prefix}"
    return matches[0]


def _domain_row(*, row_id: int, event_id: str, seq: int, event_name: str, context: dict, event_type: str | None = None):
    normalized_context = dict(context)
    if event_name == "HEALTH_STATUS_REPORTED" and "warning_types" not in normalized_context:
        normalized_context["warning_types"] = ["runtime"]
    candle = normalized_context.get("candle") if isinstance(normalized_context.get("candle"), dict) else {}
    return _FakeViewStateRow(
        {
            "id": row_id,
            "event_id": event_id,
            "seq": seq,
            "event_type": event_type or f"botlens_domain.{event_name.lower()}",
            "event_name": event_name,
            "series_key": normalized_context.get("series_key"),
            "root_id": event_id,
            "correlation_id": f"corr-{row_id}",
            "instrument_id": normalized_context.get("instrument_id"),
            "symbol": normalized_context.get("symbol"),
            "timeframe": normalized_context.get("timeframe"),
            "signal_id": normalized_context.get("signal_id"),
            "decision_id": normalized_context.get("decision_id"),
            "trade_id": normalized_context.get("trade_id"),
            "reason_code": normalized_context.get("reason_code"),
            "bar_time": normalized_context.get("bar_time") or candle.get("time"),
            "payload": {
                "schema_version": 1,
                "event_id": event_id,
                "event_ts": "2026-02-01T00:00:00Z",
                "event_name": event_name,
                "root_id": event_id,
                "parent_id": None,
                "correlation_id": f"corr-{row_id}",
                "context": {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    **normalized_context,
                },
            },
        }
    )


def test_get_latest_bot_runtime_run_id_prefers_latest_run_row(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_db = _FakeDb(["run-new", "run-from-events"])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    result = runtime_events.get_latest_bot_runtime_run_id("bot-1")

    assert result == "run-new"


def test_get_latest_bot_runtime_run_id_falls_back_to_event_row_when_run_row_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb([None, "run-from-events", "run-from-events"])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    result = runtime_events.get_latest_bot_runtime_run_id("bot-1")

    assert result == "run-from-events"


def test_record_bot_runtime_events_batch_records_observation_started_timer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _AvailableDb:
        available = True

    observed: dict[str, object] = {}

    def _fake_execute_write_with_retry(**_kwargs):
        return runtime_events.StorageWriteOutcome(
            result=1,
            rows_written=1,
            payload_bytes=7,
        )

    def _fake_observe_db_write_outcome(*, storage_target, context, started, outcome):
        observed["storage_target"] = storage_target
        observed["context"] = context
        observed["started"] = started
        observed["outcome"] = outcome

    monkeypatch.setattr(runtime_events, "db", _AvailableDb())
    monkeypatch.setattr(runtime_events, "_execute_write_with_retry", _fake_execute_write_with_retry)
    monkeypatch.setattr(runtime_events, "_observe_db_write_outcome", _fake_observe_db_write_outcome)

    result = runtime_events.record_bot_runtime_events_batch(
        [
            {
                "event_id": "evt-1",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 1,
                "event_type": "series_bar.telemetry",
                "payload": {"bar_index": 1},
            }
        ]
    )

    assert result == 1
    assert observed["storage_target"] == "bot_runtime_events"
    assert observed["context"] == {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "event_id": "evt-1",
        "series_key": None,
        "worker_id": None,
        "message_kind": "series_bar.telemetry",
        "pipeline_stage": "runtime_event_persist",
        "batch_size": 1,
        "event_name": None,
        "source_emitter": None,
        "source_reason": "ingest",
        "conflict_strategy": "seq_guard_then_insert_on_conflict_do_nothing",
        "conflict_target_name": "uq_portal_bot_run_events_event_id",
        "write_contract": "insert_first_event_id_dedupe",
        "precheck_mode": "seq_guard_only",
    }
    assert isinstance(observed["started"], float)
    assert observed["started"] >= 0.0


def test_record_bot_runtime_events_batch_emits_ingest_candle_continuity_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_observability_sink()
    fake_db = _FakeDb([[], ["evt-1", "evt-2", "evt-3"]])
    monkeypatch.setattr(runtime_events, "db", fake_db)
    monkeypatch.setattr(runtime_events, "_observe_db_write_outcome", lambda **kwargs: None)

    result = runtime_events.record_bot_runtime_events_batch(
        [
            {
                "event_id": "evt-1",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 1,
                "event_type": "botlens_domain.candle_observed",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-1",
                    "event_ts": "2026-02-01T00:00:00Z",
                    "event_name": "CANDLE_OBSERVED",
                    "root_id": "evt-1",
                    "parent_id": None,
                    "correlation_id": "corr-1",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "candle": {"time": "2026-02-01T00:00:00Z", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
                    },
                },
            },
            {
                "event_id": "evt-2",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 2,
                "event_type": "botlens_domain.candle_observed",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-2",
                    "event_ts": "2026-02-01T00:01:00Z",
                    "event_name": "CANDLE_OBSERVED",
                    "root_id": "evt-2",
                    "parent_id": None,
                    "correlation_id": "corr-2",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "candle": {"time": "2026-02-01T00:01:00Z", "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
                    },
                },
            },
            {
                "event_id": "evt-3",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 3,
                "event_type": "botlens_domain.candle_observed",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-3",
                    "event_ts": "2026-02-01T00:03:00Z",
                    "event_name": "CANDLE_OBSERVED",
                    "root_id": "evt-3",
                    "parent_id": None,
                    "correlation_id": "corr-3",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "candle": {"time": "2026-02-01T00:03:00Z", "open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5},
                    },
                },
            },
        ],
        context={
            "series_key": "instrument-btc|1m",
            "message_kind": "botlens_runtime_facts",
            "pipeline_stage": "botlens_ingest_facts",
            "source_reason": "ingest",
        },
    )

    assert result == 3
    snapshot = get_observability_sink().snapshot()
    gap_metric = next(
        metric
        for metric in snapshot["metrics"]
        if metric["metric_name"] == "candle_continuity_gap_count"
    )
    assert gap_metric["value"] == 1.0
    assert gap_metric["tags"]["pipeline_stage"] == "botlens_ingest_admission"
    assert gap_metric["tags"]["series_key"] == "instrument-btc|1m"
    event = next(event for event in snapshot["events"] if event["name"] == "candle_continuity_summary")
    assert event["context"]["boundary_name"] == "ingest_admission"
    assert event["context"]["storage_target"] == "bot_runtime_events"
    assert event["context"]["detected_gap_count"] == 1


def test_record_bot_runtime_events_batch_observes_compacted_botlens_domain_payload_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb([[], ["evt-1"]])
    observed: dict[str, object] = {}

    def _capture_outcome(*, storage_target, context, started, outcome, error=None):
        observed["storage_target"] = storage_target
        observed["context"] = context
        observed["outcome"] = outcome
        observed["error"] = error

    monkeypatch.setattr(runtime_events, "db", fake_db)
    monkeypatch.setattr(runtime_events, "_observe_db_write_outcome", _capture_outcome)

    result = runtime_events.record_bot_runtime_events_batch(
        [
            {
                "event_id": "evt-1",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 1,
                "event_type": "botlens_domain.overlay_state_changed",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-1",
                    "event_ts": "2026-02-01T00:00:00Z",
                    "event_name": "OVERLAY_STATE_CHANGED",
                    "root_id": "evt-1",
                    "parent_id": None,
                    "correlation_id": "corr-1",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "overlay_delta": {
                            "seq": 4,
                            "base_seq": 3,
                            "ops": [
                                {
                                    "op": "upsert",
                                    "key": "overlay-1",
                                    "overlay": {
                                        "overlay_id": "overlay-1",
                                        "type": "regime_overlay",
                                        "pane_key": "volatility",
                                        "pane_views": ["polyline"],
                                        "payload": {
                                            "polylines": [
                                                {
                                                    "points": [
                                                        {"time": index, "price": float(index)}
                                                        for index in range(2000)
                                                    ]
                                                }
                                            ]
                                        },
                                    },
                                }
                            ],
                        },
                    },
                },
            }
        ],
        context={
            "series_key": "instrument-btc|1m",
            "worker_id": "worker-1",
            "message_kind": "botlens_runtime_facts",
            "pipeline_stage": "botlens_ingest_facts",
            "source_emitter": "container_runtime",
            "source_reason": "ingest",
        },
    )

    outcome = observed["outcome"]
    assert result == 1
    assert observed["storage_target"] == "bot_runtime_events"
    assert observed["context"]["source_reason"] == "ingest"
    assert observed["context"]["source_emitter"] == "container_runtime"
    assert outcome.attempted_rows == 1
    assert outcome.inserted_rows == 1
    assert outcome.duplicate_rows == 0
    assert outcome.payload_size_bucket != "large"
    assert outcome.has_large_payload is False
    assert outcome.largest_json_field_name != "payload.context.overlay_delta"
    assert outcome.largest_json_field_bytes < 8000
    assert outcome.bytes_per_row > 0
    assert observed["error"] is None


def test_observe_db_write_outcome_merges_overlapping_event_fields_without_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeObserver:
        def __init__(self) -> None:
            self.events: list[tuple[str, dict[str, object]]] = []

        def increment(self, *_args, **_kwargs) -> None:
            return None

        def observe(self, *_args, **_kwargs) -> None:
            return None

        def event(self, name, **fields) -> None:
            self.events.append((name, dict(fields)))

    observer = _FakeObserver()
    monkeypatch.setattr(storage_shared, "_STORAGE_OBSERVER", observer)
    monkeypatch.setattr(storage_shared, "_DB_SLOW_MS", 0.0)

    storage_shared._observe_db_write_outcome(
        storage_target="bot_runtime_events",
        context={
            "bot_id": "bot-1",
            "run_id": "run-1",
            "series_key": "instrument-btc|1m",
            "worker_id": "worker-1",
            "pipeline_stage": "botlens_ingest_facts",
            "message_kind": "botlens_runtime_facts",
            "source_reason": "ingest",
            "batch_size": 3,
            "conflict_target_name": "bot_run_seq_guard",
        },
        started=time.perf_counter() - 1.0,
        outcome=storage_shared.StorageWriteOutcome(
            result=3,
            rows_written=3,
            attempted_rows=3,
            inserted_rows=3,
            payload_bytes=70_000,
            payload_size_bucket="large",
            largest_json_field_name="payload.context.overlay_delta",
            largest_json_field_bytes=70_000,
        ),
    )

    event_names = [name for name, _fields in observer.events]
    assert "db_write_observed" in event_names
    assert "db_write_slow" in event_names

    observed_fields = next(fields for name, fields in observer.events if name == "db_write_observed")
    assert observed_fields["payload_size_bucket"] == "large"
    assert observed_fields["source_reason"] == "ingest"

    slow_fields = next(fields for name, fields in observer.events if name == "db_write_slow")
    assert slow_fields["payload_size_bucket"] == "large"
    assert slow_fields["storage_target"] == "bot_runtime_events"


def test_record_bot_runtime_events_batch_classifies_duplicate_reason_from_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = _FakeViewStateRow(
        {
            "event_id": "evt-1",
            "seq": 1,
            "event_type": "botlens_domain.candle_observed",
        }
    )
    fake_db = _FakeDb([[existing], []])
    observed: dict[str, object] = {}

    def _capture_outcome(*, storage_target, context, started, outcome, error=None):
        observed["storage_target"] = storage_target
        observed["context"] = context
        observed["outcome"] = outcome
        observed["error"] = error

    monkeypatch.setattr(runtime_events, "db", fake_db)
    monkeypatch.setattr(runtime_events, "_observe_db_write_outcome", _capture_outcome)

    result = runtime_events.record_bot_runtime_events_batch(
        [
            {
                "event_id": "evt-1",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 1,
                "event_type": "botlens_domain.candle_observed",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-1",
                    "event_ts": "2026-02-01T00:00:00Z",
                    "event_name": "CANDLE_OBSERVED",
                    "root_id": "evt-1",
                    "parent_id": None,
                    "correlation_id": "corr-1",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "candle": {"time": "2026-02-01T00:00:00Z", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
                    },
                },
            }
        ],
        context={
            "series_key": "instrument-btc|1m",
            "message_kind": "botlens_runtime_bootstrap_facts",
            "pipeline_stage": "botlens_ingest_bootstrap",
            "source_emitter": "container_runtime",
            "source_reason": "bootstrap",
        },
    )

    outcome = observed["outcome"]
    assert result == 0
    assert outcome.inserted_rows == 0
    assert outcome.duplicate_rows == 1
    assert outcome.duplicate_reasons == {"bootstrap_reemit_duplicate": 1}
    assert observed["context"]["conflict_target_name"] == "uq_portal_bot_run_events_event_id"
    assert observed["error"] is None


def test_record_bot_runtime_events_batch_counts_same_batch_event_id_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb([[], ["evt-dup"]])
    observed: dict[str, object] = {}

    def _capture_outcome(*, storage_target, context, started, outcome, error=None):
        observed["outcome"] = outcome
        observed["error"] = error

    monkeypatch.setattr(runtime_events, "db", fake_db)
    monkeypatch.setattr(runtime_events, "_observe_db_write_outcome", _capture_outcome)

    result = runtime_events.record_bot_runtime_events_batch(
        [
            {
                "event_id": "evt-dup",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 1,
                "event_type": "botlens_domain.health_status_reported",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-dup",
                    "event_ts": "2026-02-01T00:00:00Z",
                    "event_name": "HEALTH_STATUS_REPORTED",
                    "root_id": "evt-dup",
                    "parent_id": None,
                    "correlation_id": "corr-1",
                        "context": {"run_id": "run-1", "bot_id": "bot-1", "status": "running", "warning_types": ["runtime"]},
                },
            },
            {
                "event_id": "evt-dup",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 2,
                "event_type": "botlens_domain.health_status_reported",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-dup",
                    "event_ts": "2026-02-01T00:00:01Z",
                    "event_name": "HEALTH_STATUS_REPORTED",
                    "root_id": "evt-dup",
                    "parent_id": None,
                    "correlation_id": "corr-2",
                        "context": {"run_id": "run-1", "bot_id": "bot-1", "status": "running", "warning_types": ["runtime"]},
                },
            },
        ],
        context={"message_kind": "botlens_runtime_facts", "source_reason": "ingest"},
    )

    outcome = observed["outcome"]
    assert result == 1
    assert outcome.attempted_rows == 2
    assert outcome.inserted_rows == 1
    assert outcome.duplicate_rows == 1
    assert outcome.duplicate_reasons == {"same_batch_event_id_duplicate": 1}
    assert observed["error"] is None


def test_record_bot_runtime_event_uses_conflict_insert_without_event_id_precheck(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb(
        [
            [],
            [
                {
                    "id": 41,
                    "event_id": "evt-1",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "seq": 7,
                    "event_type": "botlens_domain.candle_observed",
                    "critical": False,
                    "schema_version": 1,
                    "payload": {
                        "event_name": "CANDLE_OBSERVED",
                        "context": {"series_key": "instrument-btc|1m"},
                    },
                    "event_name": "CANDLE_OBSERVED",
                    "series_key": "instrument-btc|1m",
                    "known_at": "2026-02-01T00:00:00Z",
                    "created_at": "2026-02-01T00:00:00Z",
                }
            ],
        ]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    result = runtime_events.record_bot_runtime_event(
        {
            "event_id": "evt-1",
            "bot_id": "bot-1",
            "run_id": "run-1",
            "seq": 7,
            "event_type": "botlens_domain.candle_observed",
            "schema_version": 1,
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-1",
                    "event_name": "CANDLE_OBSERVED",
                    "event_ts": "2026-02-01T00:00:00Z",
                    "root_id": "root-1",
                    "parent_id": None,
                    "correlation_id": "corr-1",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                    "series_key": "instrument-btc|1m",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                    "candle": {"time": "2026-02-01T00:00:00Z", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
                },
            },
        }
    )

    select_sql = _statement_sql(fake_db.session_handle.statements[0])
    insert_sql = _statement_sql(fake_db.session_handle.statements[1])
    assert "portal_bot_run_events.event_id = " not in select_sql
    assert "portal_bot_run_events.seq IN" in select_sql
    assert "INSERT INTO portal_bot_run_events" in insert_sql
    assert "ON CONFLICT ON CONSTRAINT uq_portal_bot_run_events_event_id DO NOTHING" in insert_sql
    assert result["event_id"] == "evt-1"


def test_record_bot_runtime_events_batch_fieldizes_hot_columns_in_insert_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb([[], ["evt-1", "evt-2"]])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    result = runtime_events.record_bot_runtime_events_batch(
        [
            {
                "event_id": "evt-1",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 8,
                "event_type": "botlens_domain.decision_emitted",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-1",
                    "event_ts": "2026-02-01T00:01:00Z",
                    "event_name": "DECISION_EMITTED",
                    "root_id": "root-1",
                    "parent_id": None,
                    "correlation_id": "corr-1",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "bar_time": "2026-02-01T00:01:00Z",
                        "decision_state": "rejected",
                        "decision_id": "decision-1",
                        "signal_id": "signal-1",
                        "reason_code": "rule_blocked",
                        "message": "rule blocked",
                    },
                },
            },
            {
                "event_id": "evt-2",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 9,
                "event_type": "botlens_domain.trade_opened",
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-2",
                    "event_ts": "2026-02-01T00:02:00Z",
                    "event_name": "TRADE_OPENED",
                    "root_id": "root-2",
                    "parent_id": None,
                    "correlation_id": "corr-2",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "trade_id": "trade-1",
                        "bar_time": "2026-02-01T00:02:00Z",
                        "event_time": "2026-02-01T00:02:00Z",
                        "trade_state": "open",
                        "direction": "long",
                    },
                },
            },
        ]
    )

    select_sql = _statement_sql(fake_db.session_handle.statements[0])
    compiled = fake_db.session_handle.statements[1].compile(dialect=postgresql.dialect())
    assert "portal_bot_run_events.event_id = " not in select_sql
    assert "portal_bot_run_events.seq IN" in select_sql
    assert result == 2
    assert _compiled_param(compiled.params, "event_name_m0") == "DECISION_EMITTED"
    assert _compiled_param(compiled.params, "series_key_m0") == "instrument-btc|1m"
    assert _compiled_param(compiled.params, "correlation_id_m0") == "corr-1"
    assert _compiled_param(compiled.params, "root_id_m0") == "root-1"
    assert _compiled_param(compiled.params, "signal_id_m0") == "signal-1"
    assert _compiled_param(compiled.params, "decision_id_m0") == "decision-1"
    assert _compiled_param(compiled.params, "reason_code_m0") == "rule_blocked"
    assert _compiled_param(compiled.params, "trade_id_m1") == "trade-1"


def test_runtime_event_row_values_reject_signal_id_aliasing_decision_id() -> None:
    with pytest.raises(ValueError, match="signal_id != decision_id"):
        runtime_events._runtime_event_row_values(
            event_id="evt-1",
            bot_id="bot-1",
            run_id="run-1",
            seq=8,
            event_type="botlens_domain.signal_emitted",
            critical=False,
            schema_version=1,
            payload={
                "schema_version": 1,
                "event_id": "evt-1",
                "event_ts": "2026-02-01T00:01:00Z",
                "event_name": "SIGNAL_EMITTED",
                "root_id": "root-1",
                "correlation_id": "corr-1",
                "context": {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "series_key": "instrument-btc|1m",
                    "signal_id": "decision-1",
                    "decision_id": "decision-1",
                    "signal_type": "strategy_signal",
                    "direction": "long",
                    "signal_price": 100.0,
                    "bar_epoch": 1700000000,
                },
            },
            event_time=datetime(2026, 2, 1, 0, 1, tzinfo=timezone.utc),
            known_at=datetime(2026, 2, 1, 0, 1, tzinfo=timezone.utc),
        )


def test_botlens_trade_hot_fields_use_context_bar_time_only() -> None:
    hot_fields = runtime_events._runtime_event_payload_hot_fields(
        event_type="botlens_domain.trade_opened",
        payload={
            "event_name": "TRADE_OPENED",
            "event_ts": "2026-02-01T00:05:00Z",
            "known_at": "2026-04-25T07:36:35Z",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "series_key": "instrument-btc|1m",
                "trade_id": "trade-1",
                "bar_time": "2026-02-01T00:05:00Z",
                "event_time": "2026-02-01T00:05:00Z",
            },
        },
    )

    assert hot_fields["trade_id"] == "trade-1"
    assert hot_fields["bar_time"] == datetime(2026, 2, 1, 0, 5)


def test_botlens_trade_hot_fields_do_not_fallback_to_wall_clock_known_at() -> None:
    hot_fields = runtime_events._runtime_event_payload_hot_fields(
        event_type="botlens_domain.trade_opened",
        payload={
            "event_name": "TRADE_OPENED",
            "event_ts": "2026-02-01T00:05:00Z",
            "known_at": "2026-04-25T07:36:35Z",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "series_key": "instrument-btc|1m",
                "trade_id": "trade-1",
            },
        },
    )

    assert hot_fields["trade_id"] == "trade-1"
    assert hot_fields["bar_time"] is None


def test_botlens_decision_hot_fields_preserve_entry_request_identity() -> None:
    hot_fields = runtime_events._runtime_event_payload_hot_fields(
        event_type="botlens_domain.decision_emitted",
        payload={
            "event_name": "DECISION_EMITTED",
            "event_ts": "2026-02-01T00:05:00Z",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "series_key": "instrument-btc|1m",
                "decision_state": "rejected",
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "entry_request_id": "entry_request:abc",
                "attempt_id": "entry_request:abc",
                "reason_code": "WALLET_INSUFFICIENT_MARGIN",
            },
        },
    )

    assert hot_fields["trade_id"] is None
    assert hot_fields["entry_request_id"] == "entry_request:abc"
    assert hot_fields["attempt_id"] == "entry_request:abc"
    assert hot_fields["reason_code"] == "WALLET_INSUFFICIENT_MARGIN"


def test_list_bot_runtime_events_queries_typed_columns_for_hot_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb([[]])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    rows = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        event_names=["SIGNAL_EMITTED"],
        series_key="instrument-btc|1m",
        correlation_id="corr-1",
        signal_id="signal-1",
        bar_time_gte="2026-02-01T00:00:00Z",
        bar_time_lt="2026-02-01T00:05:00Z",
    )

    sql = _statement_sql(fake_db.session_handle.statements[0])
    assert rows == []
    assert "portal_bot_run_events.series_key = " in sql
    assert "portal_bot_run_events.event_name IN" in sql
    assert "portal_bot_run_events.correlation_id = " in sql
    assert "portal_bot_run_events.signal_id = " in sql
    assert "portal_bot_run_events.bar_time >= " in sql
    assert "portal_bot_run_events.bar_time < " in sql
    assert "payload ->>" not in sql
    assert "payload #>>" not in sql


def test_list_bot_runtime_events_does_not_backfill_typed_hot_fields_from_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb(
        [[
            _FakeViewStateRow(
                {
                    "event_id": "evt-1",
                    "seq": 4,
                    "payload": {
                        "series_key": "instrument-btc|1M",
                        "bridge_session_id": "bridge-1",
                        "bridge_seq": "7",
                        "run_seq": "9",
                        "instrument_id": "instrument-btc",
                        "event_name": "CANDLE_OBSERVED",
                        "correlation_id": "corr-1",
                    },
                }
            )
        ]]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    rows = runtime_events.list_bot_runtime_events(bot_id="bot-1", run_id="run-1")

    assert rows == [
        {
            "event_id": "evt-1",
            "seq": 4,
            "payload": {
                "series_key": "instrument-btc|1M",
                "bridge_session_id": "bridge-1",
                "bridge_seq": "7",
                "run_seq": "9",
                "instrument_id": "instrument-btc",
                "event_name": "CANDLE_OBSERVED",
                "correlation_id": "corr-1",
            },
            "bridge_session_id": "bridge-1",
            "bridge_seq": 7,
            "run_seq": 9,
        }
    ]


def test_list_bot_runtime_events_projects_domain_context_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb(
        [[
            _FakeViewStateRow(
                {
                    "event_id": "evt-1",
                    "event_type": "botlens_domain.candle_observed",
                    "seq": 4,
                    "event_name": "CANDLE_OBSERVED",
                    "series_key": "instrument-btc|1m",
                    "root_id": "evt-1",
                    "correlation_id": "corr-1",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1m",
                    "bar_time": "2026-01-01T00:00:00Z",
                    "payload": {
                        "schema_version": 1,
                        "event_id": "evt-1",
                        "event_ts": "2026-01-01T00:00:00Z",
                        "event_name": "CANDLE_OBSERVED",
                        "root_id": "evt-1",
                        "parent_id": None,
                        "correlation_id": "corr-1",
                        "context": {
                            "run_id": "run-1",
                            "bot_id": "bot-1",
                            "series_key": "instrument-btc|1M",
                            "instrument_id": "instrument-btc",
                            "symbol": "BTC",
                            "timeframe": "1M",
                            "candle": {"time": "2026-01-01T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.5},
                        },
                    },
                }
            )
        ]]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    rows = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        canonicalize_botlens_payloads=True,
    )
    assert len(rows) == 1
    observed = rows[0]
    assert observed["event_id"] == "evt-1"
    assert observed["event_type"] == "botlens_domain.candle_observed"
    assert observed["seq"] == 4
    assert observed["series_key"] == "instrument-btc|1m"
    assert observed["event_name"] == "CANDLE_OBSERVED"
    assert observed["instrument_id"] == "instrument-btc"
    assert observed["symbol"] == "BTC"
    assert observed["timeframe"] == "1m"
    assert observed["bar_time"] == "2026-01-01T00:00:00Z"
    assert observed["root_id"] == "evt-1"
    assert observed["correlation_id"] == "corr-1"
    assert observed["payload"]["context"]["series_key"] == "instrument-btc|1m"


def test_list_bot_runtime_events_does_not_match_legacy_payload_only_hot_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb([[]])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    rows = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        series_key="instrument-btc|1m",
        signal_id="signal-1",
    )

    assert rows == []


def test_bot_run_event_record_declares_runtime_event_hot_indexes() -> None:
    index_names = {index.name for index in storage_shared.BotRunEventRecord.__table__.indexes}

    assert storage_shared.REQUIRED_BOT_RUN_EVENT_INDEXES <= index_names


def test_list_bot_runtime_events_preserves_bounded_overlay_render_payloads_on_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb(
        [[
            _FakeViewStateRow(
                {
                    "event_id": "evt-overlay",
                    "event_type": "botlens_domain.overlay_state_changed",
                    "seq": 4,
                    "payload": {
                        "schema_version": 1,
                        "event_id": "evt-overlay",
                        "event_ts": "2026-01-01T00:00:00Z",
                        "event_name": "OVERLAY_STATE_CHANGED",
                        "root_id": "evt-overlay",
                        "parent_id": None,
                        "correlation_id": "corr-overlay",
                        "context": {
                            "run_id": "run-1",
                            "bot_id": "bot-1",
                            "series_key": "instrument-btc|1m",
                            "overlay_delta": {
                                "seq": 9,
                                "base_seq": 8,
                                "ops": [
                                    {
                                        "op": "upsert",
                                        "key": "overlay-1",
                                        "overlay": {
                                            "overlay_id": "overlay-1",
                                            "type": "regime_overlay",
                                            "pane_key": "volatility",
                                            "pane_views": ["polyline"],
                                            "payload": {
                                                "polylines": [
                                                    {
                                                        "points": [
                                                            {"time": 1, "price": 100.0},
                                                            {"time": 2, "price": 101.0},
                                                        ]
                                                    }
                                                ]
                                            },
                                        },
                                    }
                                ],
                            },
                        },
                    },
                }
            )
        ]]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    rows = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        canonicalize_botlens_payloads=True,
    )
    overlay = rows[0]["payload"]["context"]["overlay_delta"]["ops"][0]["overlay"]

    assert overlay["detail_level"] == "bounded_render"
    assert overlay["pane_key"] == "volatility"
    assert overlay["pane_views"] == ["polyline"]
    assert overlay["payload"]["polylines"][0]["points"] == [
        {"time": 1, "price": 100.0},
        {"time": 2, "price": 101.0},
    ]
    assert overlay["payload_summary"] == {
        "geometry_keys": ["polylines"],
        "payload_counts": {"polylines": 1},
        "point_count": 2,
    }


def test_record_bot_runtime_event_allows_duplicate_seq_for_botlens_domain_siblings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = _FakeViewStateRow(
        {
            "event_id": "existing-event",
            "event_type": "botlens_domain.health_status_reported",
            "seq": 7,
        }
    )
    fake_db = _FakeDb(
        [
            [existing],
            [
                {
                    "id": 14,
                    "event_id": "incoming-event",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "seq": 7,
                        "event_type": "botlens_domain.candle_observed",
                        "critical": False,
                        "schema_version": 1,
                        "payload": {
                            "schema_version": 1,
                            "event_id": "incoming-event",
                            "event_ts": "2026-02-01T00:00:00Z",
                            "event_name": "CANDLE_OBSERVED",
                            "root_id": "incoming-event",
                            "parent_id": None,
                            "correlation_id": "corr-1",
                            "context": {
                                "run_id": "run-1",
                                "bot_id": "bot-1",
                                "series_key": "instrument-btc|1m",
                                "candle": {
                                    "time": "2026-02-01T00:00:00Z",
                                    "open": 1.0,
                                    "high": 2.0,
                                    "low": 0.5,
                                    "close": 1.5,
                                },
                            },
                        },
                        "event_name": "CANDLE_OBSERVED",
                        "series_key": "instrument-btc|1m",
                        "known_at": "2026-02-01T00:00:00Z",
                        "created_at": "2026-02-01T00:00:00Z",
                }
            ],
        ]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    result = runtime_events.record_bot_runtime_event(
        {
            "event_id": "incoming-event",
            "bot_id": "bot-1",
            "run_id": "run-1",
                "seq": 7,
                "event_type": "botlens_domain.candle_observed",
                "schema_version": 1,
                "payload": {
                    "schema_version": 1,
                    "event_id": "incoming-event",
                    "event_ts": "2026-02-01T00:00:00Z",
                    "event_name": "CANDLE_OBSERVED",
                    "root_id": "incoming-event",
                    "parent_id": None,
                    "correlation_id": "corr-1",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "candle": {
                            "time": "2026-02-01T00:00:00Z",
                            "open": 1.0,
                            "high": 2.0,
                            "low": 0.5,
                            "close": 1.5,
                        },
                    },
                },
            }
        )

    assert result["event_id"] == "incoming-event"


def test_record_bot_runtime_event_observes_seq_collision_without_secondary_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = _FakeViewStateRow(
        {
            "event_id": "existing-event",
            "event_type": "runtime.trade_filled",
            "seq": 7,
        }
    )
    fake_db = _FakeDb([[existing], None])
    observed: dict[str, list[dict[str, object]]] = {"increments": [], "events": []}

    class _Observer:
        def increment(self, metric_name, **labels):
            observed["increments"].append({"metric_name": metric_name, **labels})

        def event(self, event_name, **payload):
            observed["events"].append({"event_name": event_name, **payload})

    monkeypatch.setattr(runtime_events, "db", fake_db)
    monkeypatch.setattr(runtime_events, "_OBSERVER", _Observer())

    with pytest.raises(ValueError, match="seq collision"):
        runtime_events.record_bot_runtime_event(
            {
                "event_id": "incoming-event",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 7,
                "event_type": "runtime.signal_emitted",
                "schema_version": 1,
                "payload": {
                    "event_name": "SIGNAL_EMITTED",
                    "context": {
                        "series_key": "instrument-btc|1m",
                        "signal_id": "signal-1",
                    },
                },
            }
        )

    assert observed["increments"] == [
        {
            "metric_name": "db_write_fail_total",
            "bot_id": "bot-1",
            "run_id": "run-1",
            "storage_target": "bot_runtime_events",
            "failure_mode": "seq_collision",
        }
    ]
    assert len(observed["events"]) == 1
    assert observed["events"][0]["event_name"] == "db_seq_collision"
    assert observed["events"][0]["level"] == runtime_events.logging.ERROR
    assert observed["events"][0]["failure_mode"] == "seq_collision"
    assert "seq collision" in str(observed["events"][0]["error"])


def test_record_bot_runtime_event_rejects_malformed_botlens_domain_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _AvailableDb:
        available = True

    monkeypatch.setattr(runtime_events, "db", _AvailableDb())

    with pytest.raises(ValueError, match="DIAGNOSTIC_RECORDED context contains unsupported fields: traceback"):
        runtime_events.record_bot_runtime_event(
            {
                "event_id": "evt-1",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 1,
                "event_type": "botlens_domain.diagnostic_recorded",
                "schema_version": 1,
                "payload": {
                    "schema_version": 1,
                    "event_id": "evt-1",
                    "event_ts": "2026-02-01T00:00:00Z",
                    "event_name": "DIAGNOSTIC_RECORDED",
                    "root_id": "evt-1",
                    "parent_id": None,
                    "correlation_id": "corr-1",
                    "context": {
                        "run_id": "run-1",
                        "bot_id": "bot-1",
                        "series_key": "instrument-btc|1m",
                        "level": "WARN",
                        "message": "bad diagnostic",
                        "traceback": "must-not-persist",
                    },
                },
            }
        )


def test_record_bot_runtime_event_rejects_decision_ledger_shape_as_botlens_truth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _AvailableDb:
        available = True

    monkeypatch.setattr(runtime_events, "db", _AvailableDb())

    with pytest.raises(ValueError, match="unsupported fields: event_type, reason_code"):
        runtime_events.record_bot_runtime_event(
            {
                "event_id": "evt-legacy",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "seq": 1,
                "event_type": "botlens_domain.signal_emitted",
                "schema_version": 1,
                "payload": {
                    "event_type": "signal_accepted",
                    "reason_code": "accepted",
                    "event_ts": "2026-02-01T00:00:00Z",
                },
            }
        )


def test_list_bot_runtime_events_filters_runtime_rows_when_querying_botlens_domain_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb(
        [[
            _FakeViewStateRow(
                {
                    "id": 901,
                    "event_id": "evt-runtime",
                    "event_type": "runtime.signal_emitted",
                    "seq": 9,
                    "payload": {
                        "event_name": "SIGNAL_EMITTED",
                        "context": {"series_key": "instrument-btc|1m"},
                    },
                }
            ),
            _domain_row(
                row_id=902,
                event_id="evt-domain",
                seq=10,
                event_name="SIGNAL_EMITTED",
                context={
                    "series_key": "instrument-btc|1m",
                    "signal_id": "signal-1",
                    "decision_id": "decision-1",
                    "signal_type": "strategy_signal",
                    "direction": "long",
                    "signal_price": 100.0,
                    "bar_epoch": 1700000000,
                },
            ),
        ]]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    rows = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        event_type_prefixes=["botlens_domain."],
    )

    assert [row["event_id"] for row in rows] == ["evt-domain"]


def test_list_bot_runtime_events_filters_before_page_slicing_across_mixed_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb(
        [
            [
                _domain_row(row_id=401, event_id="evt-health", seq=1, event_name="HEALTH_STATUS_REPORTED", context={"status": "running"}),
                _domain_row(
                    row_id=402,
                    event_id="evt-signal-1",
                    seq=2,
                    event_name="SIGNAL_EMITTED",
                    context={
                        "series_key": "instrument-btc|1m",
                        "signal_id": "signal-1",
                        "decision_id": "decision-1",
                        "signal_type": "strategy_signal",
                        "direction": "long",
                        "signal_price": 100.0,
                        "bar_epoch": 1700000000,
                    },
                ),
            ],
            [
                _domain_row(
                    row_id=403,
                    event_id="evt-candle",
                    seq=3,
                    event_name="CANDLE_OBSERVED",
                    context={
                        "series_key": "instrument-btc|1m",
                        "candle": {"time": "2026-02-01T00:01:00Z", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
                    },
                ),
                _domain_row(
                    row_id=404,
                    event_id="evt-signal-2",
                    seq=4,
                    event_name="SIGNAL_EMITTED",
                    context={
                        "series_key": "instrument-btc|1m",
                        "signal_id": "signal-2",
                        "decision_id": "decision-2",
                        "signal_type": "strategy_signal",
                        "direction": "short",
                        "signal_price": 99.0,
                        "bar_epoch": 1700000060,
                    },
                ),
            ],
        ]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    first = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        limit=1,
        event_names=["SIGNAL_EMITTED"],
    )
    second = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        after_seq=int(first[-1]["seq"]),
        after_row_id=int(first[-1]["id"]),
        limit=1,
        event_names=["SIGNAL_EMITTED"],
    )

    assert [row["id"] for row in first] == [402]
    assert [row["id"] for row in second] == [404]


def test_list_bot_runtime_events_cursor_tracks_filtered_same_seq_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb(
        [
            [
                _domain_row(row_id=701, event_id="evt-health", seq=7, event_name="HEALTH_STATUS_REPORTED", context={"status": "running"}),
                _domain_row(
                    row_id=702,
                    event_id="evt-signal-1",
                    seq=7,
                    event_name="SIGNAL_EMITTED",
                    context={
                        "series_key": "instrument-btc|1m",
                        "signal_id": "signal-1",
                        "decision_id": "decision-1",
                        "signal_type": "strategy_signal",
                        "direction": "long",
                        "signal_price": 100.0,
                        "bar_epoch": 1700000000,
                    },
                ),
                _domain_row(
                    row_id=703,
                    event_id="evt-signal-2",
                    seq=7,
                    event_name="SIGNAL_EMITTED",
                    context={
                        "series_key": "instrument-btc|1m",
                        "signal_id": "signal-2",
                        "decision_id": "decision-2",
                        "signal_type": "strategy_signal",
                        "direction": "short",
                        "signal_price": 99.0,
                        "bar_epoch": 1700000060,
                    },
                ),
            ],
            [
                _domain_row(
                    row_id=703,
                    event_id="evt-signal-2",
                    seq=7,
                    event_name="SIGNAL_EMITTED",
                    context={
                        "series_key": "instrument-btc|1m",
                        "signal_id": "signal-2",
                        "decision_id": "decision-2",
                        "signal_type": "strategy_signal",
                        "direction": "short",
                        "signal_price": 99.0,
                        "bar_epoch": 1700000060,
                    },
                ),
            ],
            [
                _domain_row(row_id=704, event_id="evt-health-2", seq=8, event_name="HEALTH_STATUS_REPORTED", context={"status": "running"}),
            ],
            [],
        ]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    first = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        limit=1,
        event_names=["SIGNAL_EMITTED"],
    )
    second = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        after_seq=int(first[-1]["seq"]),
        after_row_id=int(first[-1]["id"]),
        limit=1,
        event_names=["SIGNAL_EMITTED"],
    )
    exhausted = runtime_events.list_bot_runtime_events(
        bot_id="bot-1",
        run_id="run-1",
        after_seq=int(second[-1]["seq"]),
        after_row_id=int(second[-1]["id"]),
        limit=1,
        event_names=["SIGNAL_EMITTED"],
    )

    assert [row["id"] for row in first] == [702]
    assert [row["id"] for row in second] == [703]
    assert exhausted == []


@pytest.mark.parametrize(
    ("event_name", "context"),
    [
        ("CANDLE_OBSERVED", {"candle": {"time": "2026-02-01T00:00:00Z", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}}),
        (
            "SIGNAL_EMITTED",
            {
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "signal_type": "strategy_signal",
                "direction": "long",
                "signal_price": 100.0,
                "bar_epoch": 1700000000,
            },
        ),
        (
            "DECISION_EMITTED",
            {
                "decision_state": "accepted",
                "signal_id": "signal-1",
                "decision_id": "decision-1",
                "direction": "long",
                "signal_price": 100.0,
                "bar_epoch": 1700000000,
            },
        ),
        (
            "TRADE_OPENED",
            {
                "trade_id": "trade-1",
                "trade_state": "open",
                "direction": "long",
            },
        ),
    ],
)
def test_list_bot_runtime_events_rejects_missing_series_key_on_persisted_series_rows(
    monkeypatch: pytest.MonkeyPatch,
    event_name: str,
    context: dict,
) -> None:
    fake_db = _FakeDb([[_domain_row(row_id=801, event_id="evt-bad", seq=8, event_name=event_name, context=context)]])
    monkeypatch.setattr(runtime_events, "db", fake_db)

    with pytest.raises(ValueError, match=fr"context.series_key is required for {event_name}"):
        runtime_events.list_bot_runtime_events(
            bot_id="bot-1",
            run_id="run-1",
            limit=1,
            canonicalize_botlens_payloads=True,
        )


def test_list_bot_runtime_events_leaves_invalid_botlens_payloads_raw_on_normal_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = _FakeDb(
        [[
            _domain_row(
                row_id=901,
                event_id="evt-invalid-decision",
                seq=9,
                event_name="DECISION_EMITTED",
                context={
                    "series_key": "instrument-btc|1m",
                    "decision_state": "accepted",
                    "signal_id": "signal-1",
                    "decision_id": "signal-1",
                    "direction": "long",
                    "signal_price": 100.0,
                    "bar_epoch": 1700000000,
                },
            )
        ]]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    rows = runtime_events.list_bot_runtime_events(bot_id="bot-1", run_id="run-1", limit=1)

    assert len(rows) == 1
    assert rows[0]["event_id"] == "evt-invalid-decision"
    assert rows[0]["payload"]["context"]["signal_id"] == "signal-1"
    assert rows[0]["payload"]["context"]["decision_id"] == "signal-1"
