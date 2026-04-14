from __future__ import annotations

from contextlib import contextmanager

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.storage.repos import runtime_events


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

    def to_dict(self):
        return dict(self._payload)


class _SequencedSession:
    def __init__(self, values):
        self._values = list(values)
        self.execute_calls = 0

    def execute(self, _stmt):
        if not self._values:
            raise AssertionError("session requires at least one result")
        index = min(self.execute_calls, len(self._values) - 1)
        value = self._values[index]
        self.execute_calls += 1
        if isinstance(value, Exception):
            raise value
        return _FakeScalarResult(value)


class _FakeDb:
    available = True

    def __init__(self, values):
        self.session_handle = _SequencedSession(values)

    @contextmanager
    def session(self):
        yield self.session_handle


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


def test_list_bot_run_view_states_filters_noncanonical_series_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_db = _FakeDb(
        [[
            _FakeViewStateRow({"series_key": "bot", "seq": 1}),
            _FakeViewStateRow({"series_key": "instrument-btc|1M", "seq": 2}),
            _FakeViewStateRow({"series_key": "instrument-btc|", "seq": 3}),
            _FakeViewStateRow({"series_key": "instrument-eth|5m", "seq": 4}),
        ]]
    )
    monkeypatch.setattr(runtime_events, "db", fake_db)

    rows = runtime_events.list_bot_run_view_states(bot_id="bot-1", run_id="run-1")

    assert rows == [
        {"series_key": "instrument-btc|1m", "seq": 2},
        {"series_key": "instrument-eth|5m", "seq": 4},
    ]


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
    assert observed["context"] == {"run_id": "run-1", "bot_id": "bot-1", "event_id": "evt-1"}
    assert isinstance(observed["started"], float)
    assert observed["started"] >= 0.0


def test_list_bot_runtime_events_projects_common_payload_fields(
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
            },
            "series_key": "instrument-btc|1m",
            "bridge_session_id": "bridge-1",
            "bridge_seq": 7,
            "run_seq": 9,
            "instrument_id": "instrument-btc",
        }
    ]
