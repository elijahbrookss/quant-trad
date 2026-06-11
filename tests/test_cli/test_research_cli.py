from __future__ import annotations

import json
import urllib.parse
import urllib.request

from cli.main import main


class _Response:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_research_observe_create_uses_memory_item_route(monkeypatch):
    observed = {}

    def fake_urlopen(request, timeout):
        _ = timeout
        observed["method"] = request.get_method()
        observed["path"] = urllib.parse.urlparse(request.full_url).path
        observed["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(b'{"id":"obs-1","kind":"observation"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--no-audit-log",
            "research",
            "observe",
            "create",
            "--title",
            "ETH range contractions look cleaner",
            "--symbol",
            "ETH/USD",
            "--timeframe",
            "1h",
            "--tag",
            "range",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/research/items",
        "body": {
            "kind": "observation",
            "status": "active",
            "title": "ETH range contractions look cleaner",
            "symbol": "ETH/USD",
            "timeframe": "1h",
            "tags": ["range"],
        },
    }


def test_research_check_raw_builds_raw_condition_request(monkeypatch):
    observed = {}

    def fake_urlopen(request, timeout):
        _ = timeout
        observed["method"] = request.get_method()
        observed["path"] = urllib.parse.urlparse(request.full_url).path
        observed["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(b'{"schema_version":"research_check_run.v1","status":"completed"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--no-audit-log",
            "research",
            "check",
            "raw",
            "--title",
            "ETH close follow-through",
            "--observation-id",
            "obs-1",
            "--instrument-id",
            "inst-eth",
            "--timeframe",
            "1h",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-02-01T00:00:00Z",
            "--field",
            "close",
            "--operator",
            "gt",
            "--value-field",
            "previous_close",
            "--forward-bars",
            "1,3,5",
            "--direction",
            "long",
            "--min-sample-count",
            "5",
            "--max-examples",
            "7",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/research/checks/run",
        "body": {
            "title": "ETH close follow-through",
            "check_family": "raw_forward_outcome",
            "observation_id": "obs-1",
            "scope": {
                "instrument_id": "inst-eth",
                "timeframe": "1h",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-02-01T00:00:00Z",
            },
            "detector": {
                "type": "raw_condition",
                "field": "close",
                "operator": "gt",
                "value_field": "previous_close",
            },
            "outcomes": {
                "forward_bars": [1, 3, 5],
                "direction": "long",
                "min_sample_count": 5,
                "max_examples": 7,
            },
        },
    }


def test_research_check_signal_builds_report_request(monkeypatch):
    observed = {}

    def fake_urlopen(request, timeout):
        _ = timeout
        observed["method"] = request.get_method()
        observed["path"] = urllib.parse.urlparse(request.full_url).path
        observed["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(b'{"schema_version":"research_check_run.v1","status":"completed"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--no-audit-log",
            "research",
            "check",
            "signal",
            "--run-id",
            "run-1",
            "--output-name",
            "confirmed_balance_breakout",
            "--bucket-by",
            "symbol,event_key",
            "--min-sample-count",
            "5",
        ]
    )

    assert exit_code == 0
    assert observed["method"] == "POST"
    assert observed["path"] == "/api/research/checks/run"
    assert observed["body"] == {
        "title": "Run signal check: confirmed_balance_breakout",
        "check_family": "run_signal_summary",
        "scope": {"run_id": "run-1"},
        "detector": {"type": "run_signal_match", "output_name": "confirmed_balance_breakout"},
        "outcomes": {"bucket_by": ["symbol", "event_key"], "min_sample_count": 5},
    }


def test_research_check_indicator_builds_indicator_request(monkeypatch):
    observed = {}

    def fake_urlopen(request, timeout):
        _ = timeout
        observed["method"] = request.get_method()
        observed["path"] = urllib.parse.urlparse(request.full_url).path
        observed["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(b'{"schema_version":"research_check_run.v1","status":"completed"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--no-audit-log",
            "research",
            "check",
            "indicator",
            "--indicator-id",
            "stats-1",
            "--instrument-id",
            "inst-eth",
            "--timeframe",
            "1h",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-02-01T00:00:00Z",
            "--output",
            "candle_stats",
            "--field",
            "range_pct",
            "--operator",
            "gt",
            "--value",
            "0.02",
            "--forward-bars",
            "1,3",
        ]
    )

    assert exit_code == 0
    assert observed["body"]["check_family"] == "indicator_forward_outcome"
    assert observed["body"]["scope"] == {
        "instrument_id": "inst-eth",
        "timeframe": "1h",
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-02-01T00:00:00Z",
        "indicator_id": "stats-1",
    }
    assert observed["body"]["detector"] == {
        "type": "indicator_output_match",
        "output_name": "candle_stats",
        "field": "range_pct",
        "operator": "gt",
        "value": 0.02,
    }


def test_research_read_models_use_backend_routes(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        calls.append((request.get_method(), urllib.parse.urlparse(request.full_url).path, urllib.parse.urlparse(request.full_url).query))
        return _Response(b'{"schema_version":"ok"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    assert main(["--no-audit-log", "research", "run", "run-1"]) == 0
    assert main(["--no-audit-log", "research", "trail", "obs-1"]) == 0
    assert main(["--no-audit-log", "research", "compare", "check-a", "check-b"]) == 0

    assert calls == [
        ("GET", "/api/research/runs/run-1/evidence", ""),
        ("GET", "/api/research/items/obs-1/trail", ""),
        ("GET", "/api/research/checks/compare", "left_check_id=check-a&right_check_id=check-b"),
    ]
