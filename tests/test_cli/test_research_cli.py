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


def test_research_checks_run_builds_candle_condition_request(monkeypatch):
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
            "checks",
            "run",
            "--title",
            "ETH contraction follow-through",
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
            "range_pct",
            "--operator",
            "lt",
            "--value",
            "0.01",
            "--forward-bars",
            "1,3,5",
            "--direction",
            "long",
            "--min-sample-count",
            "5",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/research/checks/run",
        "body": {
            "title": "ETH contraction follow-through",
            "observation_id": "obs-1",
            "scope": {
                "instrument_id": "inst-eth",
                "timeframe": "1h",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-02-01T00:00:00Z",
            },
            "detector": {
                "type": "candle_condition",
                "field": "range_pct",
                "operator": "lt",
                "value": 0.01,
            },
            "outcomes": {
                "forward_bars": [1, 3, 5],
                "direction": "long",
                "min_sample_count": 5,
            },
        },
    }
