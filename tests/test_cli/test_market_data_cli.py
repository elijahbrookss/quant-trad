from __future__ import annotations

import json
import urllib.parse
import urllib.request

from cli.main import main


class _Response:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self._body


def test_data_ingest_candles_uses_explicit_mutation_contract(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
            timeout=timeout,
        )
        return _Response({"outcome": {"inserted_count": 60}})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "ingest-candles",
            "--instrument-id",
            "instrument-1",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-01T01:00:00Z",
            "--timeframe",
            "1m",
        ]
    )

    assert exit_code == 0
    assert observed["method"] == "POST"
    assert observed["path"] == "/api/candles/ingest"
    assert observed["body"] == {
        "instrument_id": "instrument-1",
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-01T01:00:00Z",
        "timeframe": "1m",
        "source_revision": None,
    }


def test_data_freeze_dataset_builds_exact_single_series_request(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"dataset_id": "mds_123", "dataset_hash": "123"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "freeze-dataset",
            "--instrument-id",
            "instrument-1",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-02-01T00:00:00Z",
            "--timeframe",
            "1h",
            "--name",
            "reference-window",
            "--created-by",
            "operator",
        ]
    )

    assert exit_code == 0
    assert observed["method"] == "POST"
    assert observed["path"] == "/api/candles/datasets/freeze"
    assert observed["body"] == {
        "series": [
            {
                "instrument_id": "instrument-1",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-02-01T00:00:00Z",
                "timeframe": "1h",
            }
        ],
        "name": "reference-window",
        "purpose": "research",
        "created_by": "operator",
        "metadata": {},
    }


def test_prepare_backtest_dataset_is_separate_from_execution(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"status": "ready", "dataset": {"dataset_id": "mds_123"}})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "prepare-backtest-dataset",
            "--bot-id",
            "bot-1",
            "--start",
            "2024-01-01T00:00:00Z",
            "--end",
            "2025-01-01T00:00:00Z",
            "--acquire-missing",
            "--created-by",
            "operator",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/bots/bot-1/backtest-dataset/prepare",
        "body": {
            "evaluation_start": "2024-01-01T00:00:00Z",
            "evaluation_end": "2025-01-01T00:00:00Z",
            "acquire_missing": True,
            "created_by": "operator",
        },
    }


def test_backtest_start_sends_the_existing_dataset_identity(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"status": "started", "run_id": "run-1"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "bots",
            "start",
            "bot-1",
            "--run-type",
            "backtest",
            "--dataset-id",
            "mds_123",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/bots/bot-1/runs/start",
        "body": {"run_type": "backtest", "dataset_id": "mds_123"},
    }
