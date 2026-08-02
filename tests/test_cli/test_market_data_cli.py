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


def test_data_collectors_create_coinbase_oi_is_explicit_and_disabled_by_default(
    monkeypatch,
) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"definition": {"id": "mcd_1", "enabled": False}})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "collectors",
            "create-coinbase-oi",
            "--instrument-id",
            "coinbase-btc-future",
            "--provider-product-id",
            "BIT-28NOV25-CDE",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/market-data/collectors",
        "body": {
            "instrument_id": "coinbase-btc-future",
            "provider_product_id": "BIT-28NOV25-CDE",
            "fact_type": "derivatives.open_interest",
            "poll_interval_seconds": 60,
            "max_attempts": 3,
            "minimum_spacing_seconds": 1.0,
            "enabled": False,
        },
    }


def test_data_open_interest_latest_declares_decision_time_and_staleness(
    monkeypatch,
) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        parsed = urllib.parse.urlparse(request.full_url)
        observed.update(
            method=request.get_method(),
            path=parsed.path,
            query=urllib.parse.parse_qs(parsed.query),
        )
        return _Response({"available": False, "reason": "stale"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "open-interest-latest",
            "--instrument-id",
            "coinbase-btc-future",
            "--decision-time",
            "2026-08-01T18:00:00Z",
            "--max-staleness-seconds",
            "120",
            "--optional",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "GET",
        "path": "/api/market-data/open-interest/latest",
        "query": {
            "instrument_id": ["coinbase-btc-future"],
            "decision_time": ["2026-08-01T18:00:00Z"],
            "max_staleness_seconds": ["120"],
            "required": ["false"],
        },
    }


def test_data_collectors_create_coinbase_funding_is_explicit(
    monkeypatch,
) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"definition": {"id": "mcd_funding", "enabled": True}})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "collectors",
            "create-coinbase-funding",
            "--instrument-id",
            "coinbase-eth-future",
            "--provider-product-id",
            "ETP-20DEC30-CDE",
            "--enabled",
        ]
    )

    assert exit_code == 0
    assert observed["body"] == {
        "instrument_id": "coinbase-eth-future",
        "provider_product_id": "ETP-20DEC30-CDE",
        "fact_type": "derivatives.funding_rate",
        "poll_interval_seconds": 60,
        "max_attempts": 3,
        "minimum_spacing_seconds": 1.0,
        "enabled": True,
    }


def test_data_funding_rate_latest_declares_decision_time_and_staleness(
    monkeypatch,
) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        parsed = urllib.parse.urlparse(request.full_url)
        observed.update(
            method=request.get_method(),
            path=parsed.path,
            query=urllib.parse.parse_qs(parsed.query),
        )
        return _Response({"available": False, "reason": "stale"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "funding-rate-latest",
            "--instrument-id",
            "coinbase-eth-future",
            "--decision-time",
            "2026-08-01T18:00:00Z",
            "--max-staleness-seconds",
            "120",
            "--optional",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "GET",
        "path": "/api/market-data/funding-rate/latest",
        "query": {
            "instrument_id": ["coinbase-eth-future"],
            "decision_time": ["2026-08-01T18:00:00Z"],
            "max_staleness_seconds": ["120"],
            "required": ["false"],
        },
    }


def test_market_structure_configure_is_bounded_and_never_production_enrolls(
    monkeypatch,
) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"pair_id": "bip_btc", "production_admitted": False})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "configure-pair",
            "--pair",
            "bip_btc",
            "--auth-mode",
            "authenticated",
            "--spool-gib",
            "8",
            "--segment-mib",
            "128",
        ]
    )
    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/market-data/market-structure/pairs",
        "body": {
            "pair_id": "bip_btc",
            "auth_mode": "authenticated",
            "max_spool_bytes": 8 * 1024**3,
            "max_segment_bytes": 128 * 1024**2,
            "enable_production": False,
        },
    }


def test_market_structure_capture_is_explicitly_bounded(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
            timeout=timeout,
        )
        return _Response({"status": "completed"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "--timeout",
            "180",
            "data",
            "market-structure",
            "capture",
            "ms_coinbase_bip_20dec30_cde",
            "--duration",
            "60",
            "--storage-root",
            "/data/market-structure",
        ]
    )
    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/market-data/market-structure/definitions/ms_coinbase_bip_20dec30_cde/capture",
        "body": {
            "duration_seconds": 60.0,
            "storage_root": "/data/market-structure",
            "owner_id": None,
        },
        "timeout": 180.0,
    }


def test_market_structure_status_and_replay_use_typed_routes(monkeypatch) -> None:
    observed = []

    def fake_urlopen(request, timeout):
        observed.append(
            {
                "method": request.get_method(),
                "path": urllib.parse.urlparse(request.full_url).path,
                "body": json.loads(request.data.decode("utf-8")) if request.data else None,
            }
        )
        return _Response({"schema_version": "test.v1"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "status",
            "definition-a",
        ]
    ) == 0
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "replay",
            "manifest-a",
        ]
    ) == 0
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "replay-book",
            "definition-a",
            "session-a",
            "--storage-root",
            "/data/market-structure",
        ]
    ) == 0
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "compact",
            "definition-a",
            "session-a",
            "--manifest-id",
            "manifest-a",
            "--manifest-id",
            "manifest-b",
        ]
    ) == 0
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "retention-pin",
            "raw_manifest",
            "manifest-a",
            "--owner-kind",
            "operator",
            "--owner-id",
            "test",
            "--reason",
            "test complete",
            "--release",
        ]
    ) == 0
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "retention-status",
            "raw_manifest",
            "manifest-a",
        ]
    ) == 0
    assert observed == [
        {
            "method": "GET",
            "path": "/api/market-data/market-structure/definitions/definition-a/status",
            "body": None,
        },
        {
            "method": "POST",
            "path": "/api/market-data/market-structure/manifests/manifest-a/replay",
            "body": {"storage_root": None},
        },
        {
            "method": "POST",
            "path": "/api/market-data/market-structure/definitions/definition-a/sessions/session-a/replay-book",
            "body": {"storage_root": "/data/market-structure"},
        },
        {
            "method": "POST",
            "path": "/api/market-data/market-structure/definitions/definition-a/sessions/session-a/compact",
            "body": {
                "source_manifest_ids": ["manifest-a", "manifest-b"],
                "storage_root": None,
                "owner_id": None,
            },
        },
        {
            "method": "POST",
            "path": "/api/market-data/market-structure/archive-retention/raw_manifest/manifest-a/pin",
            "body": {
                "owner_kind": "operator",
                "owner_id": "test",
                "active": False,
                "reason": "test complete",
            },
        },
        {
            "method": "GET",
            "path": "/api/market-data/market-structure/archive-retention/raw_manifest/manifest-a",
            "body": None,
        },
    ]


def test_market_structure_recent_reconciliation_is_bounded(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        parsed = urllib.parse.urlparse(request.full_url)
        observed.update(
            method=request.get_method(),
            path=parsed.path,
            query=urllib.parse.parse_qs(parsed.query),
        )
        return _Response(
            {
                "schema_version": "market.recent_trade_reconciliation.v1",
                "historical_completeness_claim": "none",
            }
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "reconcile-recent",
            "definition-a",
            "--limit",
            "25",
        ]
    ) == 0
    assert observed == {
        "method": "POST",
        "path": "/api/market-data/market-structure/definitions/definition-a/reconcile-recent",
        "query": {"limit": ["25"]},
    }
