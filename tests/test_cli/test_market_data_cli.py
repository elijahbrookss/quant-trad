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
        "body": {
            "run_type": "backtest",
            "dataset_id": "mds_123",
            "economic_claim_intent": "exploration",
        },
    }


def test_data_collectors_fleet_uses_canonical_operational_snapshot(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        parsed = urllib.parse.urlparse(request.full_url)
        observed.update(
            method=request.get_method(),
            path=parsed.path,
            query=urllib.parse.parse_qs(parsed.query),
        )
        return _Response({"collectors": []})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "collectors",
            "fleet",
            "--attempt-limit",
            "7",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "GET",
        "path": "/api/market-data/operations/collectors/snapshot",
        "query": {"attempt_limit": ["7"]},
    }


def test_data_collectors_restart_is_audited_and_confirmed(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"operation": {"status": "succeeded"}})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "collectors",
            "restart",
            "continuous_stream",
            "stream-1",
            "--request-id",
            "request-1",
            "--actor-id",
            "operator-1",
            "--reason",
            "recover stale worker",
            "--confirm",
        ]
    )

    assert exit_code == 0
    assert observed["method"] == "POST"
    assert observed["path"] == (
        "/api/market-data/operations/collectors/continuous_stream/"
        "stream-1/actions/restart"
    )
    assert observed["body"]["request_id"] == "request-1"
    assert observed["body"]["actor_id"] == "operator-1"
    assert observed["body"]["confirmation"] == (
        "continuous_stream:stream-1:restart"
    )
    assert observed["body"]["context"] == {
        "surface": "qt",
        "reason": "recover stale worker",
    }
    assert observed["body"]["requested_at"].endswith("+00:00")


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


def test_market_structure_enroll_applies_a_manifest(
    monkeypatch,
) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"fleet_id": "coinbase_perpetual_trades"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "enroll",
            "--manifest-path",
            "config/custom-fleet.json",
        ]
    )
    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/market-data/market-structure/enrollments/apply",
        "body": {"manifest_path": "config/custom-fleet.json"},
    }


def test_market_structure_safety_halt_is_scoped_and_audited(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response(
            {
                "schema_version": "market.collector_safety_event.v1",
            }
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "safety-halt",
            "--scope-type",
            "fleet",
            "--scope-id",
            "coinbase_perpetual_trades",
            "--request-id",
            "request-a",
            "--requested-by",
            "operator-a",
            "--reason",
            "operator test",
            "--policy-hash",
            "abc123",
        ]
    )
    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/market-data/market-structure/safety/halt",
        "body": {
            "request_id": "request-a",
            "scope_type": "fleet",
            "scope_id": "coinbase_perpetual_trades",
            "requested_by": "operator-a",
            "reason": "operator test",
            "policy_hash": "abc123",
            "evidence": None,
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


def test_market_structure_historical_continuous_evidence_is_read_only(monkeypatch) -> None:
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
            "continuous-evidence",
            "definition-a",
            "session-a",
        ]
    ) == 0
    assert observed == [
        {
            "method": "GET",
            "path": "/api/market-data/market-structure/definitions/definition-a/continuous/validation/session-a",
            "body": None,
        },
    ]


def test_market_storage_lifecycle_cli_is_dry_run_first(monkeypatch) -> None:
    observed = []

    def fake_urlopen(request, timeout):
        parsed = urllib.parse.urlparse(request.full_url)
        observed.append(
            {
                "method": request.get_method(),
                "path": parsed.path,
                "query": urllib.parse.parse_qs(parsed.query),
                "body": (
                    json.loads(request.data.decode("utf-8"))
                    if request.data
                    else None
                ),
            }
        )
        return _Response({"schema_version": "market.storage_lifecycle_run.v1"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "lifecycle-plan",
        ]
    ) == 0
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "lifecycle-run",
            "--storage-root",
            "/portable/market-data",
        ]
    ) == 0
    assert main(
        [
            "--no-audit-log",
            "data",
            "market-structure",
            "lifecycle-events",
            "--limit",
            "11",
        ]
    ) == 0

    assert observed == [
        {
            "method": "GET",
            "path": "/api/market-data/market-structure/storage-lifecycle/plan",
            "query": {},
            "body": None,
        },
        {
            "method": "POST",
            "path": "/api/market-data/market-structure/storage-lifecycle/run",
            "query": {},
            "body": {
                "execute": False,
                "storage_root": "/portable/market-data",
                "owner_id": None,
            },
        },
        {
            "method": "GET",
            "path": "/api/market-data/market-structure/storage-lifecycle/events",
            "query": {"limit": ["11"]},
            "body": None,
        },
    ]
