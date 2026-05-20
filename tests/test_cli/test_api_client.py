from __future__ import annotations

import json
from pathlib import Path
import urllib.parse
import urllib.request

from cli.api import ApiBytesResponse, ApiClient, filename_from_content_disposition
from cli.audit import report_export_dir
from types import SimpleNamespace

from cli.main import _build_output_filters, _key_value_map, _write_report_export, main


class _Response:
    status = 200

    def __init__(self, body: bytes, headers: dict[str, str] | None = None) -> None:
        self._body = body
        self.headers = headers or {}

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_api_client_encodes_params_and_json_payload(monkeypatch):
    observed = {}

    def fake_urlopen(request, timeout):
        observed["url"] = request.full_url
        observed["method"] = request.get_method()
        observed["timeout"] = timeout
        observed["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(b'{"ok": true}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    client = ApiClient("http://backend.test", timeout=12)
    payload = client.request_json(
        "POST",
        "/api/reports/run-1/export",
        params={"include_candles": True, "skip": None},
        payload={"include_json": True},
    )

    assert payload == {"ok": True}
    assert observed == {
        "url": "http://backend.test/api/reports/run-1/export?include_candles=true",
        "method": "POST",
        "timeout": 12.0,
        "body": {"include_json": True},
    }


def test_api_client_emits_http_observer_event(monkeypatch):
    events = []

    def fake_urlopen(_request, timeout):
        _ = timeout
        return _Response(b'{"ok": true}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    client = ApiClient("http://backend.test", observer=lambda event, fields: events.append((event, fields)))
    assert client.request_json("GET", "/api/health") == {"ok": True}

    assert events[0][0] == "http_request"
    assert events[0][1]["method"] == "GET"
    assert events[0][1]["status"] == 200


def test_filename_from_content_disposition_uses_header_filename():
    assert (
        filename_from_content_disposition(
            'attachment; filename="run_run-1_report_export.zip"',
            "fallback.zip",
        )
        == "run_run-1_report_export.zip"
    )
    assert filename_from_content_disposition(None, "fallback.zip") == "fallback.zip"


def test_key_value_map_preserves_json_scalar_types():
    assert _key_value_map(["alpha=1", "enabled=true", "name=fast", "weights=[1,2]"]) == {
        "alpha": 1,
        "enabled": True,
        "name": "fast",
        "weights": [1, 2],
    }


def test_build_output_filters_from_single_cli_filter():
    filters = _build_output_filters(
        SimpleNamespace(
            filters_json=None,
            filter=[],
            intent=["enter_long", "enter_short"],
            rule_id=[],
            indicator_id="regime-1",
            output_name="market_regime",
            field="expansion_state",
            operator="equals",
            value=None,
            equals="expanding",
        )
    )

    assert filters == [
        {
            "scope": {"intent": ["enter_long", "enter_short"]},
            "indicator_id": "regime-1",
            "output_name": "market_regime",
            "field": "expansion_state",
            "operator": "equals",
            "value": "expanding",
        }
    ]


def test_report_export_dir_partitions_by_date_and_run():
    path = report_export_dir("logs/reports", run_id="run/1")

    assert "logs/reports" in str(path)
    assert path.name == "run_run-1"


def test_write_report_export_uses_partitioned_output_dir(tmp_path):
    class _Client:
        def request_bytes(self, _method, _path, *, payload):
            assert payload == {"include_json": True, "include_csv": True, "include_candles": False}
            return ApiBytesResponse(
                body=b"zip-bytes",
                headers={"content-disposition": 'attachment; filename="run_run-1_report_export.zip"'},
                status=200,
            )

    args = SimpleNamespace(out_dir=None, log_root=str(tmp_path), _audit_log=None)

    payload = _write_report_export(
        args,
        _Client(),
        run_id="run-1",
        include_json=True,
        include_csv=True,
        include_candles=False,
    )

    assert Path(payload["path"]).read_bytes() == b"zip-bytes"
    assert Path(payload["path"]).parent.name == "run_run-1"
    assert str(Path(tmp_path) / "reports") in payload["path"]


def test_cli_main_writes_audit_log(tmp_path, monkeypatch):
    def fake_urlopen(_request, timeout):
        _ = timeout
        return _Response(b'{"status": "ok"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(["--log-root", str(tmp_path), "health"])

    audit_files = list((tmp_path / "cli").glob("**/*.json"))
    assert exit_code == 0
    assert len(audit_files) == 1
    audit_payload = json.loads(audit_files[0].read_text(encoding="utf-8"))
    assert audit_payload["command"] == "health"
    assert audit_payload["exit_code"] == 0
    assert audit_payload["events"][0]["event"] == "command_started"


def test_bots_create_and_update_use_backend_bot_routes(tmp_path, monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        calls.append((request.get_method(), urllib.parse.urlparse(request.full_url).path, json.loads(request.data.decode("utf-8"))))
        return _Response(b'{"id": "bot-1"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    create_code = main(
        [
            "--log-root",
            str(tmp_path),
            "bots",
            "create",
            "--name",
            "baseline",
            "--strategy-id",
            "strategy-1",
            "--variant-id",
            "variant-1",
            "--execution-mode",
            "full",
            "--snapshot-interval-ms",
            "1000",
            "--wallet-json",
            '{"balances":{"USD":10000}}',
        ]
    )
    update_code = main(
        [
            "--log-root",
            str(tmp_path),
            "bots",
            "update",
            "bot-1",
            "--execution-mode",
            "full",
            "--backtest-start",
            "2026-01-01T00:00:00Z",
        ]
    )

    assert create_code == 0
    assert update_code == 0
    assert calls[0] == (
        "POST",
        "/api/bots",
        {
            "name": "baseline",
            "strategy_id": "strategy-1",
            "strategy_variant_id": "variant-1",
            "execution_mode": "full",
            "snapshot_interval_ms": 1000,
            "wallet_config": {"balances": {"USD": 10000}},
        },
    )
    assert calls[1] == (
        "PUT",
        "/api/bots/bot-1",
        {"execution_mode": "full", "backtest_start": "2026-01-01T00:00:00Z"},
    )


def test_providers_stream_smoke_uses_backend_route(tmp_path, monkeypatch):
    observed = {}

    def fake_urlopen(request, timeout):
        observed["method"] = request.get_method()
        observed["path"] = urllib.parse.urlparse(request.full_url).path
        observed["timeout"] = timeout
        observed["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(
            json.dumps(
                {
                    "schema_version": "provider_stream_smoke.v1",
                    "status": "completed",
                    "counts": {"market_ticker": 1},
                }
            ).encode("utf-8")
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--log-root",
            str(tmp_path),
            "providers",
            "stream-smoke",
            "--symbol",
            "BIP-20DEC30-CDE",
            "--duration",
            "1",
            "--channel",
            "ticker",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/providers/stream-smoke",
        "timeout": 30.0,
        "body": {
            "provider_id": "COINBASE",
            "venue_id": "COINBASE_DIRECT",
            "symbol": "BIP-20DEC30-CDE",
            "product_id": None,
            "channels": ["ticker"],
            "timeframe": None,
            "auth_mode": "public",
            "duration_seconds": 1.0,
            "sample_limit": 10,
        },
    }


def test_provider_credentials_add_reads_stdin_json_and_redacts_audit(tmp_path, monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        path = urllib.parse.urlparse(request.full_url).path
        method = request.get_method()
        body = json.loads(request.data.decode("utf-8")) if request.data else None
        calls.append((method, path, body))
        if method == "GET" and path == "/api/providers/credentials/schema":
            return _Response(
                json.dumps(
                    {
                        "provider_id": "COINBASE",
                        "venue_id": "COINBASE_DIRECT",
                        "environment": "paper",
                        "default_credential_ref": "coinbase-coinbase-direct-paper",
                        "required": ["COINBASE_API_KEY", "COINBASE_API_SECRET"],
                        "optional": [],
                        "accepted": ["COINBASE_API_KEY", "COINBASE_API_SECRET"],
                        "secrets_are_returned": False,
                    }
                ).encode("utf-8")
            )
        return _Response(
            json.dumps(
                {
                    "credential": {
                        "credential_ref": "coinbase-coinbase-direct-paper",
                        "provider_id": "COINBASE",
                        "venue_id": "COINBASE_DIRECT",
                        "status": "active",
                    },
                    "secrets_are_returned": False,
                }
            ).encode("utf-8")
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    secret_json = '{"COINBASE_API_KEY":"key-123","COINBASE_API_SECRET":"secret-456"}'
    exit_code = main(
        [
            "--log-root",
            str(tmp_path),
            "providers",
            "credentials",
            "add",
            "--provider",
            "COINBASE",
            "--venue",
            "COINBASE_DIRECT",
            "--secrets-json",
            secret_json,
            "--no-input",
        ]
    )

    assert exit_code == 0
    assert calls[1] == (
        "POST",
        "/api/providers/credentials",
        {
            "provider_id": "COINBASE",
            "venue_id": "COINBASE_DIRECT",
            "credential_ref": "coinbase-coinbase-direct-paper",
            "environment": "paper",
            "display_name": None,
            "credentials": {
                "COINBASE_API_KEY": "key-123",
                "COINBASE_API_SECRET": "secret-456",
            },
        },
    )
    audit_payload = next((tmp_path / "cli").glob("**/*.json")).read_text(encoding="utf-8")
    assert "key-123" not in audit_payload
    assert "secret-456" not in audit_payload
    assert "***REDACTED***" in audit_payload


def test_bots_start_supports_observe_only_paper_overrides(tmp_path, monkeypatch):
    observed = {}

    def fake_urlopen(request, timeout):
        _ = timeout
        observed["method"] = request.get_method()
        observed["path"] = urllib.parse.urlparse(request.full_url).path
        observed["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(b'{"status":"started","run_id":"run-1"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--log-root",
            str(tmp_path),
            "bots",
            "start",
            "bot-1",
            "--request-id",
            "req-1",
            "--run-type",
            "paper",
            "--execution",
            "observe-only",
            "--duration-seconds",
            "30",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/bots/bot-1/runs/start",
        "body": {
            "request_id": "req-1",
            "run_type": "paper",
            "execution_behavior": "observe-only",
            "duration_seconds": 30.0,
        },
    }


def test_experiments_start_bot_writes_resumable_record(tmp_path, monkeypatch):
    def fake_urlopen(request, timeout):
        _ = timeout
        assert request.get_method() == "POST"
        assert urllib.parse.urlparse(request.full_url).path == "/api/bots/bot-1/runs/start"
        return _Response(
            json.dumps(
                {
                    "schema_version": "bot_run_start.v1",
                    "request_id": "req-1",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "status": "starting",
                    "phase": "launching_container",
                }
            ).encode("utf-8")
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--log-root",
            str(tmp_path),
            "experiments",
            "start-bot",
            "bot-1",
            "--request-id",
            "req-1",
            "--baseline-run-id",
            "base-run",
            "--export",
        ]
    )

    records = list((tmp_path / "experiments").glob("**/experiment.json"))
    assert exit_code == 0
    assert len(records) == 1
    payload = json.loads(records[0].read_text(encoding="utf-8"))
    assert payload["schema_version"] == "qt_cli_experiment.v1"
    assert payload["experiment_id"] == "req-1"
    assert payload["run_id"] == "run-1"
    assert payload["baseline_run_id"] == "base-run"
    assert payload["collect_defaults"]["export"] is True


def test_experiments_collect_exports_materializes_and_compares(tmp_path, monkeypatch):
    record_dir = tmp_path / "experiments" / "2026" / "05" / "17" / "req-1"
    record_dir.mkdir(parents=True)
    record_path = record_dir / "experiment.json"
    record_path.write_text(
        json.dumps(
            {
                "schema_version": "qt_cli_experiment.v1",
                "experiment_id": "req-1",
                "request_id": "req-1",
                "bot_id": "bot-1",
                "run_id": "run-1",
                "baseline_run_id": "base-run",
            }
        ),
        encoding="utf-8",
    )
    calls: list[tuple[str, str]] = []

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        calls.append((request.get_method(), parsed.path))
        if parsed.path == "/api/bots/bot-1/runs/run-1/status":
            return _Response(
                b'{"schema_version":"bot_run_status.v1","bot_id":"bot-1","run_id":"run-1","status":"completed","terminal":true}'
            )
        if parsed.path == "/api/reports/run-1/export":
            return _Response(b"zip-bytes", headers={"content-disposition": 'attachment; filename="run_run-1_report_export.zip"'})
        if parsed.path in {"/api/reports/base-run/run-report/build", "/api/reports/run-1/run-report/build"}:
            run_id = parsed.path.split("/")[3]
            return _Response(
                json.dumps(
                    {
                        "contract_version": "run_report_v2",
                        "schema_version": "run_report_materialization_status.v1",
                        "run_id": run_id,
                        "report_status": {"status": "ready", "can_view": True, "can_build": False, "can_retry": False},
                    }
                ).encode("utf-8")
            )
        if parsed.path == "/api/reports/compare/summary":
            return _Response(
                b'{"schema_version":"run_report_comparison_summary.v1","left_run_id":"base-run","right_run_id":"run-1","comparison_status":"ready","comparison_verdict":"semantic_drift","can_compare":true}'
            )
        raise AssertionError(f"unexpected API call: {request.get_method()} {request.full_url}")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(["--log-root", str(tmp_path), "experiments", "collect", str(record_path), "--export"])

    updated_records = []
    for path in (tmp_path / "experiments").glob("**/experiment.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("experiment_id") == "req-1":
            updated_records.append(payload)
    updated = next(payload for payload in updated_records if "collect" in payload)
    assert exit_code == 0
    assert ("GET", "/api/bots/bot-1/runs/run-1/status") in calls
    assert ("POST", "/api/reports/run-1/export") in calls
    assert ("POST", "/api/reports/base-run/run-report/build") in calls
    assert ("GET", "/api/reports/compare/summary") in calls
    assert updated["collect"]["comparison"]["comparison_verdict"] == "semantic_drift"
    assert list((tmp_path / "reports").glob("**/run_run-1_report_export.zip"))
