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


def test_strategies_create_and_rule_create_use_backend_contracts(tmp_path, monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        calls.append((request.get_method(), urllib.parse.urlparse(request.full_url).path, json.loads(request.data.decode("utf-8"))))
        return _Response(b'{"ok": true}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    strategy_payload = {
        "name": "ATR expansion BTC",
        "instrument_slots": [{"symbol": "BTC/USD", "instrument_id": "inst-btc"}],
        "timeframe": "1h",
        "datasource": "CCXT",
        "exchange": "COINBASE",
        "indicator_ids": ["candle-stats-1"],
        "atm_template_id": "atm-1",
        "risk_config": {"base_risk_per_trade": 250.0},
    }
    rule_payload = {
        "name": "ATR expansion long",
        "intent": "enter_long",
        "trigger": {
            "type": "signal_match",
            "indicator_id": "candle-stats-1",
            "output_name": "atr_expansion",
            "event_key": "atr_expansion_long",
        },
        "guards": [],
    }

    create_code = main(
        [
            "--log-root",
            str(tmp_path),
            "strategies",
            "create",
            "--payload-json",
            json.dumps(strategy_payload),
        ]
    )
    rule_code = main(
        [
            "--log-root",
            str(tmp_path),
            "strategies",
            "rule-create",
            "strategy-1",
            "--payload-json",
            json.dumps(rule_payload),
        ]
    )

    assert create_code == 0
    assert rule_code == 0
    assert calls == [
        ("POST", "/api/strategies/", strategy_payload),
        ("POST", "/api/strategies/strategy-1/rules", rule_payload),
    ]


def test_strategies_preview_defaults_to_summary_and_compare_uses_cases(tmp_path, monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        body = json.loads(request.data.decode("utf-8")) if request.data else None
        path = urllib.parse.urlparse(request.full_url).path
        calls.append((request.get_method(), path, body))
        if path.endswith("/preview/summary"):
            return _Response(
                json.dumps(
                    {
                        "schema_version": "strategy_preview_summary.v1",
                        "preview_id": "preview-1",
                        "strategy_id": "strategy-1",
                        "strategy_name": "Strategy One",
                        "instruments": {
                            "instrument-1": {
                                "instrument_id": "instrument-1",
                                "symbol": "BTC/USD",
                                "signals": 1,
                                "why_empty": [],
                                "examples": [{"signal_id": "signal-1", "bar_epoch": 1}],
                                "signals_detail": [{"signal_id": "signal-1", "bar_epoch": 1}],
                            }
                        },
                    }
                ).encode("utf-8")
            )
        if path.endswith("/preview"):
            return _Response(b'{"preview_id":"preview-full"}')
        if path == "/api/strategies/preview/compare":
            return _Response(b'{"schema_version":"strategy_preview_compare.v1","case_count":2}')
        raise AssertionError(f"unexpected API call: {request.get_method()} {request.full_url}")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    summary_code = main(
        [
            "--log-root",
            str(tmp_path),
            "strategies",
            "preview",
            "strategy-1",
            "--start",
            "2026-02-01T00:00:00Z",
            "--end",
            "2026-02-02T00:00:00Z",
            "--interval",
            "1h",
            "--instrument-id",
            "instrument-1",
            "--examples",
            "2",
        ]
    )
    signals_code = main(
        [
            "--log-root",
            str(tmp_path),
            "strategies",
            "preview",
            "strategy-1",
            "--start",
            "2026-02-01T00:00:00Z",
            "--end",
            "2026-02-02T00:00:00Z",
            "--interval",
            "1h",
            "--instrument-id",
            "instrument-1",
            "--signals",
        ]
    )
    full_code = main(
        [
            "--log-root",
            str(tmp_path),
            "strategies",
            "preview",
            "strategy-1",
            "--start",
            "2026-02-01T00:00:00Z",
            "--end",
            "2026-02-02T00:00:00Z",
            "--interval",
            "1h",
            "--instrument-id",
            "instrument-1",
            "--full",
        ]
    )
    compare_code = main(
        [
            "--log-root",
            str(tmp_path),
            "strategies",
            "preview-compare",
            "--start",
            "2026-02-01T00:00:00Z",
            "--end",
            "2026-02-02T00:00:00Z",
            "--interval",
            "1h",
            "--case",
            "btc=strategy-btc:instrument-btc",
            "--case",
            "eth=strategy-eth:instrument-eth",
        ]
    )

    assert summary_code == 0
    assert signals_code == 0
    assert full_code == 0
    assert compare_code == 0
    assert calls == [
        (
            "POST",
            "/api/strategies/strategy-1/preview/summary",
            {
                "start": "2026-02-01T00:00:00Z",
                "end": "2026-02-02T00:00:00Z",
                "interval": "1h",
                "instrument_ids": ["instrument-1"],
                "max_examples": 2,
                "include_signals": False,
            },
        ),
        (
            "POST",
            "/api/strategies/strategy-1/preview/summary",
            {
                "start": "2026-02-01T00:00:00Z",
                "end": "2026-02-02T00:00:00Z",
                "interval": "1h",
                "instrument_ids": ["instrument-1"],
                "max_examples": 5,
                "include_signals": True,
            },
        ),
        (
            "POST",
            "/api/strategies/strategy-1/preview",
            {
                "start": "2026-02-01T00:00:00Z",
                "end": "2026-02-02T00:00:00Z",
                "interval": "1h",
                "instrument_ids": ["instrument-1"],
            },
        ),
        (
            "POST",
            "/api/strategies/preview/compare",
            {
                "start": "2026-02-01T00:00:00Z",
                "end": "2026-02-02T00:00:00Z",
                "interval": "1h",
                "cases": [
                    {"strategy_id": "strategy-btc", "instrument_ids": ["instrument-btc"], "label": "btc"},
                    {"strategy_id": "strategy-eth", "instrument_ids": ["instrument-eth"], "label": "eth"},
                ],
                "max_examples": 5,
                "include_signals": False,
            },
        ),
    ]


def test_research_check_cli_accepts_run_id_for_report_backed_checks(tmp_path, monkeypatch):
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
            "--log-root",
            str(tmp_path),
            "research",
            "check",
            "signal",
            "--title",
            "Run signal check",
            "--run-id",
            "run-1",
            "--detector-json",
            '{"type":"run_signal_match","output_name":"confirmed_balance_breakout"}',
            "--bucket-by",
            "symbol,event_key",
            "--max-examples",
            "10",
            "--min-sample-count",
            "1",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/research/checks/run",
        "body": {
            "title": "Run signal check",
            "check_family": "run_signal_summary",
            "scope": {"run_id": "run-1"},
            "detector": {"type": "run_signal_match", "output_name": "confirmed_balance_breakout"},
            "outcomes": {
                "bucket_by": ["symbol", "event_key"],
                "max_examples": 10,
                "min_sample_count": 1,
            },
        },
    }


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
                "economic_claim_intent": "exploration",
            },
    }


def test_bots_start_supports_opt_in_backtest_profiling(tmp_path, monkeypatch):
    observed = {}

    def fake_urlopen(request, timeout):
        _ = timeout
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
            "--run-type",
            "backtest",
            "--dataset-id",
            "mds-1",
            "--profile",
        ]
    )

    assert exit_code == 0
    assert observed["body"] == {
        "run_type": "backtest",
        "dataset_id": "mds-1",
        "profile": True,
        "economic_claim_intent": "exploration",
    }


def test_reports_semantic_inspection_commands_use_backend_routes(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        calls.append((request.get_method(), parsed.path, urllib.parse.parse_qs(parsed.query)))
        return _Response(b'{"ok": true}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    assert main(["reports", "instruments", "run-1"]) == 0
    assert main(["reports", "symbol-summary", "run-1"]) == 0
    assert main(["reports", "trades", "run-1", "--symbol", "BTC/USD", "--limit", "25"]) == 0
    assert main(["reports", "decisions", "run-1", "--state", "rejected"]) == 0
    assert main(["reports", "candle-catalog", "run-1"]) == 0

    assert calls == [
        ("GET", "/api/reports/run-1/instruments", {}),
        ("GET", "/api/reports/run-1/symbol-summary", {}),
        ("GET", "/api/reports/run-1/trades", {"limit": ["25"], "offset": ["0"], "symbol": ["BTC/USD"]}),
        ("GET", "/api/reports/run-1/decisions", {"limit": ["100"], "offset": ["0"], "state": ["rejected"]}),
        ("GET", "/api/reports/run-1/candles/catalog", {}),
    ]


def test_indicators_commands_use_backend_routes(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        body = json.loads(request.data.decode("utf-8")) if request.data else None
        calls.append((request.get_method(), parsed.path, urllib.parse.parse_qs(parsed.query), body))
        if parsed.path == "/api/indicators/types":
            return _Response(b'["candle_stats"]')
        if parsed.path == "/api/indicators/":
            if request.get_method() == "GET":
                return _Response(b'[{"id":"ind-1","type":"candle_stats"}]')
            return _Response(b'{"instance":{"id":"ind-2"}}')
        if parsed.path == "/api/indicators/validate-config":
            return _Response(b'{"instance":{"id":"planned"},"outputs":{"typed":[]}}')
        return _Response(b'{"ok": true}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    assert main(["indicators", "types"]) == 0
    assert main(["indicators", "list"]) == 0
    assert main(["indicators", "validate-config", "--type", "candle_stats", "--param", "warmup_bars=20"]) == 0
    assert main(["indicators", "create", "--type", "candle_stats", "--params-json", '{"warmup_bars":20}']) == 0
    assert (
        main(
            [
                "indicators",
                "validate-runtime",
                "ind-1",
                "--instrument-id",
                "inst-1",
                "--start",
                "2026-01-01T00:00:00Z",
                "--end",
                "2026-01-02T00:00:00Z",
                "--interval",
                "1h",
                "--require-ready-by-end",
                "--min-ready-bars",
                "12",
            ]
        )
        == 0
    )

    assert calls == [
        ("GET", "/api/indicators/types", {}, None),
        ("GET", "/api/indicators/", {}, None),
        ("POST", "/api/indicators/validate-config", {}, {"type": "candle_stats", "params": {"warmup_bars": 20}}),
        ("POST", "/api/indicators/validate-config", {}, {"type": "candle_stats", "params": {"warmup_bars": 20}}),
        (
            "POST",
            "/api/indicators/ind-1/runtime-validation",
            {},
            {
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-02T00:00:00Z",
                "interval": "1h",
                "instrument_id": "inst-1",
                "require_ready_by_end": True,
                "min_ready_bars": 12,
            },
        ),
    ]


def test_indicator_mutation_commands_use_backend_routes(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        body = json.loads(request.data.decode("utf-8")) if request.data else None
        calls.append((request.get_method(), parsed.path, urllib.parse.parse_qs(parsed.query), body))
        if parsed.path == "/api/indicators/ind-1":
            if request.get_method() == "GET":
                return _Response(
                    b'{"instance":{"id":"ind-1","type":"candle_stats","name":"Base","params":{"warmup_bars":200},"dependencies":[],"color":"#fff"}}'
                )
            if request.get_method() == "PUT":
                return _Response(b'{"instance":{"id":"ind-1","type":"candle_stats"}}')
        if parsed.path == "/api/indicators/ind-1/strategies":
            return _Response(b"[]")
        if parsed.path == "/api/indicators/validate-config":
            return _Response(b'{"instance":{"id":"planned"},"outputs":{"typed":[]}}')
        if parsed.path == "/api/indicators/":
            return _Response(b'{"instance":{"id":"ind-2"}}')
        if parsed.path == "/api/indicators/ind-1/enabled":
            return _Response(b'{"instance":{"id":"ind-1","enabled":true}}')
        if parsed.path == "/api/indicators/ind-2":
            return _Response(b"")
        return _Response(b'{"ok": true}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    assert main(["indicators", "clone", "ind-1", "--name", "Fast", "--param", "warmup_bars=80"]) == 0
    assert main(["indicators", "clone", "ind-1", "--name", "Fast", "--param", "warmup_bars=80", "--apply", "--confirm"]) == 0
    assert main(["indicators", "edit", "ind-1", "--name", "Base renamed"]) == 0
    assert main(["indicators", "edit", "ind-1", "--name", "Base renamed", "--apply", "--confirm"]) == 0
    assert main(["indicators", "on", "ind-1"]) == 0
    assert main(["indicators", "off", "ind-1"]) == 0
    assert main(["indicators", "rm", "ind-2", "--confirm"]) == 0

    assert calls[0:2] == [
        ("GET", "/api/indicators/ind-1", {}, None),
        (
            "POST",
            "/api/indicators/validate-config",
            {},
            {
                "type": "candle_stats",
                "name": "Fast",
                "params": {"warmup_bars": 80},
                "dependencies": [],
                "color": "#fff",
                "color_palette": None,
            },
        ),
    ]
    assert ("POST", "/api/indicators/", {}, {
        "type": "candle_stats",
        "name": "Fast",
        "params": {"warmup_bars": 80},
        "dependencies": [],
        "color": "#fff",
        "color_palette": None,
    }) in calls
    assert ("PUT", "/api/indicators/ind-1", {}, {
        "type": "candle_stats",
        "name": "Base renamed",
        "params": {"warmup_bars": 200},
        "dependencies": [],
        "color": "#fff",
        "color_palette": None,
    }) in calls
    assert ("PATCH", "/api/indicators/ind-1/enabled", {}, {"enabled": True}) in calls
    assert ("PATCH", "/api/indicators/ind-1/enabled", {}, {"enabled": False}) in calls
    assert ("DELETE", "/api/indicators/ind-2", {}, None) in calls


def test_data_coverage_command_uses_backend_route(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        body = json.loads(request.data.decode("utf-8")) if request.data else None
        calls.append((request.get_method(), parsed.path, body))
        return _Response(b'{"schema_version":"candle_coverage_preflight.v1","status":"warning"}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    assert (
        main(
            [
                "data",
                "coverage",
                "--symbol",
                "BTC/USD",
                "--datasource",
                "CCXT",
                "--exchange",
                "coinbase",
                "--start",
                "2026-01-01T00:00:00Z",
                "--end",
                "2026-01-02T00:00:00Z",
                "--timeframe",
                "1h",
            ]
        )
        == 0
    )

    assert calls == [
        (
            "POST",
            "/api/candles/coverage",
            {
                "instrument_id": None,
                "symbol": "BTC/USD",
                "datasource": "CCXT",
                "exchange": "coinbase",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-02T00:00:00Z",
                "timeframe": "1h",
            },
        )
    ]


def test_instruments_commands_use_backend_routes(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        body = json.loads(request.data.decode("utf-8")) if request.data else None
        calls.append((request.get_method(), parsed.path, urllib.parse.parse_qs(parsed.query), body))
        if parsed.path == "/api/instruments/":
            return _Response(b'[{"id":"inst-1","symbol":"BTC/USD","datasource":"CCXT","exchange":"coinbase"}]')
        return _Response(b'{"ok": true}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    assert main(["instruments", "list", "--datasource", "CCXT", "--symbol", "BTC"]) == 0
    assert main(["instruments", "profile", "inst-1", "--execution-semantics", "proxy_derivative"]) == 0
    assert main(["instruments", "resolve", "--symbol", "ETH/USD", "--provider", "CCXT", "--venue", "coinbase"]) == 0
    assert (
        main(
            [
                "instruments",
                "coverage-matrix",
                "--start",
                "2026-01-01T00:00:00Z",
                "--end",
                "2026-01-02T00:00:00Z",
                "--timeframe",
                "1h",
                "--instrument-id",
                "inst-1",
                "--symbol",
                "BTC/USD",
                "--datasource",
                "CCXT",
                "--exchange",
                "coinbase",
                "--instrument-type",
                "spot",
                "--runtime-ready",
                "true",
                "--research-ready",
                "false",
                "--execution-semantics",
                "proxy_derivative",
            ]
        )
        == 0
    )

    assert calls == [
        ("GET", "/api/instruments/", {}, None),
        ("GET", "/api/instruments/inst-1/runtime-profile", {"execution_semantics": ["proxy_derivative"]}, None),
        (
            "POST",
            "/api/instruments/resolve",
            {},
            {
                "symbol": "ETH/USD",
                "datasource": None,
                "exchange": None,
                "provider_id": "CCXT",
                "venue_id": "coinbase",
                "force_refresh": False,
            },
        ),
        (
            "POST",
            "/api/instruments/coverage-matrix",
            {},
            {
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-02T00:00:00Z",
                "timeframe": "1h",
                "instrument_ids": ["inst-1"],
                "symbol": "BTC/USD",
                "datasource": "CCXT",
                "exchange": "coinbase",
                "instrument_type": "spot",
                "runtime_ready": True,
                "research_ready": False,
                "execution_semantics": "proxy_derivative",
            },
        ),
    ]


def test_experiments_prepare_instrument_matrix_creates_solo_bots_and_plan(tmp_path, monkeypatch):
    calls = []
    created_strategy_ids = iter(["strategy-spot", "strategy-derivative"])
    created_bot_ids = iter(["bot-spot", "bot-derivative"])
    instruments = {
        "spot-1": {
            "id": "spot-1",
            "symbol": "BTC/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
            "instrument_type": "spot",
        },
        "derivative-1": {
            "id": "derivative-1",
            "symbol": "BIP-20DEC30-CDE",
            "datasource": "COINBASE",
            "exchange": "coinbase_direct",
            "instrument_type": "future",
        },
    }

    def fake_urlopen(request, timeout):
        _ = timeout
        parsed = urllib.parse.urlparse(request.full_url)
        path = parsed.path
        body = json.loads(request.data.decode("utf-8")) if request.data else None
        calls.append((request.get_method(), path, urllib.parse.parse_qs(parsed.query), body))
        if path == "/api/bots/source-bot" and request.get_method() == "GET":
            return _Response(
                json.dumps(
                    {
                        "id": "source-bot",
                        "name": "Source bot",
                        "strategy_id": "strategy-source",
                        "strategy_variant_name": "default",
                        "atm_template_id": "atm-1",
                        "risk_config": {"max_risk": 0.01},
                        "mode": "instant",
                        "execution_mode": "fast",
                        "execution_behavior": "simulated",
                        "run_type": "backtest",
                        "wallet_config": {"balances": {"USD": 10000}},
                        "market_data_stream_policy": {},
                        "snapshot_interval_ms": 1000,
                        "bot_env": {},
                    }
                ).encode("utf-8")
            )
        if path == "/api/bots/source-bot/run-context":
            return _Response(b'{"strategy":{"strategy_id":"strategy-source","strategy_variant_name":"default"}}')
        if path == "/api/strategies/strategy-source":
            return _Response(
                json.dumps(
                    {
                        "strategy": {
                            "id": "strategy-source",
                            "name": "Source strategy",
                            "timeframe": "1h",
                            "datasource": "COINBASE",
                            "exchange": "coinbase_direct",
                            "atm_template_id": "atm-1",
                            "risk_config": {"max_risk": 0.01},
                        },
                    }
                ).encode("utf-8")
            )
        if path == "/api/strategies/strategy-source/bindings":
            return _Response(json.dumps({"bindings": {"indicator_ids": ["ind-1"]}}).encode("utf-8"))
        if path == "/api/strategies/strategy-source/rules":
            return _Response(
                json.dumps(
                    {
                        "rules": [
                            {
                                "id": "rule-1",
                                "name": "Long",
                                "intent": "enter_long",
                                "priority": 1,
                                "trigger": {"type": "always"},
                                "guards": [],
                                "enabled": True,
                            }
                        ]
                    }
                ).encode("utf-8")
            )
        if path == "/api/strategies/strategy-source/variants":
            return _Response(
                json.dumps(
                    {
                        "variants": [
                            {
                                "id": "variant-source-default",
                                "strategy_id": "strategy-source",
                                "name": "default",
                                "description": None,
                                "output_filters": [],
                                "is_default": True,
                            }
                        ]
                    }
                ).encode("utf-8")
            )
        if path.startswith("/api/instruments/") and path.endswith("/runtime-profile"):
            instrument_id = path.split("/")[3]
            semantics = urllib.parse.parse_qs(parsed.query).get("execution_semantics", [None])[0]
            return _Response(
                json.dumps(
                    {
                        "schema_version": "series_execution_profile.v1",
                        "instrument": {
                            "instrument_id": instrument_id,
                            "instrument_type": instruments[instrument_id]["instrument_type"],
                            "source_instrument_type": instruments[instrument_id]["instrument_type"],
                            "execution_semantics": semantics,
                        },
                    }
                ).encode("utf-8")
            )
        if path.startswith("/api/instruments/") and request.get_method() == "GET":
            instrument_id = path.split("/")[3]
            return _Response(json.dumps(instruments[instrument_id]).encode("utf-8"))
        if path == "/api/strategies/" and request.get_method() == "POST":
            strategy_id = next(created_strategy_ids)
            return _Response(
                json.dumps(
                    {
                        "strategy": {"id": strategy_id, "name": body["name"], "timeframe": body["timeframe"]},
                    }
                ).encode("utf-8")
            )
        if path.startswith("/api/strategies/") and path.endswith("/variants") and request.get_method() == "GET":
            strategy_id = path.split("/")[3]
            return _Response(
                json.dumps(
                    {
                        "variants": [
                            {
                                "id": f"{strategy_id}-default",
                                "strategy_id": strategy_id,
                                "name": "default",
                                "output_filters": [],
                                "is_default": True,
                            }
                        ]
                    }
                ).encode("utf-8")
            )
        if path.endswith("/rules") and request.get_method() == "POST":
            return _Response(b'{"ok":true}')
        if "/variants/" in path and request.get_method() == "PUT":
            return _Response(json.dumps({"id": path.rsplit("/", 1)[-1], "name": body["name"]}).encode("utf-8"))
        if path == "/api/bots" and request.get_method() == "POST":
            return _Response(json.dumps({"id": next(created_bot_ids), **body}).encode("utf-8"))
        raise AssertionError(f"unexpected API call: {request.get_method()} {request.full_url}")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    plan_path = tmp_path / "matrix_plan.json"
    request_payload = {
        "schema_version": "instrument_matrix_experiment_request.v1",
        "name": "btc-matrix",
        "source_bot_id": "source-bot",
        "window": {
            "id": "six_months",
            "start": "2025-10-04T00:00:00Z",
            "end": "2026-04-04T23:59:59Z",
        },
        "groups": [
            {
                "id": "btc",
                "label": "BTC",
                "spot_instrument_id": "spot-1",
                "derivative_instrument_id": "derivative-1",
            }
        ],
    }

    exit_code = main(
        [
            "--log-root",
            str(tmp_path),
            "experiments",
            "prepare-instrument-matrix",
            "--request-json",
            json.dumps(request_payload),
            "--out",
            str(plan_path),
            "--apply",
            "--confirm",
        ]
    )

    assert exit_code == 0
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert [variant["bot_id"] for variant in plan["variants"]] == ["bot-spot", "bot-derivative"]
    assert plan["comparisons"] == [
        {
            "aggregate_summary": True,
            "baseline_variant_id": "btc_derivative",
            "candidate_variant_id": "btc_spot_proxy",
            "compare_per_window": True,
            "id": "btc_spot_proxy_vs_derivative",
        }
    ]
    bot_payloads = [call[3] for call in calls if call[0] == "POST" and call[1] == "/api/bots"]
    assert bot_payloads[0]["execution_semantics"] == "proxy_derivative"
    assert bot_payloads[0]["strategy_id"] == "strategy-spot"
    assert bot_payloads[1]["execution_semantics"] == "derivative"


def test_experiments_start_bot_writes_resumable_record(tmp_path, monkeypatch):
    def fake_urlopen(request, timeout):
        _ = timeout
        assert request.get_method() == "POST"
        assert urllib.parse.urlparse(request.full_url).path == "/api/bots/bot-1/runs/start"
        assert json.loads(request.data.decode("utf-8")) == {
            "run_type": "backtest",
            "dataset_id": "mds-1",
            "request_id": "req-1",
            "economic_claim_intent": "exploration",
        }
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
            "--dataset-id",
            "mds-1",
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
                        "contract_version": "run_report.v2",
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
