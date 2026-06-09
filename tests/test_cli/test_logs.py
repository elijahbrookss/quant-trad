from __future__ import annotations

import json
import urllib.parse
import urllib.request

from cli.logs import DEFAULT_RUN_SELECTORS, doctor_log_payload, LokiClient
from cli.logs import parse_log_line
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


def test_parse_log_line_extracts_structured_context():
    parsed = parse_log_line(
        "2026-06-01 05:29:47,103 WARNING runner_observability.py:507 | "
        "docker_lifecycle_event | runner_id=backend.quanttrad | "
        "container_name=quant-trad-bots-bot-1 | action=die | exit_code=1 | "
        "bot_id=bot-1 | container_family=bot"
    )

    assert parsed["event"] == "docker_lifecycle_event"
    assert parsed["level"] == "WARNING"
    assert parsed["source"] == "runner_observability.py:507"
    assert parsed["fields"]["action"] == "die"
    assert parsed["fields"]["exit_code"] == "1"
    assert parsed["fields"]["bot_id"] == "bot-1"


def test_default_run_selectors_use_bounded_routing_labels():
    assert DEFAULT_RUN_SELECTORS == ('{service="bot-runtime"}', '{service="backend"}', '{service="docker-events"}')


def test_logs_run_queries_run_and_bot_lifecycle(monkeypatch, capsys):
    calls: list[str] = []

    def fake_urlopen(request, timeout):
        _ = timeout
        params = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
        query = params["query"][0]
        calls.append(query)
        if query.startswith('{app="quant_trad"}'):
            body = {"status": "success", "data": {"result": []}}
        elif "run-1" in query:
            body = {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "stream": {"job": "quanttrad", "service": "backend"},
                            "values": [
                                [
                                    "1780291772947858422",
                                    "2026-06-01 05:29:32,947 INFO  runner.py:239 | "
                                    "docker_bot_runner_start | bot_id=bot-1 | run_id=run-1",
                                ]
                            ],
                        }
                    ]
                },
            }
        else:
            body = {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "stream": {"job": "quanttrad", "service": "backend"},
                            "values": [
                                [
                                    "1780291787104172949",
                                    "2026-06-01 05:29:47,103 WARNING runner_observability.py:507 | "
                                    "docker_lifecycle_event | action=die | exit_code=1 | bot_id=bot-1",
                                ]
                            ],
                        }
                    ]
                },
            }
        return _Response(json.dumps(body).encode("utf-8"))

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--no-audit-log",
            "logs",
            "--loki-url",
            "http://loki.test:3100",
            "run",
            "run-1",
            "--start",
            "2026-06-01T05:25:00Z",
            "--end",
            "2026-06-01T05:40:00Z",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert calls == [
        '{service="bot-runtime"} |= "run-1"',
        '{service="backend"} |= "run-1"',
        '{service="docker-events"} |= "run-1"',
        '{service="bot-runtime"} |= "bot-1" |= "docker_lifecycle_event"',
        '{service="backend"} |= "bot-1" |= "docker_lifecycle_event"',
        '{service="docker-events"} |= "bot-1" |= "docker_lifecycle_event"',
    ]
    assert payload["schema_version"] == "qt_loki_run_logs.v1"
    assert payload["selectors"] == ['{service="bot-runtime"}', '{service="backend"}', '{service="docker-events"}']
    assert payload["bot_ids"] == ["bot-1"]
    assert payload["summary"]["events"]["docker_lifecycle_event"] == 1
    assert [entry["parsed"]["event"] for entry in payload["entries"]] == [
        "docker_bot_runner_start",
        "docker_lifecycle_event",
    ]


def test_logs_query_uses_raw_logql(monkeypatch, capsys):
    observed = {}

    def fake_urlopen(request, timeout):
        _ = timeout
        params = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
        observed["query"] = params["query"][0]
        return _Response(json.dumps({"status": "success", "data": {"result": []}}).encode("utf-8"))

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--no-audit-log",
            "logs",
            "--loki-url",
            "http://loki.test:3100",
            "query",
            '{service="backend"} |= "docker_lifecycle_event"',
            "--lookback-hours",
            "1",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert observed["query"] == '{service="backend"} |= "docker_lifecycle_event"'
    assert payload["schema_version"] == "qt_loki_query_logs.v1"
    assert payload["summary"]["entries"] == 0


def test_logs_doctor_checks_loki_label_visibility(monkeypatch):
    def fake_urlopen(request, timeout):
        _ = timeout
        path = urllib.parse.urlparse(request.full_url).path
        if path == "/ready":
            return _Response(b"ready")
        if path.endswith("/label/job/values"):
            body = {"status": "success", "data": ["quanttrad"]}
        elif path.endswith("/label/service/values"):
            body = {"status": "success", "data": ["backend", "bot-runtime", "docker-events"]}
        elif path.endswith("/label/runtime/values"):
            body = {"status": "success", "data": ["bot"]}
        else:
            raise AssertionError(f"unexpected URL {request.full_url}")
        return _Response(json.dumps(body).encode("utf-8"))

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    payload = doctor_log_payload(
        client=LokiClient("http://loki.test:3100"),
        start="2026-06-01T00:00:00Z",
        end="2026-06-01T01:00:00Z",
    )

    assert payload["schema_version"] == "qt_loki_doctor.v1"
    assert payload["status"] == "ok"
    assert payload["ingestion_contract"] == "docker_stdout_promtail_loki"
    assert payload["labels"]["services"] == ["backend", "bot-runtime", "docker-events"]
