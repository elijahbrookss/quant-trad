import json
from types import SimpleNamespace

from cli import main


def test_report_cli_uses_explicit_prebuilt_payload_and_encoded_item_path(tmp_path, monkeypatch):
    calls = []
    def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"report_id": "retained"}
    monkeypatch.setattr(main, "_client", lambda _: SimpleNamespace(request_json=request))
    payload = {"title": "Report", "html": "<p>Finding</p>", "source_qmd": "Finding", "publication_hash": "hash", "build": {}}
    local = tmp_path / "prebuilt.json"
    local.write_text(json.dumps(payload))
    assert main._cmd_research_reports(SimpleNamespace(report_action="attach", item_id="study/name", request_json=str(local))) == 0
    assert calls == [("POST", "/api/research/items/study%2Fname/reports", {"payload": payload})]
    assert main._cmd_research_reports(SimpleNamespace(report_action="list", item_id="study")) == 0
    assert calls[-1] == ("GET", "/api/research/items/study/reports", {})
    assert main._cmd_research_reports(SimpleNamespace(report_action="source", item_id="study/name", report_id="report/name")) == 0
    assert calls[-1] == ("GET", "/api/research/items/study%2Fname/reports/report%2Fname/source", {})


def test_mcp_study_report_parity_requires_confirmation():
    import pytest
    from cli.mcp_server import McpError, QuantTradMcpServer

    calls = []
    def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"items": [], "report_id": "retained"}
    server = QuantTradMcpServer(client_factory=lambda: SimpleNamespace(request_json=request))
    request_body = {"title": "Reviewed", "publication_hash": "exact"}
    with pytest.raises(McpError, match="confirm"):
        server.call_tool("attach_study_report", {"item_id": "study/name", "request": request_body})
    assert calls == []
    server.call_tool("attach_study_report", {"item_id": "study/name", "request": request_body, "confirm": True})
    assert calls[-1] == ("POST", "/api/research/items/study%2Fname/reports", {"payload": request_body})
    server.call_tool("list_study_reports", {"item_id": "study/name"})
    assert calls[-1] == ("GET", "/api/research/items/study%2Fname/reports", {})
    server.call_tool("get_study_report_source", {"item_id": "study/name", "report_id": "report/name"})
    assert calls[-1] == ("GET", "/api/research/items/study%2Fname/reports/report%2Fname/source", {})
