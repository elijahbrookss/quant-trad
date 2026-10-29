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
