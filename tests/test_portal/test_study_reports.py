from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace

import pytest

from portal.backend.service.research import reports


@pytest.mark.parametrize("html", [
    '<script>alert(1)</script>', '<svg><script/></svg>', '<iframe src="/"/>',
    '<img src="https://attacker/a" alt="x">', '<p onclick="alert(1)">x</p>',
    '<a href="javascript:alert(1)">x</a>', '<style>body{background:url(https://attacker)}</style>',
    '<base href="https://attacker">', '<form action="/"></form>', '<meta http-equiv="refresh">',
    '<p style="background:url(https://attacker)">x</p>', '<p><div>x</p></div>',
    '<img src="data:image/png;base64,AAAA" alt="x">', '<a href="/../../etc/passwd">x</a>',
])
def test_reject_active_network_and_malformed_content(html):
    with pytest.raises(ValueError):
        reports.normalize_html(html)


def test_escape_text_and_reconstruct():
    assert reports.normalize_html('<p id="finding">A &lt; B &amp; C</p>') == '<p id="finding">A &lt; B &amp; C</p>'


def test_total_embedded_image_budget():
    import base64
    from io import BytesIO
    from PIL import Image
    stream = BytesIO()
    Image.new("RGB", (4, 4), "white").save(stream, format="PNG")
    image = '<img alt="Synthetic" src="data:image/png;base64,' + base64.b64encode(stream.getvalue()).decode() + '">'
    assert reports.normalize_html(image * 8)
    with pytest.raises(ValueError, match="eight images"):
        reports.normalize_html(image * 9)


@pytest.fixture
def memory(monkeypatch):
    item = {"id": "study", "kind": "study", "payload": {"legacy": "preserved", "question_contract": {
        "publications": [{"publication_hash": "a" * 64}]}}}
    class Record:
        payload = item["payload"]
        def to_dict(self):
            return deepcopy(item | {"payload": self.payload})
    record = Record()
    @contextmanager
    def session():
        yield SimpleNamespace(flush=lambda: None)
    monkeypatch.setattr(reports.db, "session", session)
    monkeypatch.setattr(reports.publication, "_locked", lambda *args: record)
    monkeypatch.setattr(reports.repository, "get_item", lambda *args, **kwargs: record.to_dict())
    monkeypatch.setattr(reports.publication, "_assert_access", lambda *args: None)
    def binding(item, publication_hash, session):
        if publication_hash not in [p["publication_hash"] for p in item["payload"]["question_contract"]["publications"]]:
            raise ValueError("binding_mismatch")
        return {"publication_hash": publication_hash, "revision": 1, "references": []}
    monkeypatch.setattr(reports, "_binding", binding)
    return record


def request():
    return {"title": "Report", "publication_hash": "a" * 64, "html": "<h1>Finding</h1><p>No edge established.</p>",
            "source_qmd": "# Finding", "build": {"quarto_version": "1.10.19", "source_revision": "b" * 40, "evidence_bundle_sha256": "c" * 64}}


def test_append_idempotence_binding_stale_integrity_and_limits(memory, monkeypatch):
    original = deepcopy(memory.payload)
    first = reports.attach("study", request())
    assert reports.attach("study", request()) == first
    assert memory.payload["question_contract"] == original["question_contract"]
    assert memory.payload["legacy"] == "preserved"
    assert "html" not in first and "source_qmd" not in first
    assert b"No edge established" in reports.get_content("study", first["report_id"])
    assert b"source_qmd" in reports.get_content("study", first["report_id"], source=True)
    with pytest.raises(KeyError):
        reports.get_content("study", "../etc/passwd")
    with pytest.raises(ValueError, match="binding_mismatch"):
        reports.attach("study", request() | {"publication_hash": "d" * 64})
    monkeypatch.setattr(reports, "MAX_REPORTS", 1)
    with pytest.raises(ValueError, match="study_report_limit"):
        reports.attach("study", request() | {"title": "Correction"})
    memory.payload["question_contract"]["publications"].append({"publication_hash": "d" * 64})
    assert reports.list_reports("study")["items"][0]["stale"]
    memory.payload[reports.KEY][0]["html"] = "<p>tampered</p>"
    with pytest.raises(ValueError, match="integrity"):
        reports.get_content("study", first["report_id"])


def test_http_mime_isolation_and_error_mapping(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from portal.backend.controller import research
    app = FastAPI()
    app.include_router(research.router)
    client = TestClient(app)
    monkeypatch.setattr(reports, "get_content", lambda *args, **kwargs: b"<p>report</p>")
    response = client.get("/items/study/reports/report/content")
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert "sandbox; default-src 'none'" in response.headers["content-security-policy"]
    response = client.get("/items/study/reports/report/source")
    assert response.headers["content-type"] == "application/json"
    monkeypatch.setattr(reports, "get_content", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("question_evidence_inaccessible")))
    assert client.get("/items/study/reports/report/content").status_code == 403
