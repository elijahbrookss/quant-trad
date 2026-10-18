import base64
from contextlib import contextmanager
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace

from PIL import Image
import pytest

from portal.backend.service.research import images, publication


def png():
    stream = BytesIO()
    Image.new("RGB", (20, 10), "white").save(stream, format="PNG")
    return base64.b64encode(stream.getvalue()).decode()


@pytest.mark.parametrize("content", ["<html>bad</html>", "bm90IGFuIGltYWdl", "data:image/png;base64,AAAA", ""])
def test_reject_non_image(content):
    with pytest.raises(ValueError, match="research_image_invalid"):
        images.decode_image(content)


def test_image_size_and_decoder_validation(monkeypatch):
    encoded = png()
    content, manifest = images.decode_image(encoded)
    assert (manifest["width"], manifest["height"], manifest["media_type"]) == (20, 10, "image/png")
    assert manifest["byte_count"] == len(content)
    with pytest.raises(ValueError):
        images.decode_image(base64.b64encode(content[:-12]).decode())
    monkeypatch.setattr(images, "MAX_PIXELS", 100)
    with pytest.raises(ValueError, match="megapixels"):
        images.decode_image(encoded)


@pytest.fixture
def memory(monkeypatch):
    item = {"id": "study", "kind": "study", "payload": {"legacy": "preserved", publication.KEY: {
        "schema_version": publication.SCHEMA, "publications": [
            {"publication_hash": "pub-1", "revision": 1, "references": []}
        ]}}}
    rows = {}

    class Session:
        def scalar(self, _query):
            return SimpleNamespace(to_dict=lambda: deepcopy(item))

        def get(self, _model, identifier):
            return rows.get(identifier)

        def scalars(self, _query):
            return SimpleNamespace(all=lambda: list(rows.values()))

        def add(self, record):
            rows[record.id] = record

        def flush(self):
            pass

    @contextmanager
    def scope():
        yield Session()

    monkeypatch.setattr(images.db, "session", scope)
    monkeypatch.setattr(images.repository, "get_item", lambda *args, **kwargs: deepcopy(item))
    monkeypatch.setattr(publication, "verify_history", lambda *args: None)
    monkeypatch.setattr(publication, "_assert_access", lambda *args: None)
    return item, rows


def request():
    return {"title": "Source chart", "explanation": "Limited descriptive evidence.", "alt_text": "Observed path",
            "binding": {"publication_hash": "pub-1"}, "content_base64": png()}


def test_durable_bytes_idempotency_and_historical_preservation(memory):
    item, rows = memory
    before = deepcopy(item)
    first = images.attach("study", request())
    assert images.attach("study", request()) == first
    assert len(rows) == 1 and item == before
    content, summary = images.get_content("study", first["image_id"])
    assert content == base64.b64decode(request()["content_base64"])
    assert summary == first
    assert "content_base64" not in images.list_images("study")["items"][0]
    # Later publication preserves custody of the previous image/revision.
    item["payload"][publication.KEY]["publications"].append({"publication_hash": "pub-2", "revision": 2, "references": []})
    assert images.get_content("study", first["image_id"])[1]["binding"]["revision"] == 1


def test_binding_mismatch_and_item_limits_preserve_prior(memory, monkeypatch):
    first = images.attach("study", request())
    with pytest.raises(ValueError, match="publication not found"):
        images.attach("study", request() | {"binding": {"publication_hash": "wrong"}})
    monkeypatch.setattr(images, "MAX_IMAGES", 1)
    with pytest.raises(ValueError, match="research_image_limit"):
        images.attach("study", request() | {"title": "Another chart"})
    assert images.list_images("study")["items"] == [first]


def test_integrity_and_access_rechecked(memory, monkeypatch):
    _, rows = memory
    first = images.attach("study", request())
    record = rows[first["image_id"]]
    original = record.content
    record.content = b"corrupt"
    with pytest.raises(ValueError, match="stored image bytes"):
        images.get_content("study", record.id)
    record.content = original
    record.manifest = {**record.manifest, "explanation": "changed"}
    with pytest.raises(ValueError, match="manifest hash"):
        images.list_images("study")
    record.manifest = {key: value for key, value in first.items() if key not in {"created_at", "content_url"}}
    monkeypatch.setattr(publication, "_assert_access", lambda *args: (_ for _ in ()).throw(ValueError("question_evidence_inaccessible")))
    with pytest.raises(ValueError, match="inaccessible"):
        images.get_content("study", record.id)


def test_check_binding_pins_canonical_input_result_and_evidence(monkeypatch):
    from portal.backend.service.research import service
    monkeypatch.setattr(publication, "_assert_access", lambda *args: None)
    values = [SimpleNamespace(definition_hash="definition"), SimpleNamespace(request_hash="request"),
              SimpleNamespace(plan_hash="plan"), SimpleNamespace(evidence_hash="evidence", input_hash="input",
              input_binding={"dataset_id": "dataset", "dataset_hash": "frozen"}), SimpleNamespace(result_hash="result")]
    monkeypatch.setattr(service, "_validate_research_check_evidence_payload", lambda _: values)
    item = {"id": "check", "kind": "research_check", "payload": {}}
    binding = images._binding(item, {"result_hash": "result", "evidence_hash": "evidence"}, None)
    assert binding["input_binding"]["dataset_hash"] == "frozen"
    assert binding["input_hash"] == "input"
    with pytest.raises(ValueError, match="binding_mismatch"):
        images._binding(item, {"result_hash": "other", "evidence_hash": "evidence"}, None)


def test_content_route_static_headers_and_rejection(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from portal.backend.controller import research
    app = FastAPI()
    app.include_router(research.router)
    monkeypatch.setattr(images, "get_content", lambda *args: (b"png", {"media_type": "image/png"}))
    client = TestClient(app)
    result = client.get("/items/study/images/image/content")
    assert result.content == b"png" and result.headers["content-type"] == "image/png"
    assert result.headers["x-content-type-options"] == "nosniff"
    assert result.headers["cache-control"] == "no-store"
    assert "sandbox" in result.headers["content-security-policy"]
    result = client.post("/items/study/images", json=request() | {"url": "file:///elsewhere"})
    assert result.status_code == 422
