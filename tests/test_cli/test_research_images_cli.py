import base64
import json
from types import SimpleNamespace

import pytest

from cli import main


class Client:
    def __init__(self):
        self.calls = []

    def request_json(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        return {"image_id": "saved"}

    def request_bytes(self, method, path):
        self.calls.append((method, path, {}))
        return SimpleNamespace(body=b"server bytes")


def test_image_upload_transfers_bytes_with_explicit_metadata(tmp_path, monkeypatch):
    client = Client()
    monkeypatch.setattr(main, "_client", lambda _: client)
    image = tmp_path / "local.png"
    image.write_bytes(b"local bytes")
    metadata = {"title": "Chart", "explanation": "Limitations", "alt_text": "Source path",
                "binding": {"result_hash": "result", "evidence_hash": "evidence"}}
    request = tmp_path / "request.json"
    request.write_text(json.dumps(metadata))
    args = SimpleNamespace(image_action="attach", item_id="check", file=str(image), request_json=str(request))
    assert main._cmd_research_images(args) == 0
    assert client.calls == [("POST", "/api/research/items/check/images", {"payload": {
        **metadata, "content_base64": base64.b64encode(b"local bytes").decode()}})]
    request.write_text(json.dumps(metadata | {"content_base64": "ignored"}))
    with pytest.raises(ValueError, match="metadata only"):
        main._cmd_research_images(args)


def test_download_uses_server_bytes_and_preserves_existing_file(tmp_path, monkeypatch):
    client = Client()
    monkeypatch.setattr(main, "_client", lambda _: client)
    output = tmp_path / "download.png"
    args = SimpleNamespace(image_action="download", item_id="study", image_id="image", output=str(output))
    main._cmd_research_images(args)
    assert output.read_bytes() == b"server bytes"
    output.write_bytes(b"existing evidence")
    with pytest.raises(FileExistsError):
        main._cmd_research_images(args)
    assert output.read_bytes() == b"existing evidence"
