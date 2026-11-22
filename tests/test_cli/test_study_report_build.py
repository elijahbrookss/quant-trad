"""Saved-evidence presentation only: synthetic inputs with no first-passage fields."""
import base64
import hashlib
from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

from PIL import Image
import pytest

from cli import study_reports as builder
from market_data.frozen import semantic_hash


def saved_bundle(path, **changes):
    publication = {"question_id": "synthetic-study", "revision": 1, "conclusion": "Descriptive only", "references": []}
    publication["publication_hash"] = semantic_hash(publication)
    files = {"records/selected-publication.json": json.dumps(publication).encode(),
             "manifest.json": json.dumps({"schema_version": "research_saved_evidence_bundle.v1", "item_id": "synthetic-study", "publication_hash": publication["publication_hash"]}).encode(),
             "README.txt": b"Synthetic saved records; no replay"}
    files.update(changes)
    files["checksums.sha256"] = "".join(hashlib.sha256(content).hexdigest() + "  " + name + "\n" for name, content in files.items()).encode()
    with ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return publication["publication_hash"]


def prepared_png(path):
    stream = BytesIO()
    Image.new("RGB", (20, 12), "teal").save(stream, format="PNG")
    path.write_bytes(stream.getvalue())
    return stream.getvalue()


def test_generic_second_shape_roundtrip_without_research(tmp_path, monkeypatch):
    bundle = tmp_path / "saved.zip"
    publication_hash = saved_bundle(bundle)
    qmd = tmp_path / "authored.qmd"
    qmd.write_text("# Coverage\nDescriptive only.\n![Retained coverage](coverage.png)\n")
    png = tmp_path / "coverage.png"
    content = prepared_png(png)
    monkeypatch.setattr(builder, "source_revision", lambda: "a" * 40)
    def render(command, directory):
        assert (directory / "coverage.png").read_bytes() == content
        if "render" in command:
            assert "--no-execute" in command and "html" in command
            (directory / "report.html").write_text('<html><body><h1>Coverage</h1><p>Descriptive only.</p><img src="coverage.png" alt="Retained coverage"></body></html>')
        return SimpleNamespace(stdout="1.10.19\n")
    monkeypatch.setattr(builder, "render", render)
    payload = builder.build_report(bundle=bundle, source_qmd=qmd, assets=[png], quarto=Path("/tool/bin/quarto"),
                                   study_id="synthetic-study", publication_hash=publication_hash, title="Coverage report")
    assert "data:image/png;base64," + base64.b64encode(content).decode() in payload["html"]
    assert payload["source_qmd"] == qmd.read_text()
    assert payload["publication_hash"] == publication_hash
    assert payload["build"]["evidence_bundle_sha256"] == hashlib.sha256(bundle.read_bytes()).hexdigest()


@pytest.mark.parametrize("member", ["../secret", "/absolute", "a\\b"])
def test_bundle_rejects_unsafe_members(tmp_path, member):
    bundle = tmp_path / "bad.zip"
    publication_hash = saved_bundle(bundle, **{member: b"not extracted"})
    with pytest.raises(ValueError, match="unsafe"):
        builder.verify_bundle(bundle, "synthetic-study", publication_hash)


def test_checksum_inventory_and_exact_selection(tmp_path):
    bundle = tmp_path / "saved.zip"
    publication_hash = saved_bundle(bundle)
    with pytest.raises(ValueError, match="mismatch"):
        builder.verify_bundle(bundle, "other-study", publication_hash)
    with ZipFile(bundle, "a") as archive:
        archive.writestr("unlisted", b"unexpected")
    with pytest.raises(ValueError, match="incomplete"):
        builder.verify_bundle(bundle, "synthetic-study", publication_hash)


@pytest.mark.parametrize("src", ["../coverage.png", "/coverage.png", "https://example.invalid/coverage.png", "data:image/png;base64,AA==", "missing.png"])
def test_fragment_only_embeds_explicit_prepared_assets(tmp_path, src):
    parser = builder.Fragment(tmp_path, {"coverage.png": b"prepared"})
    with pytest.raises(ValueError, match="path"):
        parser.feed(f'<body><img src="{src}" alt="Coverage"></body>')


def test_prepared_assets_reject_aliases_symlinks_and_wrong_format(tmp_path):
    png = tmp_path / "coverage.png"
    prepared_png(png)
    with pytest.raises(ValueError, match="distinct"):
        builder.prepare_assets([png, png])
    alias = tmp_path / "alias.png"
    alias.symlink_to(png)
    with pytest.raises(ValueError, match="symlink"):
        builder.prepare_assets([alias])
    Image.new("RGB", (10, 10)).save(png, format="JPEG")
    with pytest.raises(ValueError, match="PNG"):
        builder.prepare_assets([png])


def test_caption_is_not_silently_treated_as_image_alt(tmp_path):
    parser = builder.Fragment(tmp_path, {"coverage.png": b"prepared"})
    with pytest.raises(ValueError, match="fig-alt"):
        parser.feed('<body><figure><img src="coverage.png"><figcaption>Coverage</figcaption></figure></body>')


def test_clean_source_required_and_failed_build_does_not_publish(tmp_path, monkeypatch):
    monkeypatch.setattr(builder.subprocess, "check_output", lambda *a, **k: " M cli/study_reports.py\n")
    with pytest.raises(ValueError, match="clean committed"):
        builder.source_revision()
    def failed(**kwargs):
        raise RuntimeError("render failed")
    monkeypatch.setattr(builder, "build_report", failed)
    args = SimpleNamespace(output=tmp_path / "upload.json", bundle=tmp_path / "bundle", source_qmd=tmp_path / "source", asset=[], quarto=Path("quarto"), study_id="id", publication_hash="hash", title="title")
    with pytest.raises(RuntimeError, match="render failed"):
        builder.write_report(args)
    assert not args.output.exists()
    args.output.write_text("preserved retry request")
    with pytest.raises(ValueError, match="output already exists"):
        builder.write_report(args)
    assert args.output.read_text() == "preserved retry request"


def test_incomplete_upload_write_is_not_visible_as_retry_payload(tmp_path, monkeypatch):
    args = SimpleNamespace(output=tmp_path / "upload.json", bundle=tmp_path / "bundle", source_qmd=tmp_path / "source", asset=[], quarto=Path("quarto"), study_id="id", publication_hash="hash", title="title")
    monkeypatch.setattr(builder, "build_report", lambda **kwargs: {"build": {}})
    def incomplete(payload, stream):
        stream.write('{"partial":')
        raise OSError("synthetic disk failure")
    monkeypatch.setattr(builder.json, "dump", incomplete)
    with pytest.raises(OSError, match="disk failure"):
        builder.write_report(args)
    assert not args.output.exists()
    assert list(tmp_path.iterdir()) == []
