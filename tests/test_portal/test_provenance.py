from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from portal.backend.service import provenance
from scripts.provenance import source_tree_hash


def test_evidence_revision_rejects_dirty_checkout_even_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_REVISION", "abc123")

    def fake_run(args, **_kwargs):
        stdout = "abc123\n" if "rev-parse" in args else " M producing.py\n"
        return SimpleNamespace(stdout=stdout)

    monkeypatch.setattr(provenance.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="dirty_source_forbidden"):
        provenance.evidence_source_revision()


def test_evidence_revision_rejects_configured_head_disagreement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_REVISION", "configured")

    def fake_run(args, **_kwargs):
        stdout = "checkout-head\n" if "rev-parse" in args else ""
        return SimpleNamespace(stdout=stdout)

    monkeypatch.setattr(provenance.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="source_revision_mismatch"):
        provenance.evidence_source_revision()


def test_image_evidence_requires_matching_build_source_attestation(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    revision = "abc123"
    tree_hash = "f" * 64
    monkeypatch.setenv("SOURCE_TREE_HASH", tree_hash)
    monkeypatch.setenv("QT_IMAGE_SOURCE_REVISION", revision)
    monkeypatch.setenv("QT_IMAGE_SOURCE_TREE_HASH", tree_hash)
    (tmp_path / ".qt-source-attestation.json").write_text(
        json.dumps(
            {
                "schema_version": "qt_source_attestation.v1",
                "source_revision": revision,
                "source_tree_hash": tree_hash,
            }
        ),
        encoding="utf-8",
    )

    assert provenance._verified_image_source_revision(tmp_path, revision) == revision

    monkeypatch.setenv("SOURCE_TREE_HASH", "0" * 64)
    with pytest.raises(RuntimeError, match="source_attestation_mismatch"):
        provenance._verified_image_source_revision(tmp_path, revision)


def test_source_tree_attestation_covers_runtime_dependencies(tmp_path) -> None:
    for relative in source_tree_hash.ROOTS:
        directory = tmp_path / relative
        directory.mkdir(parents=True)
        (directory / "material.txt").write_text(relative, encoding="utf-8")
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("numpy==1.0\n", encoding="utf-8")
    original = source_tree_hash.working_tree_hash(tmp_path)

    requirements.write_text("numpy==2.0\n", encoding="utf-8")

    assert source_tree_hash.working_tree_hash(tmp_path) != original
