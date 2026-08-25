from __future__ import annotations

from pathlib import Path

from scripts.docs import build_architecture_index as architecture_index


def test_architecture_docs_have_required_frontmatter_tags():
    """QT-PROOF-316 selector: validate the complete adopted metadata model."""

    catalog = architecture_index.build_catalog(Path.cwd())

    assert catalog.components, "expected architecture component docs"
    repo_paths = [entry.repo_path for entry in catalog.components]
    components = [entry.component for entry in catalog.components]
    assert len(repo_paths) == len(set(repo_paths))
    assert len(components) == len(set(components))
    assert set(catalog.legacy_paths) == {
        entry.repo_path
        for entry in catalog.components
        if entry.metadata_version == 1
    }


def test_architecture_index_exactly_matches_validated_catalog():
    catalog = architecture_index.build_catalog(Path.cwd())
    expected = architecture_index.render_index(catalog).encode("utf-8")
    actual = Path(
        "docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md"
    ).read_bytes()

    assert actual == expected


def test_architecture_index_references_runtime_composition_doc():
    index_text = Path(
        "docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md"
    ).read_text(encoding="utf-8")
    assert "RUNTIME_COMPOSITION_ROOT.md" in index_text
    assert "portal/backend/service/bots/runtime_composition.py" in index_text
