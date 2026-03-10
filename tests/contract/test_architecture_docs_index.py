from __future__ import annotations

from pathlib import Path

REQUIRED_KEYS = {
    "component:",
    "subsystem:",
    "layer:",
    "doc_type:",
    "status:",
    "tags:",
    "code_paths:",
}


def test_architecture_docs_have_required_frontmatter_tags():
    architecture_docs = list(Path("docs/architecture").glob("*.md"))
    assert architecture_docs, "expected architecture docs"

    missing: list[str] = []
    for path in architecture_docs:
        if path.name == "ARCHITECTURE_COMPONENT_INDEX.md":
            continue
        text = path.read_text(encoding="utf-8")
        if not text.startswith("---\n"):
            missing.append(f"{path}:missing-frontmatter")
            continue
        frontmatter = text.split("\n---\n", 1)[0]
        for key in REQUIRED_KEYS:
            if key not in frontmatter:
                missing.append(f"{path}:missing-{key}")

    assert missing == [], "architecture metadata coverage gaps: " + ", ".join(missing)


def test_architecture_index_references_runtime_composition_doc():
    index_text = Path("docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md").read_text(encoding="utf-8")
    assert "RUNTIME_COMPOSITION_ROOT.md" in index_text
    assert "portal/backend/service/bots/runtime_composition.py" in index_text
