from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GLOSSARY = ROOT / "docs" / "contracts" / "platform" / "04_glossary.md"


def _text() -> str:
    return GLOSSARY.read_text(encoding="utf-8")


def _heading_anchor(heading: str) -> str:
    normalized = re.sub(r"[^a-z0-9 _-]", "", heading.lower())
    return re.sub(r"[ _]+", "-", normalized).strip("-")


def test_glossary_preserves_the_current_product_vocabulary() -> None:
    text = _text()

    assert text.index("## Start Here") < text.index("## Terms")
    assert text.index("## Terms") < text.index("## Aliases And Historical Usage")

    required_terms = (
        "Canonical Fact",
        "Fact revision",
        "Observation key",
        "Frozen Dataset",
        "Frozen Market Data Read Binding",
        "Research Observation",
        "Check",
        "Check preview",
        "RunResearchDataset",
        "Strategy definition / Compiled Strategy / effective strategy / run strategy snapshot",
        "Canonical Order Lifecycle / `FillOrder` / fill",
        "Wallet state / Wallet Ledger fact / wallet commit clock",
        "Deployment contract / strategy deployment authority",
    )
    for label in required_terms:
        assert re.search(rf"^### {re.escape(label)}$", text, flags=re.MULTILINE), label

    assert "execution event" in text.lower()
    assert "These records are not interchangeable" in text


def test_glossary_has_unique_stable_anchors_and_complete_current_counts() -> None:
    text = _text()
    anchors = re.findall(r'^<a id="([^"]+)"></a>$', text, flags=re.MULTILINE)
    headings = re.findall(r"^### (.+)$", text, flags=re.MULTILINE)
    terms = text.split("## Terms", 1)[1].split("## Aliases And Historical Usage", 1)[0]
    term_table, term_details = terms.split('<a id="', 1)
    table_anchors = re.findall(
        r"^\| \[[^]]+\]\(#([^)]+)\) \|", term_table, flags=re.MULTILINE
    )
    detail_anchors = re.findall(
        r'^<a id="([^"]+)"></a>$', '<a id="' + term_details, flags=re.MULTILINE
    )
    alias_text = text.split("## Aliases And Historical Usage", 1)[1]
    alias_anchors = re.findall(r'^<a id="([^"]+)"></a>$', alias_text, flags=re.MULTILINE)

    assert len(headings) == 73  # 53 current terms plus 20 alias rules
    assert len(anchors) == len(headings)
    assert len(anchors) == len(set(anchors))
    assert len(headings) == len(set(headings))
    assert table_anchors == detail_anchors
    assert len(detail_anchors) == 53
    assert len(alias_anchors) == 20
    for block in re.split(r'(?=^<a id="alias-)', alias_text, flags=re.MULTILINE)[1:]:
        assert "**Use:**" in block
        assert "**Related terms:**" in block
        assert "**Defined by:**" in block


def test_glossary_links_resolve_inside_the_repository() -> None:
    text = _text()
    anchors = set(re.findall(r'^<a id="([^"]+)"></a>$', text, flags=re.MULTILINE))

    for target in re.findall(r"\[[^]]+\]\(([^)]+)\)", text):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        if target.startswith("#"):
            assert target[1:] in anchors, target
            continue
        path_text, _, fragment = target.partition("#")
        resolved = (GLOSSARY.parent / path_text).resolve()
        assert resolved.is_file(), target
        if not fragment:
            continue
        if resolved == GLOSSARY.resolve():
            assert fragment in anchors, target
            continue
        target_text = resolved.read_text(encoding="utf-8")
        external_anchors = {
            _heading_anchor(heading)
            for heading in re.findall(r"^#{1,6} (.+)$", target_text, flags=re.MULTILINE)
        }
        external_anchors.update(
            re.findall(r'^<a id="([^"]+)"></a>$', target_text, flags=re.MULTILINE)
        )
        assert fragment in external_anchors, target
