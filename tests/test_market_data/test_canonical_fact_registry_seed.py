from __future__ import annotations

from pathlib import Path

from scripts.db.render_canonical_fact_registry import render_registry_seed


def test_canonical_fact_registry_sql_seed_is_current() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    seed = repository_root / "scripts/db/canonical_fact_registry_seed_v1.sql"

    assert seed.read_text(encoding="utf-8") == render_registry_seed()
