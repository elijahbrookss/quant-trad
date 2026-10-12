from datetime import UTC, datetime
import os
from pathlib import Path
import subprocess

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_lifecycle
from tests.test_market_data.test_fact_raw_lineage_db import _raw_trade_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


def test_archive_expiration_indexes_are_explicit_compatible_and_recoverable(storage, tmp_path, monkeypatch, caplog):
    fixture = _raw_trade_fixture(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    names = ("ix_market_dataset_archive_ref_manifest", "ix_market_raw_archive_expiration_order")
    script = Path(__file__).resolve().parents[2] / "scripts/db/manual_add_archive_expiration_indexes_v1.sql"
    url = make_url(storage.dsn)
    env = {
        **os.environ,
        "PGHOST": url.host or "",
        "PGPORT": str(url.port or 5432),
        "PGUSER": url.username or "",
        "PGPASSWORD": url.password or "",
        "PGDATABASE": url.database or "",
    }

    def operator(sql=None):
        args = ["psql", "--no-psqlrc", "--set", "ON_ERROR_STOP=1"]
        args += ["--command", sql] if sql else ["--file", str(script)]
        return subprocess.run(args, env=env, text=True, capture_output=True, timeout=30)

    def definitions():
        with storage.database.session() as session:
            return dict(session.execute(text(
                "SELECT indexname,indexdef FROM pg_indexes "
                "WHERE schemaname='market' AND indexname=ANY(:names)"
            ), {"names": list(names)}).all())

    def candidates():
        cutoff = datetime(2027, 1, 1, tzinfo=UTC)
        return market_lifecycle.market_storage_lifecycle_repository.list_archive_expiration_candidates(
            raw_trade_cutoff=cutoff,
            raw_l2_cutoff=cutoff,
            checkpoint_cutoff=cutoff,
            compacted_source_cutoff=cutoff,
            limit=100,
        )

    initial = definitions()
    assert set(initial) == set(names), "clean bootstrap must declare both indexes"
    expected = candidates()
    assert {row["target_id"] for row in expected} == set(fixture.manifests)
    for _ in range(2):
        result = operator()
        assert result.returncode == 0, result.stderr
    assert definitions() == initial

    # Existing installations remain compatible, but startup must not build these
    # historical indexes implicitly or remove their old data.
    with storage.database.session() as session:
        for name in names:
            session.execute(text(f"DROP INDEX market.{name}"))
    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
    finally:
        restarted._reset_engine()
    assert definitions() == {}
    assert "portal_db_operator_index_missing" in caplog.text
    assert candidates() == expected

    # Refuse an incompatible second name before creating the missing first index.
    result = operator(f"CREATE INDEX {names[1]} ON market.raw_archive_manifests(id)")
    assert result.returncode == 0, result.stderr
    wrong = definitions()
    result = operator()
    assert result.returncode != 0 and "archive_expiration_index_incompatible" in result.stderr
    assert definitions() == wrong and names[0] not in wrong
    assert operator(f"DROP INDEX market.{names[1]}").returncode == 0

    # A failed concurrent build really leaves an invalid relation. The operator
    # refuses it and preserves it for explicit recovery instead of auto-dropping.
    result = operator(f"CREATE UNIQUE INDEX CONCURRENTLY {names[1]} ON market.raw_archive_manifests(definition_id)")
    assert result.returncode != 0
    with storage.database.session() as session:
        assert session.execute(text(
            "SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass(:name)"
        ), {"name": "market." + names[1]}).scalar_one() is False
    invalid = definitions()
    result = operator()
    assert result.returncode != 0 and "archive_expiration_index_incompatible" in result.stderr
    assert definitions() == invalid
    assert operator(f"DROP INDEX market.{names[1]}").returncode == 0
    result = operator()
    assert result.returncode == 0, result.stderr
    assert definitions() == initial and candidates() == expected
