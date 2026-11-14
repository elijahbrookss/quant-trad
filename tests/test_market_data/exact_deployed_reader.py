"""Qualify an exact Git-extracted source tree against a disposable restore."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


def check_deployed_source_reader(restored_url, objects, dataset_id, series, expected, *, latest_only=False):
    """Opt-in check using Git-extracted deployed code and the isolated result."""
    path = os.environ.get("QT_EXACT_OLD_SOURCE_ROOT")
    if path is None:
        return
    source = Path(path).resolve(strict=True)
    expected_hash = os.environ["QT_EXACT_OLD_SOURCE_HASH"]
    assert restored_url.host in {"restored", "timescaledb", "127.0.0.1"}
    assert restored_url.database.startswith("qt_migration_")
    assert os.environ.get("QT_DB_TEST_ISOLATED") == "1"
    # Purely additive candidate tables are present because the fixture candidate
    # bootstraps them. Do not clean them before running the old release.
    restored = create_engine(restored_url, poolclass=NullPool)
    try:
        with restored.connect() as conn:
            assert conn.scalar(text("SELECT to_regclass('market.cold_fact_pages') IS NOT NULL"))
    finally:
        restored.dispose()
    env = {
        "PG_DSN": restored_url.render_as_string(hide_password=False),
        "QT_DISABLE_DOTENV": "1",
        "MARKET_STRUCTURE_STORAGE_ROOT": str(objects.root.parent),
        "PYTHONPATH": os.pathsep.join((str(source / "src"), str(source))),
        "PYTHONDONTWRITEBYTECODE": "1",
        "LC_ALL": "C.UTF-8",
    }
    parameters = (expected_hash, dataset_id, str(series), "latest" if latest_only else "revisions")
    result = subprocess.run([sys.executable, "-c", _EXACT_OLD_READER, *parameters],
                            stdin=subprocess.DEVNULL, capture_output=True, text=True,
                            cwd=source, env=env, timeout=45)
    if result.returncode != 0:
        # Diagnostics from the child are deliberately restricted to a known
        # check label or its exception type. Never copy subprocess logs/DSNs.
        try:
            error = json.loads(result.stdout)["error"]
        except (ValueError, KeyError, TypeError):
            error = "subprocess failed"
        pytest.fail(f"exact deployed source check failed: {error}")
    report = json.loads(result.stdout)
    assert report.get("source_attested") is True
    expected_rows = [[record.series_id, record.revision, record.market_commit_seq, record.ingestion_run_id,
                      record.source_identity_key, record.canonical_material_hash, record.fact.to_dict()]
                     for record in expected] if latest_only else [
        [record.fact_version_id, record.market_commit_seq, record.row_hash,
         record.fact.accepted_at.isoformat(), record.fact.known_at.isoformat()]
        for record in expected
    ]
    if report["rows"] != json.loads(json.dumps(expected_rows, default=lambda value: value.isoformat())):
        pytest.fail("exact deployed source frozen rows differ")
    print("QT_EXACT_OLD_SOURCE_READY")


_EXACT_OLD_READER = """
import json
import logging
import sys
from pathlib import Path

logging.disable(logging.CRITICAL)
from scripts.provenance.source_tree_hash import working_tree_hash
from portal.backend.db import session
from portal.backend.service.storage.repos import market_data

try:
    source = Path.cwd()
    actual_hash = working_tree_hash(source)
    if actual_hash != sys.argv[1] or any(
        not Path(module.__file__).resolve().is_relative_to(source) for module in (session, market_data)
    ):
        raise ValueError("source_attestation_mismatch")
    if not session.db.ensure_schema():
        detail = str(session.db.last_error)
        known = ("canonical_identity_reference_invalid", "Canonical Fact storage cutover is not ready",
                 "fact_header_legacy", "Legacy market-data tables remain active",
                 "Canonical Fact", "missing required")
        safe_error = next((name for name in known if name in detail), type(session.db.last_error).__name__)
        print(json.dumps({"error": safe_error}))
        sys.exit(3)
    repo = market_data.PostgresMarketDataRepository()
    args = {"dataset_id": sys.argv[2], "series_id": int(sys.argv[3])}
    if sys.argv[4] == "latest":
        result = [[row.series_id, row.revision, row.market_commit_seq, row.ingestion_run_id,
                   row.source_identity_key, row.canonical_material_hash, row.fact.to_dict()]
                  for row in repo.read_dataset_series(**args)]
    else:
        result = [[row.fact_version_id, row.market_commit_seq, row.row_hash,
                   row.fact.accepted_at.isoformat(), row.fact.known_at.isoformat()]
                  for row in repo.read_dataset_fact_revisions(**args)]
    print(json.dumps({"source_attested": True, "rows": result}, default=lambda value: value.isoformat()))
except Exception as exc:
    print(json.dumps({"error": type(exc).__name__}))
    sys.exit(4)
"""
