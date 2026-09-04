from copy import deepcopy
from types import SimpleNamespace
import hashlib

import pytest

from market_data.canonical_storage import legacy_material_alias
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos import market_data
from tests.test_market_data.test_canonical_fact_archive import _row


@pytest.mark.parametrize("legacy", [None, "a" * 64, "bad", 7])
def test_legacy_alias_is_derived_only_from_present_valid_provenance(legacy):
    row = {**_row(), "fact_type": "market.bbo", "provenance": {"_qt_bbo_evidence": {}}}
    if legacy is not None:
        row["provenance"]["_qt_bbo_evidence"]["legacy_material_hash"] = legacy
    original = deepcopy(row)
    if legacy is None:
        assert legacy_material_alias(row) is None
    elif legacy == "a" * 64:
        assert legacy_material_alias(row) == {
            "fact_version_id": row["id"], "series_id": row["series_id"],
            "evidence_key": "_qt_bbo_evidence", "material_hash": legacy,
        }
    else:
        with pytest.raises(RuntimeError, match="canonical_legacy_material_invalid"):
            legacy_material_alias(row)
    assert row == original


@pytest.mark.parametrize("mode", ["valid", "corrupt", "expired", "missing", "bytes", "objects"])
def test_dependency_acknowledgement_requires_available_verified_bytes_within_budgets(tmp_path, monkeypatch, mode):
    data = b"immutable raw archive fixture"
    path = tmp_path / "raw.parquet"
    path.write_bytes(data)
    if mode == "missing":
        path.unlink()
    reference = {"object_key": "raw.parquet", "object_sha256": hashlib.sha256(data).hexdigest()}
    if mode == "corrupt":
        path.write_bytes(b"corrupt")
    refs = {"raw-id": reference}
    if mode == "objects":
        refs["second-id"] = reference
    monkeypatch.setattr(market_data, "_collect_material_archive_refs", lambda *_, **__: refs)
    session = SimpleNamespace(execute=lambda *_: SimpleNamespace(scalar_one=lambda: mode == "expired"))
    archive = PostgresCanonicalFactArchiveRepository(
        database=None, object_store=SimpleNamespace(local_path=lambda _: path),
        temporary_directory=tmp_path / "staging", max_dependency_objects=1,
        max_dependency_bytes=1 if mode == "bytes" else 1024,
    )
    # Exercise the external-dependency verifier with a dependent family, not a
    # self-contained funding record whose evidence is already inside the page.
    row = {**_row(), "fact_type": "market.market_response"}
    if mode == "valid":
        assert archive._dependencies(session, [row]) == ([{
            "target_kind": "raw_manifest", "target_id": "raw-id", **reference,
        }], [])
    else:
        with pytest.raises(FileNotFoundError if mode == "missing" else RuntimeError):
            archive._dependencies(session, [row])


def test_structured_reserve_evidence_is_self_contained_not_a_missing_trade_archive(tmp_path):
    archive = PostgresCanonicalFactArchiveRepository(database=None, object_store=None, temporary_directory=tmp_path)
    assert archive._dependencies(None, [{**_row(), "fact_type": "asset.reserve_state"}]) == ([], [])


def test_normalized_page_never_gets_an_empty_dependency_acknowledgement(tmp_path):
    archive = PostgresCanonicalFactArchiveRepository(database=None, object_store=None, temporary_directory=tmp_path)
    with pytest.raises(RuntimeError, match="canonical_archive_dependency_proof_required"):
        archive._dependencies(None, [{**_row(), "fact_type": "market.normalized.example"}])


@pytest.mark.parametrize("value", [0, -1, True, "1000"])
def test_dependency_budgets_require_explicit_positive_integers(tmp_path, value):
    with pytest.raises(ValueError, match="canonical_archive_dependency_budget_invalid"):
        PostgresCanonicalFactArchiveRepository(
            database=None, object_store=None, temporary_directory=tmp_path, max_dependency_bytes=value,
        )
