from copy import deepcopy
from datetime import date
from types import SimpleNamespace
import hashlib
import logging

import pytest

from market_data.canonical_storage import legacy_material_alias
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos import fact_archival, market_data
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
    session = SimpleNamespace(execute=lambda *_: SimpleNamespace(
        scalar_one_or_none=lambda: "raw-id" if mode == "expired" else None))
    archive = PostgresCanonicalFactArchiveRepository(
        database=None, object_store=SimpleNamespace(local_path=lambda _: path),
        temporary_directory=tmp_path / "staging", max_dependency_objects=1,
        max_dependency_bytes=1 if mode == "bytes" else 1024,
    )
    monkeypatch.setattr(archive, "_flow_references", lambda *args, **kwargs: refs)
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


@pytest.mark.parametrize("mode", ["valid", "expired_tail", "stop", "deadline"])
def test_publication_batches_expiry_checks_and_stops_before_reading_bytes(tmp_path, monkeypatch, mode):
    from portal.backend.service.market.canonical_retention import (
        CanonicalRetentionBudgetExceeded, CanonicalRetentionStopRequested,
    )
    data = b"shared immutable archive"
    path = tmp_path / "raw.parquet"
    path.write_bytes(data)
    reference = {"object_key": "raw.parquet", "object_sha256": hashlib.sha256(data).hexdigest()}
    refs = {f"raw-{index:03d}": reference for index in range(129)}
    queries, byte_reads = [], []
    def execute(statement, parameters):
        identities = parameters.get("ids", [parameters.get("id")])
        queries.append(identities)
        expired = "raw-128" if mode == "expired_tail" and "raw-128" in identities else None
        return SimpleNamespace(scalar_one=lambda: expired is not None, scalar_one_or_none=lambda: expired)
    def check_budget():
        if queries and mode in {"stop", "deadline"}:
            error = CanonicalRetentionStopRequested if mode == "stop" else CanonicalRetentionBudgetExceeded
            raise error("publication expiry interrupted")
    def local_path(key):
        byte_reads.append(key)
        return path
    archive = PostgresCanonicalFactArchiveRepository(
        database=None, object_store=SimpleNamespace(local_path=local_path),
        temporary_directory=tmp_path / "staging", check_budget=check_budget,
    )
    monkeypatch.setattr(archive, "_flow_references", lambda *args, **kwargs: refs)
    row = {**_row(), "fact_type": "market.market_response"}
    if mode == "valid":
        dependencies, sources = archive._dependencies(SimpleNamespace(execute=execute), [row])
        assert [item["target_id"] for item in dependencies] == sorted(refs)
        assert sources == [] and byte_reads
        assert len(queries) <= 2 and sum(map(len, queries)) == len(refs)
    else:
        error = {"expired_tail": RuntimeError, "stop": CanonicalRetentionStopRequested,
                 "deadline": CanonicalRetentionBudgetExceeded}[mode]
        match = "dependency_expired: target_id=raw-128" if mode == "expired_tail" else "publication expiry interrupted"
        with pytest.raises(error, match=match):
            archive._dependencies(SimpleNamespace(execute=execute), [row])
        assert not byte_reads
        if mode in {"stop", "deadline"}:
            assert len(queries) == 1 and len(queries[0]) < len(refs)


def test_verification_phase_records_completed_and_failed_durations(monkeypatch, caplog):
    clock = iter((10.0, 10.25, 20.0, 20.5))
    monkeypatch.setattr(fact_archival, "perf_counter", lambda: next(clock))
    page = {"id": "manifest", "page_ordinal": 7}
    timings = {}
    with caplog.at_level(logging.INFO):
        assert fact_archival._timed_verification_call(
            timings=timings, phase="archive_read", day=date(2026, 8, 29), page=page,
            operation=lambda: "result",
        ) == "result"
        with pytest.raises(RuntimeError, match="injected"):
            fact_archival._timed_verification_call(
                timings=timings, phase="dependency_proof", day=date(2026, 8, 29), page=page,
                operation=lambda: (_ for _ in ()).throw(RuntimeError("injected")),
            )
    assert timings == {"archive_read": 0.25, "dependency_proof": 0.5}
    assert "phase=archive_read outcome=completed seconds=0.250000" in caplog.text
    assert "phase=dependency_proof outcome=failed seconds=0.500000" in caplog.text

def test_stage_phase_records_completed_and_failed_durations(monkeypatch, caplog):
    clock = iter((10.0, 10.25, 20.0, 20.5))
    monkeypatch.setattr(fact_archival, "perf_counter", lambda: next(clock))
    with caplog.at_level(logging.INFO):
        assert fact_archival._timed_stage_call(
            phase="source_select", day=date(2026, 8, 29), page_ordinal=3,
            operation=lambda: "result",
        ) == "result"
        with pytest.raises(RuntimeError, match="injected"):
            fact_archival._timed_stage_call(
                phase="canonical_dependency_catalog_insert", day=date(2026, 8, 29),
                page_ordinal=3,
                operation=lambda: (_ for _ in ()).throw(RuntimeError("injected")),
            )
    assert "phase=source_select outcome=completed seconds=0.250000" in caplog.text
    assert "phase=canonical_dependency_catalog_insert outcome=failed seconds=0.500000" in caplog.text


def test_only_canonical_dependency_catalog_uses_the_larger_stage_batch(tmp_path):
    class Session:
        def __init__(self):
            self.pending = []
            self.flush_sizes = []

        def add_all(self, rows):
            self.pending.extend(rows)

        def flush(self):
            self.flush_sizes.append(len(self.pending))
            self.pending.clear()

    archive = PostgresCanonicalFactArchiveRepository(
        database=None, object_store=None, temporary_directory=tmp_path,
    )
    canonical = Session()
    archive._stage_catalog_rows(
        canonical, fact_archival.MarketFactArchiveCanonicalDependencyRecord, "manifest",
        ({"fact_version_id": str(index), "row_hash": "a" * 64} for index in range(1025)),
    )
    assert canonical.flush_sizes == [256, 256, 256, 256, 1]

    aliases = Session()
    archive._stage_catalog_rows(
        aliases, fact_archival.MarketFactArchiveMaterialAliasRecord, "manifest",
        ({"fact_version_id": str(index), "series_id": 1, "evidence_key": "legacy",
          "material_hash": "b" * 64} for index in range(257)),
    )
    assert aliases.flush_sizes == [128, 128, 1]
