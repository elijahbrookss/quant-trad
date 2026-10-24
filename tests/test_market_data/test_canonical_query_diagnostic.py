from contextlib import nullcontext
from datetime import date
import json
from types import SimpleNamespace

import pytest

from portal.backend.service.storage.repos import query_diagnostic
from portal.backend.service.storage.repos.fact_lineage import raw_archive_mapping_lookup_statement


DAY = date(2026, 9, 2)


def _scope(*, raw=True, dependency=True):
    return {
        "partition": {
            "storage_day": DAY, "state": "verified", "expected_rows": 100,
            "manifest_set_hash": "a" * 64,
        },
        "page": {
            "id": "manifest-1", "page_ordinal": 0, "row_count": 100,
            "first_commit_seq": 1, "first_id": "fact-1",
            "last_commit_seq": 100, "last_id": "fact-100",
        },
        "raw": ({
            "raw_record_id": "raw-1", "session_id": "session-1",
            "connection_epoch": 1, "receive_ordinal": 10,
            "manifest_id": "raw-manifest-1", "definition_id": "definition-1",
            "first_receive_ordinal": 1, "last_receive_ordinal": 100,
        } if raw else None),
        "dependency": ({"target_kind": "raw_manifest", "target_id": "raw-manifest-1"}
                       if dependency else None),
        "generation": None,
        "postgresql_version": "15.15",
    }


def test_limits_preserve_fixed_live_ceiling():
    limits = query_diagnostic.QueryDiagnosticLimits()
    assert (limits.statement_timeout_ms, limits.lock_timeout_ms, limits.total_seconds) == (5000, 250, 60)
    for values in (
        {"statement_timeout_ms": 5001}, {"lock_timeout_ms": 251},
        {"total_seconds": 61}, {"max_inventory_partitions": 10001},
        {"max_page_rows": 100001},
        {"max_page_logical_bytes": 1024**3 + 1},
        {"max_dependency_rows": 100001},
        {"max_raw_mapping_rows": 50001},
        {"max_wait_samples": 65},
    ):
        with pytest.raises(ValueError, match="limits_invalid"):
            query_diagnostic.QueryDiagnosticLimits(**values)


def test_closed_inventory_names_critical_read_paths_and_disposable_only_work():
    cases = query_diagnostic._cases(_scope(), query_diagnostic.QueryDiagnosticLimits())
    by_id = {case.case_id: case for case in cases}
    assert {
        "retention.size_inventory", "retention.partition_tree", "retention.progress",
        "retention.physical_children", "retention.fact_families", "retention.page_counts",
        "archive.page_cursor", "archive.source_page", "lineage.record_id",
        "lineage.exact_position", "lineage.record_id.maximum",
        "lineage.exact_position.maximum", "verification.next_page",
        "verification.source_envelopes", "verification.catalog.series",
        "verification.catalog.aliases", "verification.catalog.dependencies",
        "verification.catalog.canonical_dependencies", "verification.dependency_expiry",
        "reclamation.eligibility_families", "reclamation.relation_identity",
        "reclamation.relation_size", "reclamation.pinned_ranges",
    } == set(by_id)
    assert by_id["archive.source_page"].execution == "plan"
    assert by_id["verification.source_envelopes"].execution == "plan"
    assert by_id["retention.partition_tree"].requested_cardinality == 10002
    assert by_id["archive.source_page"].requested_cardinality == 100000
    assert by_id["verification.catalog.dependencies"].requested_cardinality == 100001
    assert by_id["verification.catalog.canonical_dependencies"].requested_cardinality == 50001
    record_maximum = by_id["lineage.record_id.maximum"]
    position_maximum = by_id["lineage.exact_position.maximum"]
    assert record_maximum.requested_cardinality == 50000
    assert len(set(record_maximum.params["ids"])) == 50000
    positions = json.loads(position_maximum.params["positions"])
    assert position_maximum.requested_cardinality == 50000
    assert len({item["receive_ordinal"] for item in positions}) == 50000
    assert by_id["reclamation.pinned_ranges"].sample_waits is True
    write_cases = {item["case_id"]: item for item in query_diagnostic._write_path_coverage()}
    assert write_cases["publication.alias_write"]["outcome"] == "untested"
    assert write_cases["publication.alias_write"]["reason"] == "production_write_forbidden"
    assert write_cases["reclamation.exclusive_handoff_and_drop"]["outcome"] == "unsupported"
    assert all(item.get("durable_ci_coverage") for item in write_cases.values())


def test_generation_catalog_diagnostics_use_preparation_identity():
    scope = _scope()
    scope["generation"] = {
        "preparation_id": "preparation-2", "generation": 2,
        "catalog_commitment": "b" * 64,
    }
    cases = {case.case_id: case for case in query_diagnostic._cases(
        scope, query_diagnostic.QueryDiagnosticLimits(),
    )}
    aliases = cases["verification.catalog.aliases"]
    dependencies = cases["verification.catalog.canonical_dependencies"]
    assert "fact_archive_prepared_material_aliases" in str(aliases.statement)
    assert "fact_archive_prepared_canonical_dependencies" in str(dependencies.statement)
    assert aliases.params["id"] == "preparation-2"
    assert dependencies.params["id"] == "preparation-2"
    assert cases["verification.catalog.series"].params["id"] == "manifest-1"


def test_lineage_diagnostic_uses_production_builder_shapes():
    record = str(raw_archive_mapping_lookup_statement(mode="record", bound=True))
    position = str(raw_archive_mapping_lookup_statement(
        mode="position", bound=True,
        predicate="manifests.definition_id = ANY(:position_definitions)",
    ))
    assert "requested_records AS MATERIALIZED" in record
    assert "records.raw_record_id=mappings.raw_record_id" in record
    assert "manifests.id = ANY(:bound_ids)" in record
    assert "requested_positions AS MATERIALIZED" in position
    assert "positions.receive_ordinal=mappings.receive_ordinal" in position
    assert "storage_lifecycle_events" in record and "storage_lifecycle_events" in position
    with pytest.raises(ValueError, match="mode_invalid"):
        raw_archive_mapping_lookup_statement(mode="arbitrary", bound=False)


def test_strict_read_only_database_never_uses_schema_bootstrap(monkeypatch):
    captured = {}

    class Engine:
        disposed = False

        def dispose(self):
            self.disposed = True

    engine = Engine()

    def create_engine(dsn, **options):
        captured.update({"dsn": dsn, "options": options})
        return engine

    def forbidden():
        raise AssertionError("schema bootstrap/session boundary must not be used")

    source = SimpleNamespace(
        dsn="postgresql://fixture/qt",
        _resolve_dsn=forbidden,
        _engine_options=lambda: {
            "future": True,
            "pool_timeout": 30,
            "connect_args": {"application_name": "portal", "connect_timeout": 20},
        },
        ensure_schema=forbidden,
        session=forbidden,
    )
    monkeypatch.setattr(query_diagnostic, "create_engine", create_engine)
    with query_diagnostic._strict_read_only_database(
            source, query_diagnostic.QueryDiagnosticLimits(),
            deadline=query_diagnostic.monotonic() + 60) as readonly:
        assert isinstance(readonly, query_diagnostic._StrictReadOnlyDatabase)
    options = captured["options"]["connect_args"]
    assert captured["dsn"] == source.dsn
    assert options["application_name"] == "qt_canonical_query_diagnostic"
    assert "default_transaction_read_only=on" in options["options"]
    assert "statement_timeout=5000" in options["options"]
    assert "lock_timeout=250" in options["options"]
    assert options["connect_timeout"] == 1
    assert captured["options"]["pool_timeout"] == 1.0
    assert engine.disposed is True


def test_elapsed_budget_overrun_cannot_report_passed(monkeypatch):
    ticks = iter((100.0, 161.0))
    monkeypatch.setattr(query_diagnostic, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(
        query_diagnostic, "_strict_read_only_database",
        lambda *_args, **_kwargs: nullcontext(object()),
    )
    monkeypatch.setattr(
        query_diagnostic, "_select_scope",
        lambda *_args, **_kwargs: _scope(),
    )
    monkeypatch.setattr(query_diagnostic, "_cases", lambda *_args, **_kwargs: ())
    receipt = query_diagnostic.run_live_query_diagnostic(
        database=object(), storage_day=DAY, source_revision="a" * 40,
        source_tree_hash="b" * 64,
    )
    assert receipt["budget_outcome"] == "overrun"
    assert receipt["status"] == "findings"
    assert receipt["elapsed_seconds"] == 61.0


def test_missing_optional_evidence_is_reported_instead_of_silently_dropped(monkeypatch):
    monkeypatch.setattr(
        query_diagnostic, "_strict_read_only_database",
        lambda *_args, **_kwargs: nullcontext(object()),
    )
    monkeypatch.setattr(query_diagnostic, "_select_scope", lambda *_args, **_kwargs: _scope(raw=False, dependency=False))
    monkeypatch.setattr(query_diagnostic, "_execute_case", lambda _database, case, **_kwargs: {
        "case_id": case.case_id, "boundary": case.boundary, "builder": case.builder,
        "sql_fingerprint": "f" * 64, "requested_cardinality": case.requested_cardinality,
        "designation": case.designation, "execution": case.execution, "outcome": "passed",
    })
    receipt = query_diagnostic.run_live_query_diagnostic(
        database=object(), storage_day=DAY, source_revision="a" * 40,
        source_tree_hash="b" * 64,
    )
    by_id = {item["case_id"]: item for item in receipt["cases"]}
    assert by_id["lineage.record_id"]["outcome"] == "unsupported"
    assert by_id["lineage.exact_position"]["outcome"] == "unsupported"
    assert by_id["verification.dependency_expiry"]["outcome"] == "unsupported"
    assert receipt["coverage"]["claims_every_query"] is False


def test_cancelled_cases_are_explicit_and_receipt_write_is_atomic(monkeypatch, tmp_path):
    monkeypatch.setattr(
        query_diagnostic, "_strict_read_only_database",
        lambda *_args, **_kwargs: nullcontext(object()),
    )
    monkeypatch.setattr(query_diagnostic, "_select_scope", lambda *_args, **_kwargs: _scope())
    receipt = query_diagnostic.run_live_query_diagnostic(
        database=object(), storage_day=DAY, source_revision="a" * 40,
        source_tree_hash="b" * 64, cancel_requested=lambda: True,
    )
    assert receipt["status"] == "findings"
    assert all(
        item["reason"] == "cancelled"
        for item in receipt["cases"]
        if item["execution"] in {"plan", "analyze"}
    )
    path = tmp_path / "private" / "receipt.json"
    query_diagnostic.write_receipt(path, receipt)
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == query_diagnostic.SCHEMA_VERSION
    assert not path.with_name(".receipt.json.tmp").exists()
