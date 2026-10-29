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


def test_verification_detail_records_bounded_counters_and_cannot_break_proof(
        monkeypatch, caplog):
    clock = iter((10.0, 10.25, 20.0, 20.5, 30.0, 30.1))
    monkeypatch.setattr(fact_archival, "perf_counter", lambda: next(clock))
    observed = []

    assert fact_archival._timed_verification_detail(
        observer=lambda **fields: observed.append(fields),
        phase="exact_raw_proof", scope="page",
        operation=lambda: {"raw-a": {}, "raw-b": {}},
        summarize=lambda found: {"root_count": 3, "object_count": len(found)},
    ) == {"raw-a": {}, "raw-b": {}}
    with pytest.raises(RuntimeError, match="injected"):
        fact_archival._timed_verification_detail(
            observer=lambda **fields: observed.append(fields),
            phase="book_checkpoint_proof", scope="page",
            operation=lambda: (_ for _ in ()).throw(RuntimeError("injected")),
        )

    assert observed == [
        {
            "phase": "exact_raw_proof", "scope": "page", "seconds": 0.25,
            "outcome": "completed", "counters": {
                "root_count": 3, "object_count": 2,
            },
        },
        {
            "phase": "book_checkpoint_proof", "scope": "page", "seconds": 0.5,
            "outcome": "failed", "counters": {},
        },
    ]

    def unavailable_observer(**_fields):
        raise RuntimeError("diagnostic sink unavailable")

    assert fact_archival._timed_verification_detail(
        observer=unavailable_observer, phase="exact_raw_proof",
        operation=lambda: "proof-result",
    ) == "proof-result"
    assert "canonical_archive_verification_measurement_failed" in caplog.text


def test_verification_detail_log_has_fixed_low_cardinality_fields(caplog):
    page = {"id": "manifest", "page_ordinal": 7}
    with caplog.at_level(logging.INFO):
        fact_archival._record_verification_detail(
            phase="prefix_catalog_sql", scope="book", day=date(2026, 9, 8),
            page=page, seconds=1.25, outcome="completed",
            counters={
                "query_count": 4, "object_count": 3, "byte_count": 2048,
                "root_count": 2, "row_count": 10, "chunk_count": 3,
                "prefix_count": 1,
            },
        )
    assert "canonical_archive_verification_detail" in caplog.text
    assert "action=verify storage_day=2026-09-08 page_ordinal=7 manifest_id=manifest" in caplog.text
    assert "phase=prefix_catalog_sql scope=book outcome=completed seconds=1.250000" in caplog.text
    assert "query_count=4 object_count=3 byte_count=2048 root_count=2" in caplog.text
    assert "row_count=10 chunk_count=3 prefix_count=1" in caplog.text


def test_dependency_measurement_observer_labels_copy_and_cannot_change_proof(
        monkeypatch, caplog):
    page = {"id": "unpublished", "page_ordinal": 4}
    observer = fact_archival._dependency_measurement_observer(
        action="copy", day=date(2026, 9, 9), page=page,
    )
    with caplog.at_level(logging.INFO):
        assert fact_archival._timed_verification_detail(
            observer=observer, phase="exact_raw_proof", scope="page",
            operation=lambda: {"raw-a": {}},
            summarize=lambda found: {"object_count": len(found)},
        ) == {"raw-a": {}}
    assert "action=copy storage_day=2026-09-09 page_ordinal=4 manifest_id=unpublished" in caplog.text
    assert "phase=exact_raw_proof scope=page outcome=completed" in caplog.text
    assert "object_count=1" in caplog.text

    def unavailable_recorder(**_fields):
        raise RuntimeError("copy diagnostic sink unavailable")

    monkeypatch.setattr(fact_archival, "_record_verification_detail", unavailable_recorder)
    assert fact_archival._timed_verification_detail(
        observer=observer, phase="book_checkpoint_proof", scope="page",
        operation=lambda: "proof-result",
    ) == "proof-result"
    assert "canonical_archive_verification_measurement_failed" in caplog.text


def test_dependency_measurement_observer_rejects_unbounded_action_label():
    with pytest.raises(ValueError, match="canonical_archive_measurement_action_invalid"):
        fact_archival._dependency_measurement_observer(
            action="other", day=date(2026, 9, 9),
            page={"id": "manifest", "page_ordinal": 4},
        )

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


def _database_observation_fixture(**overrides):
    values = {
        "backend_pid": 1234,
        "server_version_num": 150014,
        "track_io_timing": False,
        "wal_stats_reset": "wal-reset",
        "wal_records": 100,
        "wal_fpi": 10,
        "wal_bytes": 1000,
        "wal_buffers_full": 1,
        "writer_stats_reset": "writer-reset",
        "checkpoints_timed": 20,
        "checkpoints_req": 3,
        "checkpoint_write_time_ms": 200.0,
        "checkpoint_sync_time_ms": 30.0,
        "buffers_checkpoint": 40,
        "buffers_clean": 50,
        "buffers_backend": 60,
        "buffers_backend_fsync": 2,
    }
    values.update(overrides)
    return values


def test_stage_database_snapshot_failure_rolls_back_only_its_savepoint(caplog):
    class Connection:
        def __init__(self):
            self.commands = []

        def exec_driver_sql(self, sql):
            self.commands.append(sql)

        @staticmethod
        def execute(_statement):
            raise RuntimeError("injected observation failure")

    connection = Connection()
    session = SimpleNamespace(connection=lambda: connection)
    with caplog.at_level(logging.WARNING):
        assert fact_archival._stage_database_snapshot(session) is None
    assert connection.commands == [
        "SAVEPOINT qt_archive_stage_observation",
        "ROLLBACK TO SAVEPOINT qt_archive_stage_observation",
        "RELEASE SAVEPOINT qt_archive_stage_observation",
    ]
    assert "outcome=observation_unavailable error_type=RuntimeError" in caplog.text
    assert "injected observation failure" not in caplog.text


def test_stage_database_observation_labels_cluster_deltas_without_device_claim(caplog):
    before = _database_observation_fixture()
    after = _database_observation_fixture(
        wal_records=125, wal_fpi=12, wal_bytes=1500, wal_buffers_full=2,
        checkpoints_timed=21, checkpoints_req=5, checkpoint_write_time_ms=240.0,
        checkpoint_sync_time_ms=32.0, buffers_checkpoint=47, buffers_clean=59,
        buffers_backend=71, buffers_backend_fsync=3,
    )
    with caplog.at_level(logging.INFO):
        fact_archival._record_stage_database_observation(
            day=date(2026, 8, 29), page_ordinal=4,
            before=before, after=after, outcome="committed",
        )
    assert "scope=cluster_overlap backend_pid=1234" in caplog.text
    assert "device_attribution=unsupported wal_records=25 wal_fpi=2 wal_bytes=500" in caplog.text
    assert "checkpoints_timed=1 checkpoints_req=2 checkpoint_write_time_ms=40.0" in caplog.text
    assert "buffers_checkpoint=7 buffers_clean=9 buffers_backend=11 buffers_backend_fsync=1" in caplog.text


def test_stage_database_observation_never_invents_rollback_or_reset_deltas(caplog):
    before = _database_observation_fixture()
    reset = _database_observation_fixture(wal_stats_reset="later", writer_stats_reset="later")
    with caplog.at_level(logging.INFO):
        fact_archival._record_stage_database_observation(
            day=date(2026, 8, 29), page_ordinal=5,
            before=before, after=reset, outcome="committed",
        )
        fact_archival._record_stage_database_observation(
            day=date(2026, 8, 29), page_ordinal=6,
            before=before, after=None, outcome="rolled_back_delta_unavailable",
        )
    assert caplog.text.count("wal_records=None") == 2
    assert caplog.text.count("buffers_checkpoint=None") == 2
    assert "outcome=rolled_back_delta_unavailable" in caplog.text


def test_generic_catalog_writer_keeps_128_row_batches(tmp_path):
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
    aliases = Session()
    archive._stage_catalog_rows(
        aliases, fact_archival.MarketFactArchiveMaterialAliasRecord, "manifest",
        ({"fact_version_id": str(index), "series_id": 1, "evidence_key": "legacy",
          "material_hash": "b" * 64} for index in range(257)),
    )
    assert aliases.flush_sizes == [128, 128, 1]


def test_canonical_dependency_writer_keeps_typed_128_row_batches(tmp_path, monkeypatch, caplog):
    class Session:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params):
            self.calls.append((str(statement), params))
            return SimpleNamespace(rowcount=len(params["fact_version_ids"]))

    clock = iter((1.0, 1.1, 2.0, 2.2, 3.0, 3.3))
    monkeypatch.setattr(fact_archival, "perf_counter", lambda: next(clock))
    archive = PostgresCanonicalFactArchiveRepository(
        database=None, object_store=None, temporary_directory=tmp_path,
    )
    session = Session()
    with caplog.at_level(logging.INFO):
        result = archive._stage_canonical_dependency_catalog_rows(
            session, "manifest",
            ({"fact_version_id": f"mfv_{index:04d}", "row_hash": f"{index:064x}"}
             for index in range(257)),
        )

    assert result["row_count"] == 257 and result["batch_count"] == 3
    assert result["max_batch_seconds"] == pytest.approx(0.3)
    assert [len(params["fact_version_ids"]) for _, params in session.calls] == [128, 128, 1]
    assert all(statement.startswith("INSERT INTO market.fact_archive_canonical_dependencies")
               for statement, _ in session.calls)
    assert [identity for _, params in session.calls for identity in params["fact_version_ids"]] == [
        f"mfv_{index:04d}" for index in range(257)
    ]
    assert "catalog=canonical_dependency manifest_id=manifest outcome=completed rows=257 batches=3" in caplog.text


def test_canonical_dependency_writer_rejects_incomplete_insert_count(tmp_path):
    class Session:
        @staticmethod
        def execute(_statement, _params):
            return SimpleNamespace(rowcount=0)

    archive = PostgresCanonicalFactArchiveRepository(
        database=None, object_store=None, temporary_directory=tmp_path,
    )
    with pytest.raises(RuntimeError, match=(
            "canonical_archive_dependency_insert_count_mismatch: "
            "manifest_id=manifest expected=1 actual=0")):
        archive._stage_canonical_dependency_catalog_rows(Session(), "manifest", [{
            "fact_version_id": "mfv_1", "row_hash": "a" * 64,
        }])


def test_alias_catalog_writer_sorts_identity_and_keeps_bounded_statements(tmp_path):
    class Session:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params):
            self.calls.append((str(statement), params))
            return SimpleNamespace(rowcount=len(params["fact_version_ids"]))

    archive = PostgresCanonicalFactArchiveRepository(
        database=None, object_store=None, temporary_directory=tmp_path,
    )
    session = Session()
    aliases = ({
        "fact_version_id": f"mfv_{index:04d}",
        "series_id": index % 3 + 1,
        "evidence_key": "legacy",
        "material_hash": f"{index:064x}",
    } for index in reversed(range(257)))

    result = archive._stage_alias_catalog_rows(session, "manifest", aliases)

    assert result["row_count"] == 257
    assert result["batch_count"] == 5
    assert result["max_batch_seconds"] >= 0.0
    assert [len(params["fact_version_ids"]) for _, params in session.calls] == [64, 64, 64, 64, 1]
    assert all(statement.startswith("INSERT INTO market.fact_archive_material_aliases")
               for statement, _ in session.calls)
    assert all(params["manifest_id"] == "manifest" for _, params in session.calls)
    inserted = [identity for _, params in session.calls for identity in params["fact_version_ids"]]
    assert inserted == sorted(inserted)
    inserted_rows = [
        row
        for _, params in session.calls
        for row in zip(
            params["fact_version_ids"], params["evidence_keys"],
            params["series_ids"], params["material_hashes"], strict=True,
        )
    ]
    assert inserted_rows == [
        (f"mfv_{index:04d}", "legacy", index % 3 + 1, f"{index:064x}")
        for index in range(257)
    ]


def test_alias_catalog_writer_rejects_incomplete_insert_count(tmp_path):
    class Session:
        @staticmethod
        def execute(_statement, _params):
            return SimpleNamespace(rowcount=0)

    archive = PostgresCanonicalFactArchiveRepository(
        database=None, object_store=None, temporary_directory=tmp_path,
    )
    with pytest.raises(RuntimeError, match=(
            "canonical_archive_alias_insert_count_mismatch: manifest_id=manifest expected=1 actual=0")):
        archive._stage_alias_catalog_rows(Session(), "manifest", [{
            "fact_version_id": "mfv_1", "series_id": 1,
            "evidence_key": "legacy", "material_hash": "b" * 64,
        }])
