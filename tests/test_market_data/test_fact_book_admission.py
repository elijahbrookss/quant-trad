from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from portal.backend.service.storage.repos import fact_book_admission as admission
from portal.backend.service.storage.repos import fact_archival, fact_dependencies


BASE = datetime(2026, 1, 1, tzinfo=UTC)
POSITION = {"definition_id": "definition", "session_id": "session", "connection_epoch": 0,
            "provider_product_id": "BTC-USD", "receive_ordinal": 3, "event_ordinal": 0,
            "provider_sequence_num": 1}


def _source(identity="source-1"):
    return {"id": identity, "series_id": 1, "fact_type": "market.l2_book", "row_hash": "a" * 64,
            "market_commit_seq": 10, "known_at": BASE, "observation_key": "definition:session:0:3:0",
            "provenance": {"_qt_l2_evidence": {**POSITION, "raw_record_id": "raw"}},
            "payload": {"validity_interval_id": "interval", "product_definition_version_id": "product",
                        "reconstruction_version": admission.BOOK_RECONSTRUCTION_VERSION, "after_state_hash": "state",
                        "entries": [{"provider_size_unit": "base"}]}}


def _derived():
    return {"id": "derived", "fact_type": "market.bbo", "market_commit_seq": 12, "known_at": BASE,
            "provenance": {"_qt_bbo_evidence": {"source_position": dict(POSITION), "source_l2_series_id": 1}},
            "payload": {"validity_interval_id": "interval", "product_definition_version_id": "product",
                        "source_state_hash": "state"}}


@pytest.mark.parametrize("mode", ["valid", "missing", "state", "interval", "scope", "product", "budget",
                                 "future_known", "future_commit", "wrong_key", "wrong_series"])
def test_source_admission_binds_all_causal_revisions_not_latest_aliases(monkeypatch, mode):
    rows = [_source(), _source("source-2")]
    if mode in {"state", "interval", "product"}:
        field = {"state": "after_state_hash", "interval": "validity_interval_id", "product": "product_definition_version_id"}[mode]
        for row in rows:
            row["payload"][field] = "wrong"
    if mode == "scope":
        rows[0]["provenance"]["_qt_l2_evidence"]["session_id"] = "another-session"
    elif mode == "future_known":
        rows[0]["known_at"] = BASE + timedelta(seconds=1)
    elif mode == "future_commit":
        rows[0]["market_commit_seq"] = 13
    elif mode == "wrong_key":
        rows[0]["observation_key"] = "another-observation"
    elif mode == "wrong_series":
        rows[0]["series_id"] = 99
    pairs = [] if mode == "missing" else [("derived", item["id"]) for item in rows]
    def execute(statement, params):
        assert "source.market_commit_seq<=requested.commit_seq" in str(statement)
        assert "source.known_at<=requested.known_at" in str(statement)
        assert "ORDER BY requested.root_id,source.market_commit_seq,source.id" in str(statement)
        assert json.loads(params["requests"]) == [{"root_id": "derived", "series_id": 1,
            "observation_key": "definition:session:0:3:0", "commit_seq": 12, "known_at": BASE.isoformat()}]
        return SimpleNamespace(all=lambda: pairs)
    def read(_session, ids, **kwargs):
        return {row["id"]: row for row in rows if row["id"] in ids}
    verified = []
    monkeypatch.setattr(admission, "read_canonical_dependency_rows", read)
    monkeypatch.setattr(admission, "record_from_storage_row", lambda row: verified.append(row["id"]))
    args = dict(rows=[_derived()], object_store=None, max_rows=1 if mode == "budget" else 10, max_logical_bytes=1024)
    if mode == "valid":
        assert admission.resolve_book_source_revisions(SimpleNamespace(execute=execute), **args) == rows
        assert verified == ["source-1", "source-2"]
    else:
        with pytest.raises(RuntimeError, match="canonical_book_source_"):
            admission.resolve_book_source_revisions(SimpleNamespace(execute=execute), **args)


@pytest.mark.parametrize("mode", ["valid", "missing_validity", "validity_scope", "validity_position", "version",
                                 "missing_product", "product_scope", "unit", "bound_checkpoint_missing"])
def test_book_metadata_fails_closed_without_mutable_stream_config(mode):
    row = _source()
    opening = {"series_id": 1, "status": "open_valid", "reconstruction_version": admission.BOOK_RECONSTRUCTION_VERSION,
               "opening_session_id": "session", "opening_connection_epoch": 0, "opening_receive_ordinal": 1,
               "opening_event_ordinal": 0}
    product = {"provider_product_id": "BTC-USD", "provider_size_unit": "base", "price_increment": None, "base_increment": None}
    if mode == "missing_validity":
        opening = None
    elif mode == "validity_scope":
        opening["opening_session_id"] = "wrong"
    elif mode == "validity_position":
        opening["opening_receive_ordinal"] = 4
    elif mode == "version":
        row["payload"]["reconstruction_version"] = "unsupported"
    elif mode == "missing_product":
        product = None
    elif mode == "product_scope":
        product["provider_product_id"] = "ETH-USD"
    elif mode == "unit":
        product["provider_size_unit"] = "contracts"
    def execute(statement, params):
        sql = str(statement)
        if "book_validity_interval_versions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: opening))
        if "product_definition_versions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: product))
        assert "book_checkpoint_manifests" in sql
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
    kwargs = dict(rows=[row], object_store=None, byte_verifier=None, max_objects=10,
                  bound_checkpoint_ids=["missing"] if mode == "bound_checkpoint_missing" else None)
    if mode == "valid":
        assert admission.verify_book_metadata_and_checkpoints(SimpleNamespace(execute=execute), **kwargs) == ([], [])
    else:
        with pytest.raises(RuntimeError, match="canonical_book_"):
            admission.verify_book_metadata_and_checkpoints(SimpleNamespace(execute=execute), **kwargs)


def test_every_revision_checks_validity_not_just_latest_position():
    rows = [_source(), _source("older")]
    rows[1]["provenance"]["_qt_l2_evidence"]["receive_ordinal"] = 1
    opening = {"series_id": 1, "status": "open_valid", "reconstruction_version": admission.BOOK_RECONSTRUCTION_VERSION,
               "opening_session_id": "session", "opening_connection_epoch": 0, "opening_receive_ordinal": 2,
               "opening_event_ordinal": 0}
    product = {"provider_product_id": "BTC-USD", "provider_size_unit": "base", "price_increment": None, "base_increment": None}
    values = iter([opening, product])
    session = SimpleNamespace(execute=lambda *_: SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: next(values))))
    with pytest.raises(RuntimeError, match="canonical_book_validity_position_mismatch"):
        admission.verify_book_metadata_and_checkpoints(session, rows=rows, object_store=None, byte_verifier=None, max_objects=10)


@pytest.mark.parametrize("mode", ["valid", "state", "missing", "scope", "raw_mapping", "additional"])
def test_checkpoint_requires_exact_canonical_state_as_well_as_valid_bytes(monkeypatch, mode):
    row = {**_source(), "market_commit_seq": 12}
    opening = {"series_id": 1, "status": "open_valid", "reconstruction_version": admission.BOOK_RECONSTRUCTION_VERSION,
               "opening_session_id": "session", "opening_connection_epoch": 0, "opening_receive_ordinal": 1,
               "opening_event_ordinal": 0}
    product = {"provider_product_id": "BTC-USD", "provider_size_unit": "base", "price_increment": None, "base_increment": None}
    checkpoint = {**POSITION, "id": "checkpoint", "series_id": 1, "known_at": BASE, "source_manifest_ids": ["raw"],
                  "state_hash": "wrong" if mode == "state" else "state", "validity_interval_id": "interval",
                  "product_definition_version_id": "product", "object_key": "checkpoint.parquet",
                  "object_sha256": "a" * 64, "byte_count": 100}
    if mode == "scope":
        checkpoint["provider_sequence_num"] = 99
    def execute(statement, params):
        sql = str(statement)
        if "book_validity_interval_versions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: opening))
        if "product_definition_versions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: product))
        if "book_checkpoint_manifests" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [checkpoint]))
        if "count(DISTINCT manifests.id)" in sql:
            return SimpleNamespace(scalar_one=lambda: 0 if mode == "raw_mapping" else 1)
        assert "market_commit_seq<=:seq AND known_at<=:known_at" in sql
        assert params["known_at"] == BASE and params["seq"] == 12
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [] if mode == "missing" else [row["id"]]))
    restored = []
    verified = []
    monkeypatch.setattr(admission, "restore_book_checkpoint_parquet", lambda *args, **kwargs: restored.append(kwargs["expected"]["id"]))
    roots = [row]
    decoded = []
    if mode == "additional":
        root = deepcopy(row)
        root["id"] = "root"
        root["provenance"]["_qt_l2_evidence"]["receive_ordinal"] = 4
        roots = [root]
        def grouped(_session, identities, **kwargs):
            assert identities == [row["id"]]
            assert kwargs["max_logical_bytes"] == 64 * 1024**2
            return {row["id"]: row}
        monkeypatch.setattr(admission, "read_checkpoint_dependency_rows", grouped)
        monkeypatch.setattr(admission, "record_from_storage_row", lambda source: decoded.append(source["id"]))
    kwargs = dict(rows=roots, object_store=SimpleNamespace(local_path=lambda key: key), max_objects=10,
                  byte_verifier=SimpleNamespace(verify=lambda *args, **kwargs: verified.append(args)))
    if mode in {"valid", "additional"}:
        objects, sources = admission.verify_book_metadata_and_checkpoints(SimpleNamespace(execute=execute), **kwargs)
        assert sources == [row] and objects[0]["target_id"] == "checkpoint"
        assert restored == ["checkpoint"] and len(verified) == 1
        assert decoded == ([row["id"]] if mode == "additional" else [])
    else:
        with pytest.raises(RuntimeError, match="canonical_book_checkpoint_source_"):
            admission.verify_book_metadata_and_checkpoints(SimpleNamespace(execute=execute), **kwargs)
        assert not restored and not verified


def test_source_byte_budget_rejects_before_fetching_payloads():
    class Session:
        def execute(self, statement, params):
            if "coalesce(sum" in str(statement):
                return SimpleNamespace(scalar_one=lambda: 2048)
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
    with pytest.raises(fact_dependencies.CanonicalSourceByteBudgetExceeded,
                       match="logical_bytes=2048 max_logical_bytes=1024") as caught:
        fact_dependencies.read_canonical_dependency_rows(Session(), ["source"], reader=None, max_logical_bytes=1024)
    assert caught.value.logical_bytes == 2048
    assert caught.value.max_logical_bytes == 1024


@pytest.mark.parametrize("mode", ["valid", "missing", "hash", "duplicate"])
def test_source_placement_proof_checks_exact_header(mode):
    dependency = {"fact_version_id": "source", "row_hash": "a" * 64}
    rows = [] if mode == "missing" else [{"id": "source", "row_hash": "b" * 64 if mode == "hash" else "a" * 64,
                                         "storage_day": BASE.date(), "hot_present": True}]
    session = SimpleNamespace(execute=lambda *_: SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows)))
    dependencies = [dependency, deepcopy(dependency)] if mode == "duplicate" else [dependency]
    if mode == "valid":
        assert len(fact_dependencies.verify_canonical_source_placements(session, dependencies)) == 64
    else:
        with pytest.raises(RuntimeError, match="canonical_source_dependency_"):
            fact_dependencies.verify_canonical_source_placements(session, dependencies)


class _CheckpointSources:
    """Metadata-only placement fixture with independently bounded cold pages."""
    def __init__(self):
        self.rows = {identity: {"id": identity, "revision": index + 1, "known_at": BASE,
                               "market_commit_seq": index + 5, "payload": {"state": identity}}
                     for index, identity in enumerate(("a", "b", "c"))}
        self.page_for = {"a": "page-1", "b": "page-1", "c": "page-2"}
        self.manifests = {key: SimpleNamespace(manifest_id=key, logical_byte_count=2500,
            object_key=key + ".parquet", object_sha256=key, byte_count=100) for key in set(self.page_for.values())}
        self.calls, self.verified, self.cached_verified = [], [], []
        self.coverage = "valid"
        self.hot = set()

    def execute(self, statement, params):
        sql = str(statement)
        ids = params["ids"]
        if "fact_identities" in sql:
            rows = [{"id": identity, "storage_day": BASE.date()} for identity in ids]
            if self.coverage == "identity":
                rows.pop()
        elif "AS requested_id" in sql:
            rows = [{"requested_id": identity, "hot_id": identity if identity in self.hot else None,
                     "id": None if identity in self.hot else self.page_for[identity]} for identity in ids]
            if self.coverage == "ambiguous":
                rows.append(dict(rows[0]))
            elif self.coverage == "missing":
                rows[0]["id"] = None
        elif "coalesce(sum" in sql:
            return SimpleNamespace(scalar_one=lambda: 0)
        else:
            assert "SELECT DISTINCT manifests.*" in sql
            rows = [{"id": key} for key in sorted({self.page_for[i] for i in ids if i not in self.hot})]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    def read_rows_by_ids(self, _session, ids):
        self.calls.append(list(ids))
        return {identity: deepcopy(self.rows[identity]) for identity in ids}

    def verify_cached_rows_by_ids(self, _session, rows):
        self.cached_verified.append(sorted(rows))
        for identity, row in rows.items():
            if row != self.rows[identity]:
                raise RuntimeError("canonical_archive_envelope_mismatch")

    def verify(self, key, digest, *, expected_bytes):
        assert expected_bytes == 100
        self.verified.append(key)


def _checkpoint_reader(monkeypatch, fixture, budget=4096, max_objects=10, **kwargs):
    monkeypatch.setattr(fact_dependencies, "_catalog_manifest", lambda row: fixture.manifests[row["id"]])
    return fact_dependencies.read_checkpoint_dependency_rows(fixture, ["a", "b", "c"],
        reader=fixture, byte_verifier=fixture, max_logical_bytes=budget, max_objects=max_objects, **kwargs)


def test_checkpoint_cold_groups_preserve_all_rows_with_a_bounded_decode(monkeypatch):
    fixture = _CheckpointSources()
    monkeypatch.setattr(fact_dependencies, "_catalog_manifest", lambda row: fixture.manifests[row["id"]])
    with pytest.raises(fact_dependencies.CanonicalSourceByteBudgetExceeded):
        fact_dependencies.read_canonical_dependency_rows(fixture, ["a", "b", "c"],
            reader=fixture, max_logical_bytes=4096)
    assert fixture.calls == []
    previous = fact_dependencies.read_canonical_dependency_rows(fixture, ["a", "b", "c"],
        reader=fixture, max_logical_bytes=8192)
    fixture.calls.clear()
    assert _checkpoint_reader(monkeypatch, fixture) == previous
    assert fixture.calls == [["a", "b"], ["c"]]
    assert fixture.verified == ["page-1.parquet", "page-2.parquet"]


def test_checkpoint_sparse_cache_reuses_only_selected_rows_with_fresh_proofs(monkeypatch):
    fixture = _CheckpointSources()
    cache = fact_dependencies.CheckpointSourceEvidenceCache(
        max_manifests=4, max_rows=10, max_retained_bytes=32 * 1024,
    )
    first = _checkpoint_reader(monkeypatch, fixture, evidence_cache=cache)
    assert first == fixture.rows
    assert fixture.calls == [["a", "b"], ["c"]]
    assert cache.miss_count == 2 and cache.hit_count == 0

    # Caller mutation cannot affect retained evidence.  Current bytes are still
    # verified and current headers/contracts are checked on every hit.
    first["a"]["payload"]["state"] = "caller-mutated"
    fixture.calls.clear()
    fixture.verified.clear()
    second = _checkpoint_reader(monkeypatch, fixture, evidence_cache=cache)
    assert second == fixture.rows
    assert fixture.calls == []
    assert fixture.verified == ["page-1.parquet", "page-2.parquet"]
    assert fixture.cached_verified == [["a", "b"], ["c"]]
    assert cache.hit_count == 2


def test_checkpoint_sparse_cache_detach_adopt_keeps_fresh_proofs(tmp_path, monkeypatch):
    fixture = _CheckpointSources()
    first = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=None, object_store=None, temporary_directory=tmp_path / "first",
        max_dependency_objects=4, max_raw_mapping_rows=10,
    )
    first.enable_dependency_evidence_reuse()
    _checkpoint_reader(monkeypatch, fixture, evidence_cache=first._checkpoint_evidence_cache())
    cache = first.take_checkpoint_source_evidence_cache()

    second = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=None, object_store=None, temporary_directory=tmp_path / "second",
        max_dependency_objects=4, max_raw_mapping_rows=10,
    )
    assert second.adopt_checkpoint_source_evidence_cache(cache) is True
    second.enable_dependency_evidence_reuse()
    fixture.calls.clear()
    fixture.verified.clear()
    fixture.cached_verified.clear()
    rows = _checkpoint_reader(
        monkeypatch, fixture, evidence_cache=second._checkpoint_evidence_cache(),
    )
    assert rows == fixture.rows
    assert fixture.calls == []
    assert fixture.verified == ["page-1.parquet", "page-2.parquet"]
    assert fixture.cached_verified == [["a", "b"], ["c"]]


def test_checkpoint_sparse_cache_new_identity_or_manifest_change_forces_full_decode(monkeypatch):
    fixture = _CheckpointSources()
    cache = fact_dependencies.CheckpointSourceEvidenceCache(
        max_manifests=4, max_rows=10, max_retained_bytes=32 * 1024,
    )
    _checkpoint_reader(monkeypatch, fixture, evidence_cache=cache)
    fixture.calls.clear()
    fixture.manifests["page-1"] = SimpleNamespace(
        manifest_id="page-1", logical_byte_count=2500, object_key="page-1.parquet",
        object_sha256="changed", byte_count=100,
    )
    _checkpoint_reader(monkeypatch, fixture, evidence_cache=cache)
    assert fixture.calls == [["a", "b"]]

    fixture.calls.clear()
    fixture.rows["d"] = {"id": "d", "revision": 4, "known_at": BASE,
                          "market_commit_seq": 8, "payload": {"state": "d"}}
    fixture.page_for["d"] = "page-1"
    monkeypatch.setattr(fact_dependencies, "_catalog_manifest", lambda row: fixture.manifests[row["id"]])
    result = fact_dependencies.read_checkpoint_dependency_rows(
        fixture, ["a", "b", "d"], reader=fixture, byte_verifier=fixture,
        max_logical_bytes=4096, max_objects=10, evidence_cache=cache,
    )
    assert result == {identity: fixture.rows[identity] for identity in ("a", "b", "d")}
    assert fixture.calls == [["a", "b", "d"]]


def test_checkpoint_sparse_cache_hit_refuses_changed_current_header(monkeypatch):
    fixture = _CheckpointSources()
    cache = fact_dependencies.CheckpointSourceEvidenceCache(
        max_manifests=4, max_rows=10, max_retained_bytes=32 * 1024,
    )
    _checkpoint_reader(monkeypatch, fixture, evidence_cache=cache)
    fixture.rows["a"]["market_commit_seq"] += 1
    with pytest.raises(RuntimeError, match="canonical_archive_envelope_mismatch"):
        _checkpoint_reader(monkeypatch, fixture, evidence_cache=cache)


def test_checkpoint_sparse_cache_bounds_and_deterministic_disposal():
    cache = fact_dependencies.CheckpointSourceEvidenceCache(
        max_manifests=1, max_rows=2, max_retained_bytes=8192,
    )
    one = SimpleNamespace(manifest_id="one")
    two = SimpleNamespace(manifest_id="two")
    cache.remember(one, {"a": {"id": "a"}})
    cache.remember(two, {"b": {"id": "b"}})
    assert cache.get(one, ["a"]) is None
    assert cache.get(two, ["b"]) == {"b": {"id": "b"}}
    assert cache.eviction_count == 1 and cache.row_count == 1
    cache.dispose()
    assert cache.row_count == cache.retained_bytes == 0
    assert cache.get(two, ["b"]) is None


@pytest.mark.parametrize("coverage", ["identity", "ambiguous", "missing"])
def test_checkpoint_groups_refuse_missing_or_ambiguous_placement_before_read(monkeypatch, coverage):
    fixture = _CheckpointSources()
    fixture.coverage = coverage
    with pytest.raises(RuntimeError, match="canonical_checkpoint_source_.*coverage_invalid"):
        _checkpoint_reader(monkeypatch, fixture)
    assert fixture.calls == fixture.verified == []


@pytest.mark.parametrize("mode", ["single_page", "retained_rows", "object_budget", "cancelled", "identity_changed"])
def test_checkpoint_group_resource_and_integrity_refusals(monkeypatch, mode):
    fixture = _CheckpointSources()
    if mode == "single_page":
        fixture.manifests["page-1"].logical_byte_count = 4097
    elif mode == "retained_rows":
        fixture.rows["a"]["payload"]["large"] = "x" * 200
    elif mode == "object_budget":
        fixture.verify = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("archive_verification_byte_budget_exceeded"))
    elif mode == "identity_changed":
        fixture.read_rows_by_ids = lambda *a: {}
    checks = []
    def check():
        checks.append(1)
        if mode == "cancelled" and fixture.calls:
            raise RuntimeError("cancelled")
    with pytest.raises(RuntimeError):
        _checkpoint_reader(monkeypatch, fixture, check_budget=check)
    assert checks
    if mode in {"single_page", "object_budget"}:
        assert fixture.calls == []
    elif mode in {"retained_rows", "cancelled", "identity_changed"}:
        assert fixture.calls == ([] if mode == "identity_changed" else [["a", "b"]])


def test_checkpoint_cold_group_shares_existing_object_budget_with_other_dependencies(monkeypatch, tmp_path):
    from market_data.archive import FilesystemRawArchiveObjectStore
    from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
    import hashlib
    fixture = _CheckpointSources()
    for key in ("prior", "page-1", "page-2"):
        (tmp_path / (key + ".parquet")).write_bytes(b"x" * 100)
    objects = ArchiveVerificationBatch(FilesystemRawArchiveObjectStore(tmp_path),
        limits=ArchiveVerificationLimits(max_objects=3, max_bytes=250))
    digest = hashlib.sha256(b"x" * 100).hexdigest()
    objects.verify("prior.parquet", digest, expected_bytes=100)
    for manifest in fixture.manifests.values():
        manifest.object_sha256 = digest
    fixture.verify = objects.verify
    with pytest.raises(RuntimeError, match="archive_verification_byte_budget_exceeded"):
        _checkpoint_reader(monkeypatch, fixture)
    assert fixture.calls == [["a", "b"]]
    assert objects.byte_count == 200


def test_checkpoint_groups_join_page_members_across_bounded_metadata_batches(monkeypatch):
    fixture = _CheckpointSources()
    fixture.rows = {f"source-{index:04}": {"id": f"source-{index:04}", "revision": index + 1}
                    for index in range(1002)}
    fixture.page_for = {identity: "page-1" for identity in fixture.rows}
    monkeypatch.setattr(fact_dependencies, "_catalog_manifest", lambda row: fixture.manifests[row["id"]])
    execute = fixture.execute
    sizes = []
    def bounded(statement, params):
        sizes.append(len(params["ids"]))
        assert len(params["ids"]) <= 1000
        return execute(statement, params)
    fixture.execute = bounded
    assert fact_dependencies.read_checkpoint_dependency_rows(fixture, list(reversed(fixture.rows)),
        reader=fixture, byte_verifier=fixture, max_logical_bytes=1024**2, max_objects=10) == fixture.rows
    assert max(sizes) == 1000
    assert len(fixture.calls) == 1
    assert fixture.verified == ["page-1.parquet"]


def test_checkpoint_source_metadata_obeys_existing_object_count_limit_before_read(monkeypatch):
    fixture = _CheckpointSources()
    with pytest.raises(RuntimeError, match="canonical_checkpoint_source_object_budget_exceeded"):
        _checkpoint_reader(monkeypatch, fixture, max_objects=1)
    assert fixture.calls == fixture.verified == []
