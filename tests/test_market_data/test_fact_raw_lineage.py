from types import SimpleNamespace
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import logging

import pytest

from market_data.archive import (
    FilesystemRawArchiveObjectStore, RawArchiveReadLimits, encode_raw_records_to_parquet,
    iter_raw_archive_parquet, raw_archive_content_fingerprint, read_raw_archive_content_fingerprint,
    validate_raw_archive_parquet, RAW_ARCHIVE_SCHEMA_VERSION,
)
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.structure import build_raw_record_id
from portal.backend.service.storage.repos.fact_lineage import (
    BOOK_SCOPE_FIELDS, RawLineageEvidenceCache, resolve_canonical_raw_archive_refs,
)
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from tests.test_market_data.test_market_structure_archive import _record, _segment


def _fixture(tmp_path, ordinals=(1, 2), *, requested_channel="market_trades"):
    segment = _segment(tmp_path / "spool")
    records = [replace(_record(segment, ordinal), requested_channel=requested_channel) for ordinal in ordinals]
    encoded = encode_raw_records_to_parquet(records, archive_segment_id=segment.spool_segment_id,
                                           temporary_directory=tmp_path / "encode")
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    ack = store.put_verified(object_key="fixture.parquet", source_path=encoded.path, expected_sha256=encoded.sha256)
    manifest = dict(id="raw-manifest", definition_id=records[0].definition_id, session_id=records[0].session_id,
                    connection_epoch=0, spool_segment_id=segment.spool_segment_id, format="parquet", compression="zstd",
                    schema_version=RAW_ARCHIVE_SCHEMA_VERSION, byte_count=encoded.byte_count, record_count=len(records),
                    first_receive_ordinal=ordinals[0], last_receive_ordinal=ordinals[-1], object_key=ack.object_key,
                    object_uri=ack.object_uri, object_sha256=encoded.sha256, content_fingerprint=encoded.content_fingerprint)
    mappings = [{**manifest, "raw_record_id": record.raw_record_id, "object_row_index": index, "object_row_group": 0,
                 "mapped_segment_id": record.spool_segment_id, "mapped_session_id": record.session_id,
                 "mapped_epoch": record.connection_epoch, "mapped_ordinal": record.receive_ordinal,
                 "mapped_frame_sha256": record.raw_frame_sha256} for index, record in enumerate(records)]
    rows = [{"id": f"fact-{index}", "fact_type": "market.trade", "source_provider": record.provider,
             "source_venue": record.venue, "received_at": record.received_at,
             "provenance": {"_qt_trade_evidence": {"raw_record_id": record.raw_record_id,
                 "provider_product_id": record.provider_product_id, "connection_epoch": record.connection_epoch,
                 "receive_ordinal": record.receive_ordinal}}} for index, record in enumerate(records)]
    segment.close()
    return store, records, mappings, rows


class _Session:
    def __init__(self, mappings):
        self.mappings = mappings
        self.calls = []
    def execute(self, statement, params):
        sql = str(statement)
        self.calls.append((sql, params))
        assert "raw_archive_record_mappings" in sql and "expired.event_type='completed'" in sql
        assert "first_receive_ordinal <=" not in sql
        if "ids" in params:
            found = [row for row in self.mappings if row["raw_record_id"] in params["ids"]]
        elif "positions" in params:
            wanted = json.loads(params["positions"])
            found = [row for row in self.mappings if any(
                (row["definition_id"], row["mapped_session_id"], row["mapped_epoch"], row["mapped_ordinal"]) ==
                (item["definition_id"], item["session_id"], item["connection_epoch"], item["receive_ordinal"])
                for item in wanted)]
        else:
            prefixes = []
            while f"prefix_{len(prefixes)}_definition_id" in params:
                prefixes.append({name: params[f"prefix_{len(prefixes)}_{name}"] for name in
                    ("definition_id", "session_id", "connection_epoch", "first_receive_ordinal", "receive_ordinal")})
            found = [row for row in self.mappings if any(
                (row["definition_id"], row["mapped_session_id"], row["mapped_epoch"]) ==
                (item["definition_id"], item["session_id"], item["connection_epoch"])
                and item["first_receive_ordinal"] <= row["mapped_ordinal"] <= item["receive_ordinal"] for item in prefixes)]
        if "bound_ids" in params:
            found = [row for row in found if row["id"] in params["bound_ids"]]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: found[:params["limit"]]))


def _resolve(store, mappings, rows, **kwargs):
    verified = ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits())
    result = resolve_canonical_raw_archive_refs(_Session(mappings), rows=rows, object_store=store,
                                               byte_verifier=verified, **kwargs)
    return result, verified



def test_repeated_prefix_locators_preserve_each_witness_and_requested_budget(tmp_path):
    store, records, mappings, _ = _fixture(tmp_path)
    scope = {name: getattr(records[0], name) for name in BOOK_SCOPE_FIELDS} | {
        "first_receive_ordinal": 1, "receive_ordinal": 2, "requested_channel": "market_trades",
    }
    prefixes = [{**scope, "root_fact_version_id": f"root-{index}"} for index in range(600)]
    bindings = {item["root_fact_version_id"]: {"raw-manifest"} for item in prefixes}
    session = _Session(mappings)
    result = resolve_canonical_raw_archive_refs(session, rows=[], object_store=store,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
        book_prefix_ranges=prefixes, max_mapping_rows=1200, witness_manifest_ids=bindings)
    assert set(result) == {"raw-manifest"}
    assert len(session.calls) == 1
    assert sum(name.startswith("prefix_") for name in session.calls[0][1]) == 5
    assert session.calls[0][1]["bound_ids"] == ["raw-manifest"]

    # Deduplicating the locator must not discount the declared witness work.
    with pytest.raises(RuntimeError, match="prefix_budget_exceeded"):
        _resolve(store, mappings, [], book_prefix_ranges=prefixes, max_mapping_rows=1199)
    # The first root must still be checked even when later identical SQL scopes
    # carry different product/channel evidence or a different allowed placement.
    bad = [{**prefixes[0], "provider_product_id": "different-product"}, *prefixes[1:]]
    with pytest.raises(RuntimeError, match="witness_mismatch.*fact_version_id=root-0.*provider_product_id"):
        _resolve(store, mappings, [], book_prefix_ranges=bad, max_mapping_rows=1200)
    with pytest.raises(RuntimeError, match="mapping_missing"):
        _resolve(store, mappings, [], book_prefix_ranges=prefixes, max_mapping_rows=1200,
                 witness_manifest_ids={**bindings, "root-0": set()})
    assert len(prefixes) == len(bindings) == 600
    assert bindings["root-0"] == {"raw-manifest"}


def test_exact_revisions_check_each_raw_row_and_share_one_object_read(tmp_path, caplog):
    store, _, mappings, rows = _fixture(tmp_path)
    with caplog.at_level(logging.INFO):
        refs, verified = _resolve(store, mappings, rows)
    assert list(refs) == ["raw-manifest"]
    assert verified.byte_count == mappings[0]["byte_count"]
    assert len(verified.objects) == 1
    assert ("canonical_raw_lineage_lookup | mode=record outcome=completed "
            "requested_keys=2 witnesses=2 bound_manifests=None prior_matches=0 "
            "returned_rows=2") in caplog.text
    assert ("canonical_raw_lineage_resolution | outcome=completed requested_keys=2 "
            "mapping_rows=2 selected_manifests=1 decoded_rows=2") in caplog.text
    assert "canonical_raw_lineage_object_validation" not in caplog.text
    assert rows[0]["id"] not in caplog.text
    assert mappings[0]["raw_record_id"] not in caplog.text


def test_batch_resolver_is_explicit_and_logs_one_bounded_aggregate(tmp_path, caplog, monkeypatch):
    from portal.backend.service.storage.repos import fact_lineage
    store, _, mappings, rows = _fixture(tmp_path / "first", (1, 2))
    other_store, _, other_mappings, other_rows = _fixture(tmp_path / "second", (3, 4))
    acknowledgement = store.put_verified(
        object_key="second.parquet",
        source_path=other_store.local_path("fixture.parquet"),
        expected_sha256=other_mappings[0]["object_sha256"],
    )
    for mapping in other_mappings:
        mapping.update(
            id="second-manifest",
            object_key=acknowledgement.object_key,
            object_uri=acknowledgement.object_uri,
        )
    all_mappings = [*mappings, *other_mappings]
    selected_rows = [rows[0], other_rows[0]]
    original = fact_lineage.validate_raw_archive_parquet
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(fact_lineage, "validate_raw_archive_parquet", counted)
    assert _resolve(store, all_mappings, selected_rows)[0]
    assert calls == 0, "Dataset/history callers retain the legacy reader by default"
    caplog.clear()
    with caplog.at_level(logging.INFO):
        assert _resolve(
            store, all_mappings, selected_rows,
            identity_spool_directory=tmp_path / "identity",
        )[0]
    assert calls == 2
    events = [record.getMessage() for record in caplog.records
              if record.getMessage().startswith("canonical_raw_lineage_object_validation |")]
    assert len(events) == 1
    assert "objects=2 records=4 batches=2" in events[0]
    assert "timing_semantics=decode_envelope_includes_read_conversion_validation" in events[0]
    assert "manifest_id=" not in events[0]


def test_batch_resolver_preserves_custom_stop_reason(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)

    class RequestedStop(RuntimeError):
        stop_reason = "operator_stop"

    requested = RequestedStop("stop now")

    def stop():
        raise requested

    with pytest.raises(RequestedStop) as caught:
        _resolve(
            store,
            mappings,
            rows,
            identity_spool_directory=tmp_path / "identity",
            check_budget=stop,
        )
    assert caught.value is requested
    assert caught.value.stop_reason == "operator_stop"


def test_run_scoped_evidence_reuses_decode_but_freshly_checksums_each_page(
        tmp_path, monkeypatch):
    from portal.backend.service.storage.repos import fact_lineage
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    validation_calls = verification_calls = 0
    validate = fact_lineage.validate_raw_archive_parquet
    verify = ArchiveVerificationBatch.verify

    def counted_validate(*args, **kwargs):
        nonlocal validation_calls
        validation_calls += 1
        return validate(*args, **kwargs)

    def counted_verify(self, *args, **kwargs):
        nonlocal verification_calls
        verification_calls += 1
        return verify(self, *args, **kwargs)

    monkeypatch.setattr(fact_lineage, "validate_raw_archive_parquet", counted_validate)
    monkeypatch.setattr(ArchiveVerificationBatch, "verify", counted_verify)
    first, first_bytes = _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )
    second, second_bytes = _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )
    assert first == second
    assert validation_calls == 1
    assert verification_calls == 2, "a cache hit must still checksum current bytes"
    assert first_bytes.byte_count == second_bytes.byte_count == mappings[0]["byte_count"]
    assert (cache.hits, cache.misses, cache.invalidations) == (1, 1, 0)
    assert cache.object_count == 1 and cache.witness_claim_count == 2


def test_repository_requires_an_explicit_executor_scope_for_cross_call_reuse(tmp_path):
    repository = PostgresCanonicalFactArchiveRepository(
        database=object(), object_store=object(), temporary_directory=tmp_path,
        max_dependency_objects=3, max_raw_mapping_rows=5,
    )
    first = repository._dependency_evidence_cache()
    second = repository._dependency_evidence_cache()
    assert first is not second

    repository.enable_dependency_evidence_reuse()
    scoped = repository._dependency_evidence_cache()
    assert repository._dependency_evidence_cache() is scoped
    repository.dispose_dependency_evidence()
    assert scoped.disposed
    assert repository._dependency_evidence_cache() is not scoped


def test_run_scoped_evidence_refuses_same_byte_object_replacement(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]
    path = store.local_path("fixture.parquet")
    replacement = path.with_suffix(".replacement")
    replacement.write_bytes(path.read_bytes())
    replacement.replace(path)
    with pytest.raises(RuntimeError, match="cached_object_changed"):
        _resolve(
            store, mappings, rows, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache,
        )
    assert cache.invalidations == 1 and cache.object_count == 0


def test_run_scoped_evidence_fresh_checksum_rejects_between_page_mutation(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]
    path = store.local_path("fixture.parquet")
    changed = bytearray(path.read_bytes())
    changed[len(changed) // 2] ^= 1
    path.write_bytes(changed)
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        _resolve(
            store, mappings, rows, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache,
        )


def test_run_scoped_evidence_never_reuses_changed_mapping_authority(
        tmp_path, monkeypatch):
    from portal.backend.service.storage.repos import fact_lineage
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    validation_calls = 0
    validate = fact_lineage.validate_raw_archive_parquet

    def counted(*args, **kwargs):
        nonlocal validation_calls
        validation_calls += 1
        return validate(*args, **kwargs)

    monkeypatch.setattr(fact_lineage, "validate_raw_archive_parquet", counted)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]
    changed = [{**item, "mapped_segment_id": "changed-authority"} for item in mappings]
    with pytest.raises(RuntimeError, match="mapping_mismatch"):
        _resolve(
            store, changed, rows, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache,
        )
    assert validation_calls == 2 and cache.hits == 0


def test_run_scoped_evidence_never_reuses_changed_witness_claim(
        tmp_path, monkeypatch):
    from portal.backend.service.storage.repos import fact_lineage
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    validation_calls = 0
    validate = fact_lineage.validate_raw_archive_parquet

    def counted(*args, **kwargs):
        nonlocal validation_calls
        validation_calls += 1
        return validate(*args, **kwargs)

    monkeypatch.setattr(fact_lineage, "validate_raw_archive_parquet", counted)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]
    changed = [dict(row) for row in rows]
    changed[0]["provenance"] = {
        "_qt_trade_evidence": {
            **rows[0]["provenance"]["_qt_trade_evidence"],
            "provider_product_id": "changed-witness-product",
        },
    }
    with pytest.raises(RuntimeError, match="witness_mismatch.*provider_product_id"):
        _resolve(
            store, mappings, changed, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache,
        )
    assert validation_calls == 2 and cache.hits == 0


def test_run_scoped_evidence_final_fence_detects_mutation_after_cache_lookup(
        tmp_path, monkeypatch):
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]
    path = store.local_path("fixture.parquet")
    lookup = cache.lookup

    def mutate_after_lookup(**kwargs):
        evidence = lookup(**kwargs)
        changed = bytearray(path.read_bytes())
        changed[len(changed) // 2] ^= 1
        path.write_bytes(changed)
        return evidence

    monkeypatch.setattr(cache, "lookup", mutate_after_lookup)
    with pytest.raises(RuntimeError, match="archive_verification_object_changed"):
        _resolve(
            store, mappings, rows, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache,
        )


def test_run_scoped_evidence_never_replaces_fresh_placement_authority(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]
    with pytest.raises(RuntimeError, match="mapping_missing"):
        _resolve(
            store, [], rows, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache,
        )
    assert cache.hits == 0, "missing or expired placement authority must fail before cache lookup"


@pytest.mark.parametrize(
    "field",
    ["first_received_at", "last_received_at", "uploaded_at", "acknowledged_at"],
)
def test_run_scoped_evidence_refuses_changed_manifest_identity(tmp_path, field):
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]
    changed = [{**item, field: "different-catalog-version"} for item in mappings]
    with pytest.raises(RuntimeError, match="cached_object_changed"):
        _resolve(
            store, changed, rows, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache,
        )
    assert cache.invalidations == 1 and cache.object_count == 0


def test_run_scoped_evidence_is_bounded_and_disposed(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)
    too_small = RawLineageEvidenceCache(max_objects=1, max_witness_claims=1)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity-small",
        evidence_cache=too_small,
    )[0]
    assert too_small.object_count == 0 and too_small.witness_claim_count == 0

    cache = RawLineageEvidenceCache(max_objects=1, max_witness_claims=2)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]
    assert cache.object_count == 1 and cache.witness_claim_count == 2
    cache.dispose()
    assert cache.object_count == 0 and cache.witness_claim_count == 0
    with pytest.raises(RuntimeError, match="cache_disposed"):
        _resolve(
            store, mappings, rows, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache,
        )


def test_run_scoped_evidence_hit_still_honors_cancellation(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)
    cache = RawLineageEvidenceCache(max_objects=10, max_witness_claims=10)
    assert _resolve(
        store, mappings, rows, identity_spool_directory=tmp_path / "identity",
        evidence_cache=cache,
    )[0]

    class RequestedStop(RuntimeError):
        stop_reason = "operator_stop"

    requested = RequestedStop("stop before reuse")

    def stop():
        raise requested

    with pytest.raises(RequestedStop) as caught:
        _resolve(
            store, mappings, rows, identity_spool_directory=tmp_path / "identity",
            evidence_cache=cache, check_budget=stop,
        )
    assert caught.value is requested and cache.hits == 0


def test_bound_book_positions_remain_bounded_with_unbound_trade_witnesses(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path)
    book = {**rows[1], "fact_type": "market.bbo", "provenance": {
        "_qt_bbo_evidence": {"source_position": {
            name: getattr(records[1], name) for name in (*BOOK_SCOPE_FIELDS, "receive_ordinal")}}}}
    # A later placement of this book frame is valid, but this root already
    # pins its original placement. It must not consume the mixed page's budget.
    extra = {**mappings[1], "id": "later-book-placement"}
    session = _Session([*mappings, extra])
    refs = resolve_canonical_raw_archive_refs(session, rows=[rows[0], book], object_store=store,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
        witness_manifest_ids={book["id"]: {"raw-manifest"}}, max_mapping_rows=2)
    assert set(refs) == {"raw-manifest"}
    record_sql, record_params = next((sql, params) for sql, params in session.calls if "ids" in params)
    position_sql, position_params = next(
        (sql, params) for sql, params in session.calls if "positions" in params
    )
    assert "bound_ids" not in record_params, "the trade witness must remain unrestricted"
    assert "WITH requested_records AS MATERIALIZED" in record_sql
    assert "JOIN requested_records AS records" in record_sql
    assert "mappings.raw_record_id = ANY" not in record_sql
    assert position_params["bound_ids"] == ["raw-manifest"]
    assert "WITH requested_positions AS MATERIALIZED" in position_sql
    assert "JOIN requested_positions AS positions" in position_sql
    assert "manifests.first_receive_ordinal<=positions.receive_ordinal" in position_sql
    assert "manifests.last_receive_ordinal>=positions.receive_ordinal" in position_sql
    assert "EXISTS (SELECT 1 FROM jsonb_to_recordset" not in position_sql


def test_bound_record_witnesses_prune_manifests_before_mapping_lookup(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)
    session = _Session(mappings)
    refs = resolve_canonical_raw_archive_refs(session, rows=rows, object_store=store,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
        bound_manifest_ids={"raw-manifest"})
    assert set(refs) == {"raw-manifest"}
    record_sql, params = next((sql, params) for sql, params in session.calls if "ids" in params)
    assert params["bound_ids"] == ["raw-manifest"]
    assert "WITH requested_records AS MATERIALIZED" in record_sql
    assert "candidate_manifests AS MATERIALIZED" in record_sql
    assert "JOIN requested_records AS records" in record_sql
    assert "JOIN candidate_manifests AS manifests" in record_sql
    assert "records.raw_record_id=mappings.raw_record_id" in record_sql
    assert "mappings.raw_record_id = ANY" not in record_sql


@pytest.mark.parametrize("ids", [["one"], ["one", 'quote"\\newline\n', "\u03b1\U0001f680"]])
def test_streamed_content_fingerprint_preserves_exact_v1_identity(ids):
    hashes = [hashlib.sha256(identity.encode()).hexdigest() for identity in ids]
    expected = hashlib.sha256(json.dumps({"schema_version": "market.raw_archive_content.v1",
        "raw_record_ids": ids, "raw_frame_sha256": hashes}, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True).encode()).hexdigest()
    assert raw_archive_content_fingerprint(raw_record_ids=iter(ids), raw_frame_sha256=iter(hashes)) == expected


@pytest.mark.parametrize("ids,hashes", [([], []), (["one"], []), ([], ["a" * 64]), ([None], ["a" * 64]), (["one"], [""])])
def test_content_fingerprint_rejects_missing_or_misaligned_identity_streams(ids, hashes):
    with pytest.raises(RuntimeError, match="market_archive_content_identity_"):
        raw_archive_content_fingerprint(raw_record_ids=iter(ids), raw_frame_sha256=iter(hashes))


def test_batch_validation_preserves_full_record_and_v1_fingerprint_contract(tmp_path):
    store, records, mappings, _ = _fixture(tmp_path, (1, 2, 3))
    path = store.local_path("fixture.parquet")
    checks = []
    evidence = validate_raw_archive_parquet(
        path,
        limits=RawArchiveReadLimits(batch_rows=2),
        witness_indices={0, 2},
        identity_spool_directory=tmp_path / "identity",
        identity_memory_spool_bytes=1,
        check_budget=lambda: checks.append(True),
    )
    assert evidence.record_count == 3
    assert (evidence.first_receive_ordinal, evidence.last_receive_ordinal) == (1, 3)
    assert evidence.source_scope == (
        records[0].definition_id, records[0].session_id, records[0].connection_epoch,
    )
    assert evidence.witness_records == {0: records[0], 2: records[2]}
    assert evidence.content_fingerprint == mappings[0]["content_fingerprint"]
    assert evidence.content_fingerprint == read_raw_archive_content_fingerprint(
        path, limits=RawArchiveReadLimits(batch_rows=2),
    )
    assert evidence.identity_spilled_to_disk is True
    assert evidence.batch_count == 2
    assert evidence.archive_open_seconds >= 0
    assert evidence.decode_seconds + 1e-6 >= (
        evidence.parquet_batch_read_seconds
        + evidence.batch_conversion_seconds
        + evidence.record_validation_seconds
    )
    assert evidence.fingerprint_finalize_seconds >= 0
    assert len(checks) >= evidence.batch_count + 2
    in_memory = validate_raw_archive_parquet(
        path,
        limits=RawArchiveReadLimits(batch_rows=2),
        identity_spool_directory=tmp_path / "identity",
        identity_memory_spool_bytes=1024 * 1024,
    )
    assert in_memory.content_fingerprint == evidence.content_fingerprint
    assert in_memory.identity_spilled_to_disk is False
    assert not list((tmp_path / "identity").iterdir())


def test_batch_validation_cancellation_remains_checked_between_rows(tmp_path):
    store, _, _, _ = _fixture(tmp_path, (1, 2, 3))
    calls = 0

    def cancelled():
        nonlocal calls
        calls += 1
        # File-open, row-group, and batch checks precede row checks in this
        # fixture. Refuse between the first and second physical rows so the
        # batch reader never weakens the former per-row cancellation cadence.
        if calls == 5:
            raise RuntimeError("cancelled")

    with pytest.raises(RuntimeError, match="cancelled"):
        validate_raw_archive_parquet(
            store.local_path("fixture.parquet"),
            limits=RawArchiveReadLimits(batch_rows=3),
            identity_spool_directory=tmp_path / "identity",
            check_budget=cancelled,
        )
    assert calls == 5
    assert not list((tmp_path / "identity").iterdir())


def test_batch_validation_removes_spilled_identity_file_after_failure(tmp_path):
    store, _, _, _ = _fixture(tmp_path, (1, 2, 3))
    calls = 0

    def cancelled_after_spill():
        nonlocal calls
        calls += 1
        if calls == 7:
            raise RuntimeError("cancelled after spill")

    with pytest.raises(RuntimeError, match="cancelled after spill"):
        validate_raw_archive_parquet(
            store.local_path("fixture.parquet"),
            limits=RawArchiveReadLimits(batch_rows=3),
            identity_spool_directory=tmp_path / "identity",
            identity_memory_spool_bytes=1,
            check_budget=cancelled_after_spill,
        )
    assert calls == 7
    assert not list((tmp_path / "identity").iterdir())


@pytest.mark.parametrize("column", ["raw_record_id", "raw_frame_sha256"])
def test_batch_validation_rejects_identity_or_frame_checksum_mutation(tmp_path, column):
    import pyarrow as pa
    import pyarrow.parquet as pq
    store, _, _, _ = _fixture(tmp_path)
    path = store.local_path("fixture.parquet")
    table = pq.ParquetFile(path).read()
    index = table.schema.get_field_index(column)
    values = table.column(index).to_pylist()
    values[0] = ("raw_" + "0" * 64) if column == "raw_record_id" else "0" * 64
    table = table.set_column(index, table.schema.field(index), pa.array(values, type=pa.string()))
    pq.write_table(table, path, compression="zstd", data_page_version="2.0")
    with pytest.raises(ValueError, match=(
            "raw_record_id mismatch" if column == "raw_record_id" else "frame checksum mismatch")):
        validate_raw_archive_parquet(
            path, limits=RawArchiveReadLimits(),
            identity_spool_directory=tmp_path / "identity",
        )


def test_batch_validation_fingerprint_commits_physical_not_normalized_identity(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    store, _, mappings, _ = _fixture(tmp_path)
    path = store.local_path("fixture.parquet")
    table = pq.ParquetFile(path).read()
    index = table.schema.get_field_index("raw_frame_sha256")
    values = table.column(index).to_pylist()
    values[0] = values[0].upper()
    table = table.set_column(
        index, table.schema.field(index), pa.array(values, type=pa.string()),
    )
    pq.write_table(table, path, compression="zstd", data_page_version="2.0")
    evidence = validate_raw_archive_parquet(
        path, limits=RawArchiveReadLimits(),
        identity_spool_directory=tmp_path / "identity",
    )
    assert evidence.content_fingerprint != mappings[0]["content_fingerprint"]


@pytest.mark.parametrize("mutation,match", [
    ("order", "record order mismatch"),
    ("scope", "source scope differs"),
])
def test_batch_validation_rejects_valid_rows_with_invalid_object_order_or_scope(
        tmp_path, mutation, match):
    import pyarrow as pa
    import pyarrow.parquet as pq
    store, _, _, _ = _fixture(tmp_path)
    path = store.local_path("fixture.parquet")
    table = pq.ParquetFile(path).read()
    values = {name: table.column(name).to_pylist() for name in table.schema.names}
    if mutation == "order":
        values["receive_ordinal"][1] = values["receive_ordinal"][0]
    else:
        values["session_id"][1] = "different-session"
    values["raw_record_id"][1] = build_raw_record_id(
        definition_id=values["definition_id"][1],
        session_id=values["session_id"][1],
        connection_epoch=values["connection_epoch"][1],
        receive_ordinal=values["receive_ordinal"][1],
        raw_frame_sha256=values["raw_frame_sha256"][1],
    )
    changed = pa.Table.from_arrays(
        [pa.array(values[field.name], type=field.type) for field in table.schema],
        schema=table.schema,
    )
    pq.write_table(changed, path, compression="zstd", data_page_version="2.0")
    with pytest.raises(RuntimeError, match=match):
        validate_raw_archive_parquet(
            path, limits=RawArchiveReadLimits(batch_rows=1),
            identity_spool_directory=tmp_path / "identity",
        )


@pytest.mark.parametrize("batch_rows", [1, 2, 128])
def test_file_fingerprint_uses_bounded_identity_columns_not_a_second_frame_decode(tmp_path, monkeypatch, batch_rows):
    from market_data import archive
    import pyarrow.parquet as pq
    store, _, mappings, _ = _fixture(tmp_path, (1, 2, 3))
    path = store.local_path("fixture.parquet")
    with pq.ParquetFile(path) as parquet:
        table = parquet.read()
    pq.write_table(table, path, compression="zstd", row_group_size=1)
    open_file = archive._open_raw_archive_parquet
    selected = []

    @contextmanager
    def inspect_columns(*args, **kwargs):
        with open_file(*args, **kwargs) as parquet:
            def batches(**options):
                selected.append(options["columns"])
                assert options["batch_size"] == batch_rows and options["use_threads"] is False
                return parquet.iter_batches(**options)
            yield SimpleNamespace(iter_batches=batches, metadata=parquet.metadata)

    monkeypatch.setattr(archive, "_open_raw_archive_parquet", inspect_columns)
    assert read_raw_archive_content_fingerprint(path, limits=RawArchiveReadLimits(batch_rows=batch_rows)) == mappings[0]["content_fingerprint"]
    assert selected == [["raw_frame_sha256"], ["raw_record_id"]]


def test_fingerprint_includes_raw_rows_not_referenced_by_this_canonical_page(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path)
    mappings[0]["content_fingerprint"] = raw_archive_content_fingerprint(
        raw_record_ids=[records[0].raw_record_id], raw_frame_sha256=[records[0].raw_frame_sha256])
    with pytest.raises(RuntimeError, match="content_fingerprint_mismatch"):
        _resolve(store, mappings, rows[:1])


def test_fingerprint_verification_obeys_cancellation_and_file_stability(tmp_path, monkeypatch):
    from portal.backend.service.market.canonical_retention import CanonicalRetentionStopRequested
    from portal.backend.service.storage.repos import fact_lineage
    store, _, mappings, rows = _fixture(tmp_path)
    path = store.local_path("fixture.parquet")
    checks = 0
    def cancel():
        nonlocal checks
        checks += 1
        if checks == 5:
            raise CanonicalRetentionStopRequested("stop fingerprint scan")
    with pytest.raises(CanonicalRetentionStopRequested, match="stop fingerprint scan"):
        read_raw_archive_content_fingerprint(path, limits=RawArchiveReadLimits(batch_rows=1), check_budget=cancel)
    original_validator = fact_lineage.validate_raw_archive_parquet
    def changed(*args, **kwargs):
        result = original_validator(*args, **kwargs)
        with path.open("ab") as file:
            file.write(b"changed after identity scan")
        return result
    monkeypatch.setattr(fact_lineage, "validate_raw_archive_parquet", changed)
    with pytest.raises(RuntimeError, match="changed"):
        _resolve(
            store, mappings, rows,
            identity_spool_directory=tmp_path / "identity",
        )


@pytest.mark.parametrize("field,value,match", [
    ("object_row_index", 1, "mapping_mismatch"), ("object_row_index", 9, "row_index_invalid"),
    ("object_row_group", 1, "row_index_invalid"), ("mapped_frame_sha256", "a" * 64, "mapping_mismatch"),
    ("mapped_segment_id", "wrong-segment", "mapping_mismatch"), ("mapped_epoch", 1, "mapping_mismatch"),
    ("record_count", 3, "archive_coverage_mismatch"), ("last_receive_ordinal", 3, "archive_coverage_mismatch"),
    ("session_id", "wrong-session", "archive_scope_mismatch"), ("schema_version", "unknown", "archive_format_invalid"),
    ("content_fingerprint", "a" * 64, "content_fingerprint_mismatch"),
])
def test_manifest_or_mapping_claim_is_not_proof_of_the_physical_raw_row(tmp_path, field, value, match):
    store, _, mappings, rows = _fixture(tmp_path)
    mappings[0][field] = value
    with pytest.raises(RuntimeError, match=match):
        _resolve(store, mappings, rows[:1])


@pytest.mark.parametrize("mode", ["product", "provider", "received_at", "epoch", "ordinal", "missing_raw", "boolean_epoch"])
def test_raw_witness_must_match_the_archived_revisions_exact_evidence(tmp_path, mode):
    store, records, mappings, rows = _fixture(tmp_path)
    row = rows[0]
    evidence = row["provenance"]["_qt_trade_evidence"]
    if mode == "product": evidence["provider_product_id"] = "ETH-USD"
    if mode == "provider": row["source_provider"] = "OTHER"
    if mode == "received_at": row["received_at"] = records[1].received_at
    if mode == "epoch": evidence["connection_epoch"] = 1
    if mode == "ordinal": evidence["receive_ordinal"] = 2
    if mode == "missing_raw": evidence.pop("raw_record_id")
    if mode == "boolean_epoch": evidence["connection_epoch"] = False
    with pytest.raises(RuntimeError, match="canonical_raw_lineage_"):
        _resolve(store, mappings, [row])


def test_a_manifest_range_cannot_fill_a_missing_book_record_mapping(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path, (1, 3))
    position = {"definition_id": records[0].definition_id, "session_id": records[0].session_id,
                "connection_epoch": 0, "receive_ordinal": 2, "provider_product_id": "BTC-USD"}
    row = {**rows[0], "fact_type": "market.bbo", "provenance": {"_qt_bbo_evidence": {"source_position": position}}}
    with pytest.raises(RuntimeError, match="mapping_missing"):
        _resolve(store, mappings, [row])
    position["receive_ordinal"] = 1
    assert list(_resolve(store, mappings, [row])[0]) == ["raw-manifest"]
    position["connection_epoch"] = 1
    with pytest.raises(RuntimeError, match="mapping_missing"):
        _resolve(store, mappings, [row])


def test_exact_l2_evidence_binds_both_identity_and_connection_position(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path)
    evidence = {**rows[0]["provenance"]["_qt_trade_evidence"], "definition_id": records[0].definition_id,
                "session_id": records[0].session_id}
    row = {**rows[0], "fact_type": "market.l2_book", "provenance": {"_qt_l2_evidence": evidence}}
    assert list(_resolve(store, mappings, [row])[0]) == ["raw-manifest"]
    evidence["session_id"] = "different-connection"
    with pytest.raises(RuntimeError, match="witness_mismatch.*session_id"):
        _resolve(store, mappings, [row])


def _book_root(record, trade_row):
    return {**trade_row, "fact_type": "market.l2_book", "provenance": {"_qt_l2_evidence": {
        **trade_row["provenance"]["_qt_trade_evidence"], "definition_id": record.definition_id,
        "session_id": record.session_id}}}


def test_book_prefix_proves_each_unreferenced_position_not_just_the_last_frame(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path, (1, 2, 3), requested_channel="level2")
    root = _book_root(records[-1], rows[-1])
    assert _resolve(store, mappings, [root], preserve_book_prefixes=True)[0]
    for missing in (0, 1):
        without = [row for index, row in enumerate(mappings) if index != missing]
        assert _resolve(store, without, [root])[0], "the previous last-frame proof cannot see this hole"
        with pytest.raises(RuntimeError, match="mapping_missing.*raw_position"):
            _resolve(store, without, [root], preserve_book_prefixes=True)
    changed = [dict(row) for row in mappings]
    changed[1]["mapped_frame_sha256"] = "a" * 64
    with pytest.raises(RuntimeError, match="mapping_mismatch"):
        _resolve(store, changed, [root], preserve_book_prefixes=True)


def test_book_prefix_binds_all_required_objects_and_respects_the_root_boundary(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path / "opening", (1,), requested_channel="level2")
    other_store, later_records, later_mappings, later_rows = _fixture(tmp_path / "tail", (2, 3), requested_channel="level2")
    ack = store.put_verified(object_key="tail.parquet", source_path=other_store.local_path("fixture.parquet"),
                             expected_sha256=later_mappings[0]["object_sha256"])
    for mapping in later_mappings:
        mapping.update(id="tail", object_key=ack.object_key, object_uri=ack.object_uri)
    root = _book_root(later_records[-1], later_rows[-1])
    references = _resolve(store, [*mappings, *later_mappings], [root], preserve_book_prefixes=True)[0]
    assert set(references) == {"raw-manifest", "tail"}
    with pytest.raises(RuntimeError, match="mapping_missing"):
        _resolve(store, [*mappings, *later_mappings], [root], preserve_book_prefixes=True, bound_manifest_ids={"tail"})
    opening = _book_root(records[0], rows[0])
    assert set(_resolve(store, [*mappings, *later_mappings], [opening], preserve_book_prefixes=True)[0]) == {"raw-manifest"}
    store.local_path("fixture.parquet").unlink()
    with pytest.raises(FileNotFoundError):
        _resolve(store, [*mappings, *later_mappings], [root], preserve_book_prefixes=True)


def test_book_prefix_rejects_wrong_product_channel_and_work_beyond_its_bound(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path, (1, 2, 3))
    root = _book_root(records[-1], rows[-1])
    with pytest.raises(RuntimeError, match="witness_mismatch.*requested_channel"):
        _resolve(store, mappings, [root], preserve_book_prefixes=True)
    with pytest.raises(RuntimeError, match="prefix_budget_exceeded"):
        _resolve(store, mappings, [root], preserve_book_prefixes=True, max_mapping_rows=2)


@pytest.mark.parametrize("field", ["provider", "venue"])
def test_trade_coverage_endpoint_rechecks_raw_source_identity(tmp_path, field):
    store, records, mappings, _ = _fixture(tmp_path, (1,))
    record = records[0]
    endpoint = {name: getattr(record, name) for name in (*BOOK_SCOPE_FIELDS, "provider", "venue", "raw_record_id")}
    endpoint.update(root_fact_version_id="flow:opening", first_receive_ordinal=1, receive_ordinal=1,
                    requested_channel="market_trades")
    assert _resolve(store, mappings, [], book_prefix_ranges=[endpoint])[0]
    endpoint[field] = "different-source"
    with pytest.raises(RuntimeError, match=f"witness_mismatch.*field={field}"):
        _resolve(store, mappings, [], book_prefix_ranges=[endpoint])


def test_qt_authored_book_features_bind_provider_frames_without_relabeling_the_author(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path)
    position = {"definition_id": records[0].definition_id, "session_id": records[0].session_id,
                "connection_epoch": 0, "receive_ordinal": 1, "provider_product_id": "BTC-USD"}
    row = {**rows[0], "fact_type": "market.bbo", "source_provider": "QT", "source_venue": "",
           "provenance": {"_qt_bbo_evidence": {"source_position": position}}}
    assert list(_resolve(store, mappings, [row])[0]) == ["raw-manifest"]


@pytest.mark.parametrize("bound_witness", [False, True])
def test_aliases_with_different_raw_bytes_at_one_book_position_are_ambiguous(tmp_path, bound_witness):
    store, records, mappings, rows = _fixture(tmp_path)
    extra = {**mappings[0], "id": "other-manifest", "raw_record_id": "another-raw-id"}
    evidence = {"definition_id": records[0].definition_id, "session_id": records[0].session_id,
                "connection_epoch": 0, "receive_ordinal": 1, "provider_product_id": "BTC-USD"}
    row = {**rows[0], "fact_type": "market.bbo", "provenance": {"_qt_bbo_evidence": {"source_position": evidence}}}
    bindings = {row["id"]: {"raw-manifest", "other-manifest"}} if bound_witness else None
    with pytest.raises(RuntimeError, match="position_ambiguous"):
        _resolve(store, [*mappings, extra], [row], witness_manifest_ids=bindings)


@pytest.mark.parametrize("limit", ["max_rows", "max_file_bytes", "max_logical_bytes", "max_row_group_bytes"])
def test_streaming_raw_reader_rejects_work_outside_its_budget(tmp_path, limit):
    store, records, _, _ = _fixture(tmp_path)
    path = store.local_path("fixture.parquet")
    assert list(iter_raw_archive_parquet(path, limits=RawArchiveReadLimits(batch_rows=1))) == records
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        list(iter_raw_archive_parquet(path, limits=RawArchiveReadLimits(**{limit: 1})))
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        read_raw_archive_content_fingerprint(path, limits=RawArchiveReadLimits(**{limit: 1}))
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        validate_raw_archive_parquet(
            path,
            limits=RawArchiveReadLimits(**{limit: 1}),
            identity_spool_directory=tmp_path / "identity",
        )


def test_mapping_result_limits_never_silently_truncate_lineage(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)
    duplicate = {**mappings[0], "id": "compacted-alias"}
    with pytest.raises(RuntimeError, match="mapping_budget_exceeded"):
        _resolve(store, [*mappings, duplicate], rows[:1], max_mapping_rows=1)


def test_raw_decode_budget_is_shared_across_distinct_objects(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path / "first", (1, 2))
    other_store, _, other_mappings, other_rows = _fixture(tmp_path / "second", (3, 4))
    ack = store.put_verified(object_key="second.parquet", source_path=other_store.local_path("fixture.parquet"),
                             expected_sha256=other_mappings[0]["object_sha256"])
    for mapping in other_mappings:
        mapping.update(id="second-manifest", object_key=ack.object_key, object_uri=ack.object_uri)
    with pytest.raises(RuntimeError, match="decode_budget_exceeded"):
        _resolve(store, [*mappings, *other_mappings], [rows[0], other_rows[0]], limits=RawArchiveReadLimits(max_rows=2))


def test_reverification_preserves_its_bound_copy_when_a_smaller_placement_appears(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path)
    encoded = encode_raw_records_to_parquet(records, archive_segment_id="x", temporary_directory=tmp_path / "alternate")
    assert encoded.byte_count < mappings[0]["byte_count"]
    ack = store.put_verified(object_key="alternate.parquet", source_path=encoded.path, expected_sha256=encoded.sha256)
    alternate = [{**mapping, "id": "alternate-manifest", "object_key": ack.object_key,
                  "object_uri": ack.object_uri, "object_sha256": encoded.sha256, "byte_count": encoded.byte_count,
                  "spool_segment_id": "x"} for mapping in mappings]
    all_mappings = [*mappings, *alternate]
    assert list(_resolve(store, all_mappings, rows)[0]) == ["alternate-manifest"]
    assert list(_resolve(store, all_mappings, rows, bound_manifest_ids={"raw-manifest"})[0]) == ["raw-manifest"]
    store.local_path("fixture.parquet").unlink()
    with pytest.raises(FileNotFoundError):
        _resolve(store, all_mappings, rows, bound_manifest_ids={"raw-manifest"})


def test_bounded_raw_reader_rejects_physical_type_drift(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    store, _, _, _ = _fixture(tmp_path)
    path = store.local_path("fixture.parquet")
    table = pq.ParquetFile(path).read()
    index = table.schema.get_field_index("connection_epoch")
    changed = table.schema.set(index, pa.field("connection_epoch", pa.float64(), nullable=False))
    pq.write_table(table.cast(changed), path, compression="zstd")
    with pytest.raises(RuntimeError, match="raw schema differs"):
        list(iter_raw_archive_parquet(path, limits=RawArchiveReadLimits()))
    with pytest.raises(RuntimeError, match="raw schema differs"):
        read_raw_archive_content_fingerprint(path, limits=RawArchiveReadLimits())
    with pytest.raises(RuntimeError, match="raw schema differs"):
        validate_raw_archive_parquet(
            path, limits=RawArchiveReadLimits(),
            identity_spool_directory=tmp_path / "identity",
        )


@pytest.mark.parametrize("name", ["max_rows", "max_file_bytes", "max_logical_bytes", "max_row_group_bytes", "batch_rows"])
@pytest.mark.parametrize("value", [0, -1, True, "1000"])
def test_raw_read_limits_reject_implicit_or_nonpositive_bounds(name, value):
    with pytest.raises(ValueError, match="market_archive_read_limit_invalid"):
        RawArchiveReadLimits(**{name: value})
