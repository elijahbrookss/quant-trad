from types import SimpleNamespace
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json

import pytest

from market_data.archive import (
    FilesystemRawArchiveObjectStore, RawArchiveReadLimits, encode_raw_records_to_parquet,
    iter_raw_archive_parquet, raw_archive_content_fingerprint, read_raw_archive_content_fingerprint,
    RAW_ARCHIVE_SCHEMA_VERSION,
)
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from portal.backend.service.storage.repos.fact_lineage import BOOK_SCOPE_FIELDS, resolve_canonical_raw_archive_refs
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
            prefixes = json.loads(params["prefixes"])
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


def test_exact_revisions_check_each_raw_row_and_share_one_object_read(tmp_path):
    store, _, mappings, rows = _fixture(tmp_path)
    refs, verified = _resolve(store, mappings, rows)
    assert list(refs) == ["raw-manifest"]
    assert verified.byte_count == mappings[0]["byte_count"]
    assert len(verified.objects) == 1


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
    original_reader = fact_lineage.read_raw_archive_content_fingerprint
    def changed(*args, **kwargs):
        result = original_reader(*args, **kwargs)
        with path.open("ab") as file:
            file.write(b"changed after identity scan")
        return result
    monkeypatch.setattr(fact_lineage, "read_raw_archive_content_fingerprint", changed)
    with pytest.raises(RuntimeError, match="changed"):
        _resolve(store, mappings, rows)


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


def test_aliases_with_different_raw_bytes_at_one_book_position_are_ambiguous(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path)
    extra = {**mappings[0], "id": "other-manifest", "raw_record_id": "another-raw-id"}
    evidence = {"definition_id": records[0].definition_id, "session_id": records[0].session_id,
                "connection_epoch": 0, "receive_ordinal": 1, "provider_product_id": "BTC-USD"}
    row = {**rows[0], "fact_type": "market.bbo", "provenance": {"_qt_bbo_evidence": {"source_position": evidence}}}
    with pytest.raises(RuntimeError, match="position_ambiguous"):
        _resolve(store, [*mappings, extra], [row])


@pytest.mark.parametrize("limit", ["max_rows", "max_file_bytes", "max_logical_bytes", "max_row_group_bytes"])
def test_streaming_raw_reader_rejects_work_outside_its_budget(tmp_path, limit):
    store, records, _, _ = _fixture(tmp_path)
    path = store.local_path("fixture.parquet")
    assert list(iter_raw_archive_parquet(path, limits=RawArchiveReadLimits(batch_rows=1))) == records
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        list(iter_raw_archive_parquet(path, limits=RawArchiveReadLimits(**{limit: 1})))
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        read_raw_archive_content_fingerprint(path, limits=RawArchiveReadLimits(**{limit: 1}))


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


@pytest.mark.parametrize("name", ["max_rows", "max_file_bytes", "max_logical_bytes", "max_row_group_bytes", "batch_rows"])
@pytest.mark.parametrize("value", [0, -1, True, "1000"])
def test_raw_read_limits_reject_implicit_or_nonpositive_bounds(name, value):
    with pytest.raises(ValueError, match="market_archive_read_limit_invalid"):
        RawArchiveReadLimits(**{name: value})
