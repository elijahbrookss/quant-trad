from copy import deepcopy
import hashlib
from io import BytesIO
import json
from zipfile import ZipFile

import pytest
from portal.backend.service.research import evidence_export as export


@pytest.fixture
def saved(monkeypatch):
    root = {"id": "study", "kind": "study", "payload": {"question_contract": {"publications": [
        {"revision": 1, "publication_hash": "pub-1", "references": [
            {"item_id": "review", "kind": "observation", "content_hash": "review-hash", "snapshot": {"id": "review", "kind": "observation", "payload": {"encoded_review": "retained-original-bytes"}}}]}]}}}
    records = {"study": root, "review": {"id": "review", "kind": "observation", "payload": {"later": "changed"}}}
    monkeypatch.setattr(export, "record_snapshot", lambda identifier: deepcopy(records[identifier]))
    monkeypatch.setattr(export.publication, "verify_history", lambda *args: None)
    monkeypatch.setattr(export.publication, "content_identity", lambda item: "review-hash")
    monkeypatch.setattr(export.publication, "_assert_access", lambda *args: None)
    monkeypatch.setattr(export.images, "list_images", lambda identifier: {"items": []})
    from contextlib import contextmanager
    @contextmanager
    def session():
        yield object()
    monkeypatch.setattr(export.browse, "read_session", session)
    return root, records


def test_bundle_pins_historical_references_checksums_and_missing_raw_inputs(saved):
    before = deepcopy(saved)
    review = export.preflight("study", 1, "pub-1")
    assert review["ready"] and not review["raw_input_recomputation"]
    assert review["verification_level"] == "saved_record_verification_only"
    with ZipFile(BytesIO(export.bundle("study", 1, "pub-1"))) as archive:
        assert json.loads(archive.read("records/reference-001.json"))["payload"] == {"encoded_review": "retained-original-bytes"}
        assert json.loads(archive.read("records/selected-publication.json"))["publication_hash"] == "pub-1"
        for line in archive.read("checksums.sha256").decode().splitlines():
            digest, path = line.split("  ", 1)
            assert hashlib.sha256(archive.read(path)).hexdigest() == digest
        assert review["file_count"] == len(archive.namelist())
        assert review["uncompressed_bytes"] == sum(info.file_size for info in archive.infolist())
        assert "OHLCV" in archive.read("README.txt").decode()
        assert "Raw-input recomputation is unavailable" in archive.read("README.txt").decode()
        assert not any(path.startswith("/") or ".." in path for path in archive.namelist())
    assert saved == before


def test_changed_revision_missing_snapshot_and_byte_limits_fail_closed(saved, monkeypatch):
    with pytest.raises(ValueError, match="revision_changed"):
        export.bundle("study", 1, "new-hash")
    monkeypatch.setattr(export, "MAX_BUNDLE_BYTES", 5)
    with pytest.raises(ValueError, match="limit"):
        export.preflight("study", 1, "pub-1")
    monkeypatch.setattr(export, "MAX_BUNDLE_BYTES", 32 * 1024 * 1024)
    saved[0]["payload"]["question_contract"]["publications"][0]["references"][0].pop("snapshot")
    with pytest.raises(ValueError, match="snapshot missing"):
        export.bundle("study", 1, "pub-1")


def test_concurrent_export_is_bounded_and_slot_recovers(saved):
    with export.export_slot():
        with pytest.raises(ValueError, match="export_busy"):
            export.preflight("study", 1, "pub-1")
    assert export.preflight("study", 1, "pub-1")["ready"]


def test_referenced_study_images_exclude_later_publication_hashes(saved, monkeypatch):
    root, records = saved
    ref = root["payload"]["question_contract"]["publications"][0]["references"][0]
    ref["kind"] = "study"
    records["review"]["kind"] = "study"
    ref["snapshot"]["kind"] = "study"
    ref["snapshot"]["payload"]["question_contract"] = {"publications": [{"publication_hash": "historical"}]}
    monkeypatch.setattr(export.images, "list_images", lambda identifier: {"items": [{"image_id": "later-image", "binding": {"kind": "study", "publication_hash": "later"}}]} if identifier == "review" else {"items": []})
    monkeypatch.setattr(export.images, "get_content", lambda *args: pytest.fail("Later image bytes must not be read"))
    assert export.preflight("study", 1, "pub-1")["ready"]


def test_metadata_is_included_in_byte_and_file_limits(saved, monkeypatch):
    with pytest.raises(ValueError, match="metadata"):
        export._finalize({"payload.json": b"{}"}, {"exclusions": ["x" * export.MAX_BUNDLE_BYTES]})


def test_direct_check_export_uses_same_typed_integrity_validation(saved, monkeypatch):
    from portal.backend.service.research import service
    root, records = saved
    records["check"] = {"id": "check", "kind": "research_check", "payload": {"tampered": True}}
    def denied(payload):
        assert payload == {"tampered": True}
        raise ValueError("check_evidence_payload_invalid: hash mismatch")
    monkeypatch.setattr(service, "_validate_research_check_evidence_payload", denied)
    with pytest.raises(ValueError, match="hash mismatch"):
        export.preflight("check")


def test_comparison_uses_existing_math_with_bounded_access_checked_records(saved, monkeypatch):
    from portal.backend.service.research import service
    root, records = saved
    records["a"] = {"id": "a", "kind": "research_check", "payload": {}}
    records["b"] = {"id": "b", "kind": "research_check", "payload": {}}
    calls = []
    monkeypatch.setattr(service, "compare_research_check_records", lambda left, right: calls.append((left["id"], right["id"])) or {"left": {"id": left["id"]}, "right": {"id": right["id"]}, "deltas": {}})
    assert export.comparison("a", "b")["left"]["id"] == "a"
    assert calls == [("a", "b")]
    with pytest.raises(ValueError, match="distinct"):
        export.comparison("a", "a")
    monkeypatch.setattr(service, "compare_research_check_records", lambda *args: {"unbounded": "x" * 300000})
    with pytest.raises(ValueError, match="256 KiB"):
        export.comparison("a", "b")


def test_comparison_math_and_context_use_same_admitted_snapshots_without_rereads(saved, monkeypatch):
    from portal.backend.service.research import service
    _, records = saved
    records["a"] = {"id": "a", "kind": "research_check", "payload": {"result": {"check_family": "synthetic", "sample_count": 2, "result_hash": "a-hash"}}}
    records["b"] = {"id": "b", "kind": "research_check", "payload": {"result": {"check_family": "synthetic", "sample_count": 4, "result_hash": "b-hash"}}}
    monkeypatch.setattr(service.repository, "get_item", lambda *args: pytest.fail("Comparison must not reread admitted records"))
    monkeypatch.setattr(service, "_check_result", lambda item: item["payload"]["result"])
    result = export.comparison("a", "b")
    assert result["deltas"]["sample_count"]["delta"] == 2
    assert result["record_context"]["left"]["result_hash"] == "a-hash"
    assert result["record_context"]["right"]["result_hash"] == "b-hash"
