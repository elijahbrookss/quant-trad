"""Read-only saved-record export; never replay, acquire raw inputs or mutate custody."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
from io import BytesIO
import json
from threading import BoundedSemaphore
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from sqlalchemy import cast, func, select, String

from portal.backend.db.models import ResearchItemRecord
from . import browse, images, publication, repository

MAX_RECORD_BYTES = 8 * 1024 * 1024
MAX_BUNDLE_BYTES = 32 * 1024 * 1024
MAX_FILES = 160
_EXPORT_SLOT = BoundedSemaphore(1)


@contextmanager
def export_slot():
    if not _EXPORT_SLOT.acquire(blocking=False):
        raise ValueError("research_export_busy: another saved-record export is being prepared; retry later")
    try:
        yield
    finally:
        _EXPORT_SLOT.release()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def record_snapshot(item_id: str):
    with browse.read_session() as session:
        # Measure the complete logical row on PostgreSQL before fetching it:
        # legacy body/tags can be large even when payload alone is small.
        size = session.scalar(select(func.octet_length(cast(
            func.row_to_json(ResearchItemRecord.__table__.table_valued()), String
        ))).where(ResearchItemRecord.id == item_id))
        if size is None:
            raise KeyError("Research item not found")
        if size > MAX_RECORD_BYTES:
            raise ValueError("research_export_limit: exact record exceeds 8 MiB; use the existing owned evidence workflow")
        item = repository.get_item(item_id, session=session)
        publication._assert_access(item, session)
    if len(json_bytes(item)) > MAX_RECORD_BYTES:
        raise ValueError("research_export_limit: serialized record exceeds 8 MiB")
    return item


def _collect(item_id: str, revision: int | None, expected_publication_hash: str | None):
    root = record_snapshot(item_id)
    files: dict[str, bytes] = {}
    origins: dict[str, dict[str, Any]] = {}

    def add(path, content, origin):
        if path in files or len(files) >= MAX_FILES - 3 or sum(map(len, files.values())) + len(content) > MAX_BUNDLE_BYTES:
            raise ValueError("research_export_limit: bundle exceeds 160 files / 32 MiB; no partial archive returned")
        files[path] = content
        origins[path] = origin

    root_origin = {"item_id": item_id, "meaning": "current_record_snapshot_at_export"}
    if root["kind"] == "research_check":
        from .service import _validate_research_check_evidence_payload
        definition, request, plan, evidence, result = _validate_research_check_evidence_payload(root["payload"])
        root_origin.update({"definition_hash": definition.definition_hash, "request_hash": request.request_hash,
                            "plan_hash": plan.plan_hash, "evidence_hash": evidence.evidence_hash, "result_hash": result.result_hash})
    add("records/root-current.json", json_bytes(root), root_origin)
    contract = root.get("payload", {}).get(publication.KEY)
    selected = None
    references = []
    if contract:
        publication.verify_history(contract, item_id)
        pubs = contract.get("publications", [])
        selected = next((p for p in pubs if p["revision"] == revision), None) if revision is not None else (pubs[-1] if pubs else None)
        if revision is not None and selected is None:
            raise KeyError("Study interpretation revision not found")
    if expected_publication_hash and (selected or {}).get("publication_hash") != expected_publication_hash:
        raise ValueError("research_export_revision_changed: selected publication does not match the reviewed hash")
    if selected:
        references = selected["references"]
        if len(references) > 100:
            raise ValueError("research_export_limit: selected publication exceeds 100 references")
        add("records/selected-publication.json", json_bytes(selected), {"item_id": item_id, "publication_hash": selected["publication_hash"], "revision": selected["revision"]})
    elif revision is not None:
        raise KeyError("Study has no interpretation revision")
    emitted = {item_id}
    source_items = [(item_id, {selected["publication_hash"]} if selected else set())]
    for ref in references:
        identifier = ref["item_id"]
        # Access is always rechecked on the current record, even for a retained
        # reasoning snapshot. Snapshot hashes preserve historical reasoning.
        current = record_snapshot(identifier)
        if current["kind"] != ref["kind"]:
            raise ValueError("research_export_integrity: referenced kind changed")
        if ref["kind"] == "research_check":
            from .service import _validate_research_check_evidence_payload
            _, _, _, evidence, result = _validate_research_check_evidence_payload(current["payload"])
            if evidence.evidence_hash != ref.get("evidence_hash") or result.result_hash != ref.get("result_hash"):
                raise ValueError("research_export_integrity: referenced Check hashes changed")
            record = current
        else:
            record = ref.get("snapshot")
            if not isinstance(record, dict) or publication.content_identity(record) != ref.get("content_hash"):
                raise ValueError("research_export_integrity: retained reasoning snapshot missing or changed")
            with browse.read_session() as session:
                publication._assert_access(record, session)
        if identifier not in emitted:
            # Paths are generated numeric slots, never caller-supplied names.
            add(f"records/reference-{len(emitted):03d}.json", json_bytes(record), {key: ref[key] for key in ("item_id", "kind", "content_hash", "result_hash", "evidence_hash") if key in ref})
            emitted.add(identifier)
            source_items.append((identifier, {p["publication_hash"] for p in record.get("payload", {}).get(publication.KEY, {}).get("publications", [])}))
    exclusions = [
        "No separately fetched raw source data or OHLCV files are included. Raw-input recomputation is unavailable from this bundle.",
        "Exact saved record payloads, including embedded content, are preserved. Their raw-input completeness and redistribution rights are not established by export.",
        "Unreferenced records and current mutable relationship expansions are excluded.",
        "Images are current retained attachments bound to included interpretation hashes or exact Check evidence; later interpretation images are excluded.",
        "Encoded report/review files remain inside their original record JSON; no untyped payload is extracted or executed.",
        "No provider acquisition, Check replay, source-data redistribution or scientific certification was performed.",
    ]
    for source_id, retained_hashes in source_items:
        # Existing image service revalidates identity, access and exact binding.
        for image in images.list_images(source_id)["items"]:
            if image["binding"].get("kind") == "study" and image["binding"].get("publication_hash") not in retained_hashes:
                continue
            # Referenced reasoning records do not own image custody today.
            content, manifest = images.get_content(source_id, image["image_id"])
            extension = "png" if manifest["media_type"] == "image/png" else "jpg"
            path = f"visuals/{len(files):03d}.{extension}"
            add(path, content, {"item_id": source_id, "image_id": image["image_id"], "binding": manifest["binding"]})
            add(path + ".json", json_bytes(manifest), {"item_id": source_id, "image_id": image["image_id"]})
    manifest = {"schema_version": "research_saved_evidence_bundle.v1", "item_id": item_id,
        "selected_revision": (selected or {}).get("revision"), "publication_hash": (selected or {}).get("publication_hash"),
        "verification_level": "saved_record_verification_only", "raw_input_recomputation": False,
        "source_data_scope": "no_separate_source_files_exact_record_payloads_preserved", "exclusions": exclusions,
        "files": [{"path": path, "byte_count": len(content), "sha256": hashlib.sha256(content).hexdigest(),
                   "origin": origins[path]} for path, content in files.items()]}
    return files, manifest


def _finalize(files, manifest):
    files = dict(files)
    manifest = {**manifest, "metadata_files": ["manifest.json", "README.txt", "checksums.sha256"],
                "checksum_scope": "all_exported_files_except_checksums_sha256_itself"}
    files["manifest.json"] = json_bytes(manifest)
    files["README.txt"] = ("Saved research evidence\n\n"
        "Verify checksums.sha256 with sha256sum -c checksums.sha256.\n"
        "Compare exact record/reference hashes and selected publication before arithmetic review.\n"
        "records/root-current.json is a current snapshot; selected-publication.json pins the chosen historical interpretation.\n"
        "Reference reasoning snapshots are historical; Check definitions/requests/plans/bindings/outputs remain in their exact record payload.\n"
        "Manifest file entries describe evidence payloads; metadata_files names the three archive metadata files. Checksums cover both groups except the checksum list itself.\n"
        "A matching checksum proves bytes, not scientific correctness. No replay was run.\n\n"
        + "\n".join(manifest["exclusions"]) + "\n").encode()
    files["checksums.sha256"] = "".join(hashlib.sha256(content).hexdigest() + "  " + path + "\n" for path, content in files.items()).encode()
    if len(files) > MAX_FILES or sum(map(len, files.values())) > MAX_BUNDLE_BYTES:
        raise ValueError("research_export_limit: archive including metadata exceeds 160 files / 32 MiB")
    return files, manifest


def preflight(item_id, revision=None, expected_publication_hash=None):
    with export_slot():
        files, manifest = _finalize(*_collect(item_id, revision, expected_publication_hash))
    return {**manifest, "uncompressed_bytes": sum(map(len, files.values())), "file_count": len(files),
            "limits": {"files": MAX_FILES, "bytes": MAX_BUNDLE_BYTES}, "ready": True}


def bundle(item_id, revision=None, expected_publication_hash=None):
    with export_slot():
        files, manifest = _finalize(*_collect(item_id, revision, expected_publication_hash))
        stream = BytesIO()
        with ZipFile(stream, "w", ZIP_DEFLATED) as archive:
            for path, content in files.items():
                archive.writestr(path, content)
        return stream.getvalue()


def comparison(left, right):
    if left == right:
        raise ValueError("research_compare_invalid: choose two distinct Checks")
    records = [record_snapshot(identifier) for identifier in (left, right)]
    for record in records:
        if record["kind"] != "research_check":
            raise ValueError("research_compare_invalid: expected two research Checks")
    from .service import compare_research_check_records
    result = compare_research_check_records(*records)
    context = {}
    for side, record in zip(("left", "right"), records):
        payload = record.get("payload", {})
        evidence = payload.get("evidence", {})
        binding = evidence.get("input_binding", {})
        context[side] = {**{key: record.get(key) for key in ("id", "symbol", "timeframe", "window_start", "window_end", "source_revision")},
            "definition_hash": payload.get("definition", {}).get("definition_hash"),
            "result_hash": payload.get("result", {}).get("result_hash"), "evidence_hash": evidence.get("evidence_hash"),
            "input_binding": {key: binding.get(key) for key in ("dataset_id", "dataset_hash", "binding_hash", "run_id", "report_semantic_fingerprint")}}
    result = {**result, "record_context": context, "scope_meaning": "current_saved_Check_records_not_independent_replications"}
    if len(json_bytes(result)) > 256 * 1024:
        raise ValueError("research_compare_limit: comparison exceeds 256 KiB; inspect exact records separately")
    return result
