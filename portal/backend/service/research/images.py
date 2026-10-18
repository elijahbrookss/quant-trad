"""Bounded static image custody on the existing PostgreSQL persistence boundary.

Images explain retained evidence; uploading never executes or revises research.
"""
from __future__ import annotations

import base64
import binascii
from copy import deepcopy
import hashlib
from io import BytesIO
import logging
import warnings
from typing import Any, Mapping

from PIL import Image, UnidentifiedImageError
from sqlalchemy import select
from sqlalchemy.orm import defer

from market_data.frozen import semantic_hash
from portal.backend.db.models import ResearchImageRecord, ResearchItemRecord
from portal.backend.db.session import db
from . import publication, repository

SCHEMA = "research_image.v1"
MAX_BYTES = 4 * 1024 * 1024
MAX_ENCODED_BYTES = 4 * ((MAX_BYTES + 2) // 3)
MAX_PIXELS = 24_000_000
MAX_IMAGES = 16
MAX_ITEM_BYTES = 32 * 1024 * 1024
logger = logging.getLogger(__name__)


def decode_image(encoded: str) -> tuple[bytes, dict[str, Any]]:
    if not isinstance(encoded, str) or len(encoded) > MAX_ENCODED_BYTES:
        raise ValueError("research_image_invalid: encoded image exceeds 4 MiB")
    try:
        content = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("research_image_invalid: expected strict base64 image bytes") from exc
    if not 0 < len(content) <= MAX_BYTES:
        raise ValueError("research_image_invalid: image must contain 1..4194304 bytes")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(content)) as image:
                mime = {"PNG": "image/png", "JPEG": "image/jpeg"}.get(image.format)
                width, height = image.size
                if mime is None or getattr(image, "n_frames", 1) != 1:
                    raise ValueError("research_image_invalid: only static PNG/JPEG images are supported")
                if width * height > MAX_PIXELS or max(width, height) > 12000:
                    raise ValueError("research_image_invalid: image exceeds 24 megapixels or 12000 pixels per side")
                image.verify()
            # verify checks container structure; load also checks compressed pixels.
            with Image.open(BytesIO(content)) as image:
                image.load()
    except (UnidentifiedImageError, OSError, SyntaxError, Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
        raise ValueError("research_image_invalid: corrupt or oversized image") from exc
    return content, {"media_type": mime, "width": width, "height": height,
                     "byte_count": len(content), "sha256": hashlib.sha256(content).hexdigest()}


def _binding(item: Mapping[str, Any], expected: Mapping[str, Any], session: Any) -> dict[str, Any]:
    publication._assert_access(item, session)
    if item["kind"] == "research_check":
        from .service import _validate_research_check_evidence_payload
        definition, request, plan, evidence, result = _validate_research_check_evidence_payload(item["payload"])
        if set(expected) != {"result_hash", "evidence_hash"} or any(
            expected.get(key) != actual for key, actual in (
                ("result_hash", result.result_hash), ("evidence_hash", evidence.evidence_hash)
            )
        ):
            raise ValueError("research_image_binding_mismatch: exact Check result_hash/evidence_hash required")
        return {"kind": "research_check", "item_id": item["id"],
                "definition_hash": definition.definition_hash, "request_hash": request.request_hash,
                "plan_hash": plan.plan_hash, "result_hash": result.result_hash,
                "evidence_hash": evidence.evidence_hash, "input_hash": evidence.input_hash,
                "input_binding": deepcopy(dict(evidence.input_binding))}
    if item["kind"] != "study":
        raise ValueError("research_image_invalid: target must be a durable Check or published Study")
    if set(expected) != {"publication_hash"} or not expected.get("publication_hash"):
        raise ValueError("research_image_binding_mismatch: exact Study publication_hash required")
    contract = item.get("payload", {}).get(publication.KEY, {})
    publication.verify_history(contract, item["id"])
    revision = next((row for row in contract["publications"]
                     if row["publication_hash"] == expected["publication_hash"]), None)
    if revision is None:
        raise ValueError("research_image_binding_mismatch: Study publication not found")
    publication._assert_access(revision, session)
    # Recheck custody of referenced items without re-resolving historical reasoning.
    for reference in revision["references"]:
        publication._assert_access(repository.get_item(reference["item_id"], session=session), session)
    identities = [{key: row[key] for key in (
        "item_id", "kind", "role", "content_hash", "result_hash", "evidence_hash",
        "dataset_id", "dataset_hash", "binding_hash", "definition_hash", "request_hash", "plan_hash"
    ) if key in row} for row in revision["references"]]
    return {"kind": "study", "item_id": item["id"], "publication_hash": revision["publication_hash"],
            "revision": revision["revision"], "references": identities}


def _expected(binding: Mapping[str, Any]) -> dict[str, Any]:
    fields = ("result_hash", "evidence_hash") if binding["kind"] == "research_check" else ("publication_hash",)
    return {key: binding[key] for key in fields}


def _summary(record: ResearchImageRecord, item: Mapping[str, Any], session: Any) -> dict[str, Any]:
    manifest = deepcopy(record.manifest)
    if manifest.get("schema_version") != SCHEMA or manifest.get("item_id") != item["id"]:
        raise ValueError("research_image_integrity_error: manifest identity")
    material = {key: value for key, value in manifest.items() if key != "image_id"}
    if semantic_hash(material) != record.id or manifest.get("image_id") != record.id:
        raise ValueError("research_image_integrity_error: manifest hash")
    if _binding(item, _expected(manifest["binding"]), session) != manifest["binding"]:
        raise ValueError("research_image_binding_mismatch: retained evidence changed")
    return {**manifest, "created_at": record.created_at.isoformat() + "Z",
            "content_url": f"/api/research/items/{item['id']}/images/{record.id}/content"}


def attach(item_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    publication.exact_fields(raw, {"title", "explanation", "alt_text", "binding", "content_base64"})
    text = {key: publication.required(raw.get(key), key) for key in ("title", "explanation", "alt_text")}
    if len(text["title"]) > 255 or len(text["explanation"]) > 8192 or len(text["alt_text"]) > 2048:
        raise ValueError("research_image_invalid: title/explanation/alt_text exceeds limit")
    if not isinstance(raw.get("binding"), Mapping):
        raise ValueError("research_image_invalid: binding must be an object")
    content, image = decode_image(raw.get("content_base64"))
    with db.session() as session:
        # The existing item row serializes image admission and Study publication.
        record = session.scalar(select(ResearchItemRecord).where(ResearchItemRecord.id == item_id).with_for_update())
        if record is None:
            raise KeyError("Research item not found")
        item = record.to_dict()
        binding = _binding(item, raw["binding"], session)
        manifest = {"schema_version": SCHEMA, "item_id": item_id, **text, **image, "binding": binding,
                    "meaning": "presentation_only_not_scientific_execution_or_certification"}
        image_id = semantic_hash(manifest)
        manifest["image_id"] = image_id
        prior = session.get(ResearchImageRecord, image_id)
        if prior is not None:
            if bytes(prior.content) != content:
                raise ValueError("research_image_integrity_error: stored image bytes")
            return _summary(prior, item, session)
        existing = session.scalars(select(ResearchImageRecord).options(defer(ResearchImageRecord.content))
                                   .where(ResearchImageRecord.item_id == item_id)).all()
        if len(existing) >= MAX_IMAGES or sum(row.manifest["byte_count"] for row in existing) + len(content) > MAX_ITEM_BYTES:
            raise ValueError("research_image_limit: 16 images / 32 MiB per item; existing images preserved")
        saved = ResearchImageRecord(id=image_id, item_id=item_id, manifest=manifest,
                                    content=content, created_at=repository.utcnow())
        session.add(saved)
        session.flush()
        result = _summary(saved, item, session)
    logger.info("research_image_attached | item_id=%s image_id=%s bytes=%s", item_id, image_id, len(content))
    return result


def list_images(item_id: str) -> dict[str, Any]:
    with db.session() as session:
        item = repository.get_item(item_id, session=session)
        publication._assert_access(item, session)
        records = session.scalars(select(ResearchImageRecord).options(defer(ResearchImageRecord.content))
                                  .where(ResearchImageRecord.item_id == item_id)
                                  .order_by(ResearchImageRecord.created_at, ResearchImageRecord.id)).all()
        return {"schema_version": "research_image_list.v1", "item_id": item_id,
                "items": [_summary(row, item, session) for row in records]}


def get_content(item_id: str, image_id: str) -> tuple[bytes, dict[str, Any]]:
    with db.session() as session:
        item = repository.get_item(item_id, session=session)
        record = session.get(ResearchImageRecord, image_id)
        if record is None or record.item_id != item_id:
            raise KeyError("Research image not found")
        summary = _summary(record, item, session)
        content = bytes(record.content)
        if len(content) != summary["byte_count"] or hashlib.sha256(content).hexdigest() != summary["sha256"]:
            raise ValueError("research_image_integrity_error: stored image bytes")
        return content, summary
