"""Small, inert, revision-bound report attachments. Never execute report source."""
from __future__ import annotations

from copy import deepcopy
import hashlib
from html import escape
from html.parser import HTMLParser
import json
import logging
import re
from typing import Any

from market_data.frozen import semantic_hash
from . import images, publication, repository
from portal.backend.db.session import db

KEY = "study_reports_v1"
MAX_BYTES = 2 * 1024 * 1024
MAX_REPORTS = 3
SCHEMA = "study_report.v1"
logger = logging.getLogger(__name__)
TAGS = set("article section header footer h1 h2 h3 h4 p div span strong em b i ul ol li blockquote pre code figure figcaption table thead tbody tr th td hr br img a".split())
VOID = {"hr", "br", "img"}
CSS = """html{color-scheme:light}body{margin:0;background:#faf9f6;color:#202c36;font:18px/1.75 Georgia,serif}article{max-width:860px;margin:auto;padding:40px 6% 80px}h1,h2,h3,figcaption,th{font-family:system-ui,sans-serif}h1{font-size:clamp(30px,5vw,48px);line-height:1.15}h2{margin-top:2.5em;font-size:26px;line-height:1.3}h3{font-size:20px}p{margin:1.2em 0}img{max-width:100%;height:auto}figure{margin:2em 0}figcaption{font-size:14px;line-height:1.6;color:#465566}blockquote{border-left:4px solid #17817c;padding:8px 24px;margin:2em 0;background:#edf5f2}table{border-collapse:collapse;width:100%;font:14px/1.6 system-ui,sans-serif;display:block;overflow:auto}td,th{padding:10px;border-bottom:1px solid #d5dcdf;text-align:left}pre,code{font-size:12px;overflow-wrap:anywhere;white-space:pre-wrap}hr{border:0;border-top:1px solid #d5dcdf;margin:2em 0}a{color:#086b68}@media(max-width:480px){body{font-size:16px}article{padding:24px 5%}h2{font-size:23px}}"""


class InertHTML(HTMLParser):
    """Reconstruct an allowlisted fragment, rejecting active/network content."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.stack = []
        self.image_count = 0
        self.image_pixels = 0
        self.element_count = 0

    def handle_starttag(self, tag, attrs):
        self.element_count += 1
        if tag not in TAGS or len(self.stack) >= 64 or self.element_count > 20000:
            raise ValueError("study_report_invalid: unsupported HTML element or depth")
        admitted = []
        seen = set()
        values = dict(attrs)
        for key, value in attrs:
            if key in seen or value is None:
                raise ValueError("study_report_invalid: duplicate or empty attribute")
            seen.add(key)
            if key == "id" and re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,100}", value):
                pass
            elif tag == "a" and key == "href" and re.fullmatch(r"#[A-Za-z][A-Za-z0-9_-]{0,100}", value):
                pass
            elif tag == "img" and key == "alt" and 0 < len(value) <= 2048:
                pass
            elif tag == "img" and key == "src" and value.startswith("data:image/png;base64,"):
                _, metadata = images.decode_image(value.split(",", 1)[1])
                self.image_count += 1
                self.image_pixels += metadata["width"] * metadata["height"]
                if self.image_count > 8 or self.image_pixels > images.MAX_PIXELS:
                    raise ValueError("study_report_invalid: report exceeds eight images / 24 megapixels total")
                if metadata["media_type"] != "image/png":
                    raise ValueError("study_report_invalid: expected PNG")
            elif tag in {"th", "td"} and key in {"colspan", "rowspan"} and re.fullmatch(r"[1-9][0-9]?", value):
                pass
            else:
                raise ValueError("study_report_invalid: unsupported HTML attribute")
            admitted.append(f' {key}="{escape(value, quote=True)}"')
        if tag == "img" and not {"src", "alt"} <= set(values):
            raise ValueError("study_report_invalid: image requires PNG and alt text")
        self.parts.append("<" + tag + "".join(admitted) + ">")
        if tag not in VOID:
            self.stack.append(tag)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        if not self.stack or self.stack.pop() != tag:
            raise ValueError("study_report_invalid: unbalanced HTML")
        self.parts.append(f"</{tag}>")

    def handle_data(self, data):
        self.parts.append(escape(data))

    def handle_decl(self, decl):
        raise ValueError("study_report_invalid: document declarations unsupported")

    def unknown_decl(self, data):
        raise ValueError("study_report_invalid: document declarations unsupported")


def normalize_html(value):
    if not isinstance(value, str) or not 0 < len(value.encode()) <= MAX_BYTES:
        raise ValueError("study_report_invalid: HTML exceeds 2 MiB")
    parser = InertHTML()
    parser.feed(value)
    parser.close()
    if parser.stack:
        raise ValueError("study_report_invalid: incomplete HTML")
    return "".join(parser.parts)


def _binding(item, publication_hash, session):
    if item["kind"] != "study":
        raise ValueError("study_report_invalid: published Study required")
    return images._binding(item, {"publication_hash": publication_hash}, session)


def _summary(row, item, session):
    if row.get("schema_version") != SCHEMA or row.get("report_id") != semantic_hash({k: v for k, v in row.items() if k != "report_id"}):
        raise ValueError("study_report_integrity: report identity")
    if _binding(item, row["binding"]["publication_hash"], session) != row["binding"]:
        raise ValueError("study_report_integrity: retained binding changed")
    latest = item["payload"][publication.KEY]["publications"][-1]["publication_hash"]
    return {k: v for k, v in row.items() if k not in {"html", "source_qmd"}} | {
        "stale": latest != row["binding"]["publication_hash"],
        "content_url": f"/api/research/items/{item['id']}/reports/{row['report_id']}/content"}


def attach(item_id: str, raw: dict[str, Any]):
    publication.exact_fields(raw, {"title", "publication_hash", "html", "source_qmd", "build"})
    title = publication.required(raw.get("title"), "title")
    source = publication.required(raw.get("source_qmd"), "source_qmd")
    if len(title) > 255 or len(source.encode()) > 128 * 1024:
        raise ValueError("study_report_invalid: title/source exceeds limit")
    build = raw.get("build")
    fields = {"quarto_version", "source_revision", "evidence_bundle_sha256"}
    if not isinstance(build, dict) or set(build) != fields or any(not isinstance(build[k], str) or not 0 < len(build[k]) <= 128 for k in fields):
        raise ValueError("study_report_invalid: exact build provenance required")
    if not re.fullmatch(r"[0-9a-f]{40}", build["source_revision"]) or not re.fullmatch(r"[0-9a-f]{64}", build["evidence_bundle_sha256"]):
        raise ValueError("study_report_invalid: invalid build digest")
    html = normalize_html(raw.get("html"))
    with db.session() as session:
        record = publication._locked(item_id, session)
        item = record.to_dict()
        binding = _binding(item, raw.get("publication_hash"), session)
        material = {"schema_version": SCHEMA, "title": title, "binding": binding, "html": html,
                    "source_qmd": source, "build": build, "html_sha256": hashlib.sha256(html.encode()).hexdigest(),
                    "meaning": "derived_presentation_not_new_research_or_certification"}
        identifier = semantic_hash(material)
        rows = deepcopy(item["payload"].get(KEY, []))
        for row in rows:
            if row.get("report_id") == identifier:
                return _summary(row, item, session)
        if len(rows) >= MAX_REPORTS:
            raise ValueError("study_report_limit: three retained reports; existing reports preserved")
        row = material | {"report_id": identifier}
        payload = deepcopy(item["payload"])
        payload[KEY] = [*rows, row]
        if len(json.dumps(payload).encode()) > 7 * 1024 * 1024:
            raise ValueError("study_report_limit: Study payload exceeds 7 MiB")
        record.payload = payload
        session.flush()
        result = _summary(row, item, session)
    logger.info("study_report_attached | item_id=%s report_id=%s publication_hash=%s", item_id, identifier, binding["publication_hash"])
    return result


def list_reports(item_id):
    with db.session() as session:
        item = repository.get_item(item_id, session=session)
        publication._assert_access(item, session)
        return {"items": [_summary(row, item, session) for row in item["payload"].get(KEY, [])]}


def get_content(item_id, report_id, source=False):
    with db.session() as session:
        item = repository.get_item(item_id, session=session)
        for row in item["payload"].get(KEY, []):
            if row.get("report_id") == report_id:
                _summary(row, item, session)
                if source:
                    return json.dumps({k: row[k] for k in ("source_qmd", "build", "binding", "report_id")}, indent=2).encode()
                # Revalidate inertness even after storage; no filesystem resolution.
                content = normalize_html(row["html"])
                return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + escape(row["title"]) + '</title><style>' + CSS + '</style></head><body><article>' + content + '</article></body></html>').encode()
    raise KeyError("Study report not found")
