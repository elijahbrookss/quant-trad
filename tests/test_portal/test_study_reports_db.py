"""JSON attachment transaction proof on disposable PostgreSQL only."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import uuid

import pytest

from portal.backend.db.models import ResearchItemRecord
from portal.backend.db.session import db
from portal.backend.service.research import publication, reports, repository

pytestmark = pytest.mark.db


def test_report_is_append_only_revision_bound_and_survives_reread():
    study = repository.create_item(kind="study", title="Disposable report " + uuid.uuid4().hex, body="Original Study")
    claim = repository.create_item(kind="hypothesis", title="Synthetic claim")
    try:
        publication.adopt(study["id"], {"question": "What is retained?", "scope": "Synthetic"})
        revision = publication.publish(study["id"], {"request_id": "first", "expected_previous_hash": None,
            "conclusion": "No conclusion changed", "scope": "Synthetic", "limitations": "Not research",
            "references": [{"item_id": claim["id"], "kind": "hypothesis", "role": "context", "content_hash": publication.content_identity(claim)}]})
        before = repository.get_item(study["id"])
        request = {"title": "Inert report", "publication_hash": revision["publication_hash"],
            "html": "<h1>Retained finding</h1><p>No conclusion changed.</p>", "source_qmd": "# Retained finding",
            "build": {"quarto_version": "1.10.19", "source_revision": "a" * 40, "evidence_bundle_sha256": "b" * 64}}
        with ThreadPoolExecutor(max_workers=2) as pool:
            copies = list(pool.map(lambda _: reports.attach(study["id"], request), range(2)))
        assert copies[0] == copies[1]
        after = repository.get_item(study["id"])
        expected = deepcopy(after)
        expected["payload"].pop(reports.KEY)
        assert expected == before
        assert len(after["payload"][reports.KEY]) == 1
        assert b"Retained finding" in reports.get_content(study["id"], copies[0]["report_id"])
        assert reports.list_reports(study["id"])["items"] == [copies[0]]
        second = publication.publish(study["id"], {"request_id": "second", "expected_previous_hash": revision["publication_hash"],
            "conclusion": "Later interpretation", "scope": "Synthetic", "limitations": "Not research",
            "references": [{"item_id": claim["id"], "kind": "hypothesis", "role": "context", "content_hash": publication.content_identity(claim)}]})
        assert second["revision"] == 2
        assert reports.list_reports(study["id"])["items"][0]["stale"]
        with db.session() as session:
            record = session.get(ResearchItemRecord, study["id"])
            payload = deepcopy(record.payload)
            payload[reports.KEY][0]["html"] = "<p>changed</p>"
            record.payload = payload
        with pytest.raises(ValueError, match="integrity"):
            reports.get_content(study["id"], copies[0]["report_id"])
    finally:
        with db.session() as session:
            for identifier in (study["id"], claim["id"]):
                session.delete(session.get(ResearchItemRecord, identifier))
