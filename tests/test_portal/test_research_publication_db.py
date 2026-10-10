from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import uuid

import pytest
from sqlalchemy import select

from portal.backend.db.models import ResearchItemRecord
from portal.backend.db.session import db
from portal.backend.service.research import publication, repository, service

pytestmark = pytest.mark.db


def test_disposable_publication_concurrency_and_workspace_loss(tmp_path):
    token = uuid.uuid4().hex
    question = repository.create_item(kind="study", title=f"Question {token}", payload={"legacy": "retained"})
    claim = repository.create_item(kind="hypothesis", title=f"Claim {token}", body="Initial claim")
    finding = repository.create_item(kind="observation", title=f"Finding {token}", body="Inconclusive manual note")
    repository.create_link(source_item_id=question["id"], target_type="research_item", target_id=claim["id"], relation="tests")
    publication.adopt(question["id"], {"question": "Is the claim supported?", "scope": "Synthetic disposable evidence"})
    request = {"request_id": token, "expected_previous_hash": None, "conclusion": "No conclusion supported.",
               "limitations": "Manual synthetic finding; no statistical inference or actual replay.", "scope": "Disposable",
               "references": [{"item_id": item["id"], "kind": item["kind"], "role": "context",
                               "content_hash": publication.content_identity(item)} for item in (claim, finding)]}
    local = tmp_path / "campaign.json"
    local.write_text(json.dumps(request))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: publication.publish(question["id"], deepcopy(request)), range(2)))
    assert results[0] == results[1]
    first = results[0]
    local.unlink()
    with db.session() as session:
        record = session.get(ResearchItemRecord, claim["id"])
        record.body = "Later corrected claim"
    repository.create_link(source_item_id=question["id"], target_type="research_item", target_id=claim["id"], relation="tests", metadata={"later": True})
    history = publication.history(question["id"])
    assert len(history["publications"]) == 1
    assert history["publications"][0] == first
    assert first["references"][0]["snapshot"]["body"] == "Initial claim"
    assert first["relationships"][0]["metadata"] == {}
    with pytest.raises(ValueError, match="revision_conflict"):
        publication.publish(question["id"], request | {"request_id": token + "stale"})
    with db.session() as session:
        record = session.get(ResearchItemRecord, question["id"])
        assert record.payload["legacy"] == "retained"
        assert record.payload[publication.KEY]["publications"] == [first]
