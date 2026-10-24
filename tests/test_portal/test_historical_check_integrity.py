"""Retired records are inspectable snapshots, never special runtime authorities."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from market_data.frozen import semantic_hash
from portal.backend.service.research import service, publication, images


@pytest.fixture
def retired_item():
    payload = json.loads((Path(__file__).parent / "fixtures/historical_check_v2_synthetic.json").read_text())
    return {"id": "synthetic-retired-check", "kind": "research_check", "status": "tested",
            "body": "Retained synthetic result", "payload": payload,
            "stored_evidence_integrity": "verified", "execution_available": True,
            "replayable": True, "observation_eligible": True}


def test_retired_record_is_readable_without_current_verification_or_mutation(retired_item, monkeypatch):
    before = deepcopy(retired_item)
    monkeypatch.setattr(service.repository, "get_item", lambda *a, **k: retired_item)
    monkeypatch.setattr(service.CHECK_REGISTRY, "resolve_evaluator", lambda *a, **k: pytest.fail("No retired evaluator"))
    projected = service.get_research_item(retired_item["id"])
    assert projected["payload"] == before["payload"] and projected["body"] == before["body"]
    assert projected["evidence_classification"] == "legacy_frozen_unverifiable"
    assert projected["execution_available"] is False
    assert projected["replayable"] is False and projected["observation_eligible"] is False
    assert "stored_evidence_integrity" not in projected
    assert "unavailable" in projected["evidence_integrity_error"]
    assert retired_item == before


def test_copied_or_rehashed_retired_record_never_gains_admission(retired_item):
    retired_item["id"] = "synthetic-copy"
    retired_item["payload"]["result"]["result"]["changed"] = True
    projected = service._project_evidence_classification(retired_item)
    assert projected["replayable"] is False and projected["observation_eligible"] is False
    with pytest.raises(ValueError):
        service._validate_research_check_evidence_payload(retired_item["payload"])


def test_retired_replay_and_new_observation_fail_before_execution_or_write(retired_item, monkeypatch):
    monkeypatch.setattr(service.repository, "get_item", lambda *a, **k: retired_item)
    monkeypatch.setattr(service.repository, "create_item", lambda **k: pytest.fail("No new Observation"))
    monkeypatch.setattr(service.repository, "create_link", lambda **k: pytest.fail("No new link"))
    monkeypatch.setattr(service, "execute_check_evidence", lambda *a, **k: pytest.fail("No retired replay"))
    outcome = service.replay_research_check(retired_item["id"])
    assert outcome["status"] == "not_replayable" and outcome["provider_call_performed"] is False
    assert "unavailable" in outcome["reasons"][0]
    with pytest.raises(ValueError, match="not durable replayable"):
        service.create_observation_from_check_evidence(retired_item["id"], {"title": "Do not admit"})


def test_existing_observation_and_study_links_remain_readable(retired_item, monkeypatch):
    records = {retired_item["id"]: retired_item,
        "observation": {"id": "observation", "kind": "observation", "status": "active", "payload": {"source_check": retired_item["id"]}},
        "study": {"id": "study", "kind": "study", "status": "active", "payload": {"saved_interpretation": "unchanged"}}}
    links = [{"id": "check-observation", "source_item_id": retired_item["id"], "target_type": "research_item", "target_id": "observation"},
             {"id": "study-observation", "source_item_id": "study", "target_type": "research_item", "target_id": "observation"}]
    before = deepcopy((records, links))
    monkeypatch.setattr(service.repository, "get_item", lambda identifier, **k: records[identifier])
    monkeypatch.setattr(service.repository, "list_links", lambda identifier, **k: [link for link in links if identifier in (link["source_item_id"], link["target_id"])])
    monkeypatch.setattr(service.repository, "create_item", lambda **k: pytest.fail("Reads cannot write"))
    trail = service.get_research_trail("observation")
    assert trail["item"]["payload"] == records["observation"]["payload"]
    assert {item["id"] for item in trail["related_items"]} == {"study", retired_item["id"]}
    assert {link["id"] for link in trail["links"]} == {link["id"] for link in links}
    assert trail["checks"][0]["replayable"] is False
    assert (records, links) == before


def test_existing_study_publication_reads_without_readmitting_retired_reference(retired_item, monkeypatch):
    question = {"question": "Synthetic question", "scope": "Synthetic scope"}
    revision = {"question_id": "study", "question": question, "revision": 1,
        "previous_hash": None, "request_id": "synthetic-publication", "references": [
            {"item_id": retired_item["id"], "kind": "research_check", "role": "supports", "evidence_classification": "historical_frozen_verified"}]}
    revision["publication_hash"] = semantic_hash(revision)
    contract = {"schema_version": publication.SCHEMA, "question": question, "publications": [revision]}
    study = {"id": "study", "kind": "study", "payload": {publication.KEY: contract}}
    before = deepcopy(study)
    @contextmanager
    def session():
        yield object()
    monkeypatch.setattr(publication.db, "session", session)
    monkeypatch.setattr(publication.repository, "get_item", lambda *a, **k: study)
    monkeypatch.setattr(publication, "_assert_access", lambda *a: None)
    monkeypatch.setattr(service, "_validate_research_check_evidence_payload", lambda *a, **k: pytest.fail("Historical publication reads cannot readmit Checks"))
    assert publication.history("study")["publications"] == [revision]
    assert service.get_research_item("study")["payload"] == study["payload"]
    assert study == before


@pytest.mark.parametrize("version", ["12", "14"])
def test_retired_selection_cannot_feed_new_measurement(version):
    from portal.backend.service.research.saved_selection_binding import load_saved_selection
    frozen = json.loads((Path(__file__).parent / f"fixtures/historical_saved_selection_v{version}.json").read_text())
    item = frozen["item"]
    before = deepcopy(item)
    with pytest.raises(ValueError, match="unavailable|eligible"):
        load_saved_selection(frozen["reference"], frozen["context"], item_loader=lambda _: item)
    assert item == before


def test_retired_check_cannot_gain_new_publication_or_image_binding(retired_item, monkeypatch):
    monkeypatch.setattr(publication, "_assert_access", lambda *a: None)
    monkeypatch.setattr(publication.repository, "get_item", lambda *a, **k: retired_item)
    with pytest.raises(ValueError):
        publication.resolve_reference({"item_id": retired_item["id"], "kind": "research_check", "role": "supports"}, object())
    with pytest.raises(ValueError):
        images._binding(retired_item, {}, object())


def test_existing_retired_image_binding_is_readable_but_not_new_admission(retired_item, monkeypatch):
    monkeypatch.setattr(publication, "_assert_access", lambda *a: None)
    payload = retired_item["payload"]; evidence = payload["evidence"]
    binding = {"kind": "research_check", "item_id": retired_item["id"],
        "definition_hash": payload["definition"]["definition_hash"], "request_hash": payload["request"]["request_hash"],
        "plan_hash": payload["plan"]["plan_hash"], "result_hash": payload["result"]["result_hash"],
        "evidence_hash": evidence["evidence_hash"], "input_hash": evidence["input_hash"], "input_binding": deepcopy(evidence["input_binding"])}
    manifest = {"schema_version": images.SCHEMA, "item_id": retired_item["id"], "binding": binding}
    identity = semantic_hash(manifest); manifest["image_id"] = identity
    record = SimpleNamespace(id=identity, manifest=manifest, created_at=datetime(2026, 1, 1))
    before = deepcopy((retired_item, manifest))
    assert images._summary(record, retired_item, object())["binding"] == binding
    assert (retired_item, manifest) == before
    payload["evidence"]["input_hash"] = "changed"
    with pytest.raises(ValueError, match="binding_mismatch"):
        images._summary(record, retired_item, object())
