from copy import deepcopy
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from portal.backend.service.research import publication, service


class Session:
    def __init__(self, record):
        self.record = record
        self.protocols = []
        self.links = []
        self.flushed = 0

    def scalar(self, statement):
        return self.record

    def scalars(self, statement):
        model = statement.column_descriptions[0]["entity"]
        rows = self.protocols if model is publication.ResearchProtocolRecord else self.links
        return SimpleNamespace(all=lambda: rows)

    def flush(self):
        self.flushed += 1


@pytest.fixture
def memory(monkeypatch):
    record = SimpleNamespace(id="question", kind="study", payload={"prior": {"decision": "preserved"}}, updated_at=None)
    record.to_dict = lambda: {"id": record.id, "kind": record.kind, "payload": deepcopy(record.payload)}
    session = Session(record)

    # This fixture simulates rollback; SQL atomicity belongs to the disposable DB case.
    @contextmanager
    def scope():
        before = deepcopy(record.payload)
        try:
            yield session
        except Exception:
            record.payload = before
            raise

    monkeypatch.setattr(publication.db, "session", scope)
    items = {"claim": {"id": "claim", "kind": "hypothesis", "title": "Claim", "status": "draft", "body": "Initial claim", "payload": {}}}
    monkeypatch.setattr(publication.repository, "get_item", lambda item_id, **kwargs: deepcopy(items[item_id]) if item_id in items else record.to_dict())
    return record, session, items


def request(items):
    return {"request_id": "publish-1", "expected_previous_hash": None,
            "conclusion": "Inconclusive; the claim remains unsupported.",
            "limitations": "Insufficient outcomes; descriptive only.", "scope": "Development data only",
            "references": [{"item_id": "claim", "kind": "hypothesis", "role": "context", "content_hash": publication.content_identity(items["claim"])}]}


def test_adoption_preserves_legacy_and_is_idempotent(memory):
    record, session, _ = memory
    raw = {"question": "What changed?", "scope": "Development"}
    first = publication.adopt("question", raw)
    assert publication.adopt("question", raw) == first
    assert record.payload["prior"] == {"decision": "preserved"}
    assert record.payload[publication.KEY]["adoption_meaning"] == "explicit_adoption_not_preregistration"
    with pytest.raises(ValueError, match="adoption_conflict"):
        publication.adopt("question", {**raw, "scope": "Holdout"})


def test_history_survives_mutable_claim_and_link_edits(memory):
    record, session, items = memory
    publication.adopt("question", {"question": "What changed?", "scope": "Development"})
    link = SimpleNamespace(id="link", to_dict=lambda: {"id": "link", "source_item_id": "claim", "target_type": "research_item", "target_id": "question", "relation": "supports", "metadata": {"decision": "Initial"}})
    session.links = [link]
    raw = request(items)
    first = publication.publish("question", raw)
    items["claim"]["body"] = "Corrected claim"
    link.to_dict = lambda: {"id": "link", "source_item_id": "claim", "target_type": "research_item", "target_id": "question", "relation": "contradicts", "metadata": {"decision": "Later"}}
    assert publication.publish("question", raw) == first
    assert first["references"][0]["snapshot"]["body"] == "Initial claim"
    assert first["references"][0]["relationships"][0]["relation"] == "supports"
    next_raw = request(items) | {"request_id": "publish-2", "expected_previous_hash": first["publication_hash"], "conclusion": "Negative conclusion."}
    second = publication.publish("question", next_raw)
    assert second["revision"] == 2
    assert record.payload[publication.KEY]["publications"][0] == first
    assert second["previous_hash"] == first["publication_hash"]


@pytest.mark.parametrize("change,error", [
    ({"content_hash": "wrong"}, "hash_mismatch"),
    ({"kind": "observation"}, "wrong target kind"),
    ({"path": "local.json"}, "unexpected fields"),
    ({"role": "permission"}, "unsupported kind/role"),
])
def test_reject_invalid_required_reference(memory, change, error):
    record, session, items = memory
    publication.adopt("question", {"question": "What changed?", "scope": "Development"})
    raw = request(items)
    raw["references"][0].update(change)
    with pytest.raises(ValueError, match=error):
        publication.publish("question", raw)
    assert record.payload[publication.KEY]["publications"] == []


def test_reject_sealed_and_no_publication(memory):
    record, session, items = memory
    items["claim"]["payload"] = {"dataset_id": "sealed"}
    session.protocols = [SimpleNamespace(private_manifest={"datasets": [{"dataset_id": "sealed"}]}, public_manifest={"datasets": [{"dataset_id": None}]})]
    publication.adopt("question", {"question": "What changed?", "scope": "Development"})
    with pytest.raises(ValueError, match="inaccessible"):
        publication.publish("question", request(items))
    assert record.payload[publication.KEY]["publications"] == []


def test_conflicting_retry_and_stale_writer(memory):
    record, session, items = memory
    publication.adopt("question", {"question": "What changed?", "scope": "Development"})
    raw = request(items)
    publication.publish("question", raw)
    with pytest.raises(ValueError, match="request_conflict"):
        publication.publish("question", raw | {"conclusion": "Different"})
    with pytest.raises(ValueError, match="revision_conflict"):
        publication.publish("question", raw | {"request_id": "another"})
    assert len(record.payload[publication.KEY]["publications"]) == 1


def test_generic_json_cannot_claim_publication():
    with pytest.raises(ValueError, match="write_reserved"):
        service.create_research_item({"kind": "study", "title": "Forged", "payload": {publication.KEY: {"publications": []}}})


def test_legacy_history_is_honest(memory):
    assert publication.history("question")["classification"] == "legacy_uncontracted"


def test_check_hashes_reject_without_executing_replay(memory, monkeypatch):
    record, session, items = memory
    items["check"] = {"id": "check", "kind": "research_check", "payload": {}}
    contracts = (SimpleNamespace(definition_hash="definition", definition_id="event_fact_analysis"), SimpleNamespace(request_hash="request"),
                 SimpleNamespace(plan_hash="plan"),
                 SimpleNamespace(evidence_hash="evidence", evidence_kind="immutable_run_report", input_binding={"run_id": "run"}, code_revision="source"),
                 SimpleNamespace(result_hash="result"))
    monkeypatch.setattr(service, "_validate_research_check_evidence_payload", lambda _: contracts)
    monkeypatch.setattr(service, "replay_research_check", lambda _: pytest.fail("publication must never replay"))
    from portal.backend.service.reports import contract as reports_contract
    monkeypatch.setattr(reports_contract, "get_run_research_dataset", lambda _: {"retained": True})
    monkeypatch.setattr(service, "_immutable_run_binding", lambda _: {"run_id": "run"})
    raw = {"item_id": "check", "kind": "research_check", "role": "contradicts", "result_hash": "result", "evidence_hash": "evidence"}
    resolved = publication.resolve_reference(raw, session)
    assert resolved["result_hash"] == "result"
    assert resolved["run_id"] == "run"
    assert "snapshot" not in resolved
    with pytest.raises(ValueError, match="hash_mismatch"):
        publication.resolve_reference(raw | {"evidence_hash": "wrong"}, session)
    monkeypatch.setattr(service, "_immutable_run_binding", lambda _: {"run_id": "different"})
    with pytest.raises(ValueError, match="retained run"):
        publication.resolve_reference(raw, session)


def test_capacity_rejection_preserves_citations_under_simulated_rollback(memory, monkeypatch):
    record, session, items = memory
    publication.adopt("question", {"question": "What changed?", "scope": "Development"})
    first = publication.publish("question", request(items))
    before = deepcopy(record.payload)
    monkeypatch.setattr(publication, "MAX_HISTORY_BYTES", 1)
    with pytest.raises(ValueError, match="retain this question"):
        publication.publish("question", request(items) | {"request_id": "next", "expected_previous_hash": first["publication_hash"]})
    assert record.payload == before
    assert record.payload[publication.KEY]["publications"][0]["publication_hash"] == first["publication_hash"]


def test_stored_history_corruption_cannot_silently_complete(memory):
    record, session, items = memory
    publication.adopt("question", {"question": "What changed?", "scope": "Development"})
    publication.publish("question", request(items))
    record.payload[publication.KEY]["publications"][0]["conclusion"] = "Altered"
    with pytest.raises(ValueError, match="integrity_error"):
        publication.history("question")


def test_api_publication_uses_same_contract(memory, monkeypatch):
    from portal.backend.controller import research
    record, session, items = memory
    research.adopt_research_question("question", research.ResearchQuestionAdoptionRequest(question="What changed?", scope="Development"))
    first = research.publish_research_interpretation("question", research.ResearchPublicationRequest(**request(items)))
    assert research.research_interpretation_history("question")["publications"] == [first]


def test_generic_study_reads_cannot_bypass_history_custody(memory):
    record, session, items = memory
    items["claim"]["payload"] = {"dataset_id": "private-later"}
    publication.adopt("question", {"question": "What changed?", "scope": "Development"})
    publication.publish("question", request(items))
    session.protocols = [SimpleNamespace(private_manifest={"datasets": [{"dataset_id": "private-later"}]}, public_manifest={"datasets": [{"dataset_id": None}]})]
    with pytest.raises(ValueError, match="inaccessible"):
        service.get_research_item("question")
    with pytest.raises(ValueError, match="inaccessible"):
        publication.publish("question", request(items))


def test_missing_required_target_never_appends(memory, monkeypatch):
    record, session, items = memory
    publication.adopt("question", {"question": "What changed?", "scope": "Development"})
    raw = request(items)
    monkeypatch.setattr(publication.repository, "get_item", lambda *args, **kwargs: (_ for _ in ()).throw(KeyError("missing")))
    with pytest.raises(KeyError, match="missing"):
        publication.publish("question", raw)
    assert record.payload[publication.KEY]["publications"] == []


def test_frozen_reference_requires_retained_material_without_replay(memory, monkeypatch):
    from market_data.contracts import build_dataset_identity_hash, dataset_series_identity_payload
    from portal.backend.service.storage.repos.market_data import market_data_repo
    from portal.backend.service.market import backtest_dataset_service
    record, session, items = memory
    entry = {"series_id": 1, "range_start": "2026-01-01T00:00:00Z", "range_end": "2026-01-02T00:00:00Z",
             "max_commit_seq": 1, "row_count": 1, "material_hash": "material", "provenance_hash": "provenance", "quality_hash": "quality"}
    dataset_hash = build_dataset_identity_hash([dataset_series_identity_payload(entry)])
    dataset = SimpleNamespace(dataset_id="dataset", dataset_hash=dataset_hash, series=[entry])
    evidence = SimpleNamespace(evidence_hash="evidence", evidence_kind="frozen_market_data",
        input_binding={"dataset_id": "dataset", "dataset_hash": dataset_hash}, code_revision="source")
    contracts = (SimpleNamespace(definition_hash="definition", definition_id="event_fact_analysis"),
        SimpleNamespace(request_hash="request"), SimpleNamespace(plan_hash="plan"), evidence,
        SimpleNamespace(result_hash="result"))
    items["check"] = {"id": "check", "kind": "research_check", "payload": {}}
    monkeypatch.setattr(service, "_validate_research_check_evidence_payload", lambda _: contracts)
    monkeypatch.setattr(service, "replay_research_check", lambda _: pytest.fail("publication must not replay"))
    monkeypatch.setattr(market_data_repo, "get_dataset", lambda _: dataset)
    calls = []
    def retained(**kwargs):
        calls.append(kwargs)
        return {}, {}, []
    monkeypatch.setattr(backtest_dataset_service, "validate_frozen_dataset_series", retained)
    raw = {"item_id": "check", "kind": "research_check", "role": "supports", "result_hash": "result", "evidence_hash": "evidence"}
    resolved = publication.resolve_reference(raw, session)
    assert resolved["dataset_hash"] == dataset_hash
    assert resolved["replay_evidence"] == "not_established_by_publication"
    assert calls[0]["entry"]["dataset_id"] == "dataset"
    monkeypatch.setattr(backtest_dataset_service, "validate_frozen_dataset_series", lambda **kwargs: (_ for _ in ()).throw(ValueError("retained material missing")))
    with pytest.raises(ValueError, match="retained material missing"):
        publication.resolve_reference(raw, session)
    monkeypatch.setattr(market_data_repo, "get_dataset", lambda _: (_ for _ in ()).throw(ValueError("retained source missing")))
    with pytest.raises(ValueError, match="retained source missing"):
        publication.resolve_reference(raw, session)


def test_reasoning_content_identity_binds_source_scope_and_tags(memory):
    record, session, items = memory
    original = items["claim"]
    identity = publication.content_identity(original)
    for field in ("datasource", "exchange", "tags"):
        changed = deepcopy(original)
        changed[field] = ["changed"] if field == "tags" else "changed"
        assert publication.content_identity(changed) != identity
