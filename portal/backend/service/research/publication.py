"""Question-owned interpretation history; publication never executes research."""
from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, Mapping

from sqlalchemy import select
from market_data.frozen import semantic_hash
from portal.backend.db.models import ResearchItemRecord, ResearchLinkRecord, ResearchProtocolRecord
from portal.backend.db.session import db
from . import repository

KEY = "question_contract"
SCHEMA = "research_question.v1"
ROLES = {"supports", "contradicts", "context", "decision", "deviation", "replay_dependency"}
MAX_HISTORY_BYTES = 1024 * 1024
logger = logging.getLogger(__name__)


def required(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"question_contract_invalid: {field} is required")
    return value.strip()


def exact_fields(raw: Mapping[str, Any], fields: set[str]) -> None:
    if not isinstance(raw, Mapping) or set(raw) - fields:
        raise ValueError("question_contract_invalid: unexpected fields")


def question(raw: Mapping[str, Any]) -> dict[str, str]:
    exact_fields(raw, {"question", "scope"})
    result = {key: required(raw.get(key), key) for key in ("question", "scope")}
    if any(len(value) > 8192 for value in result.values()):
        raise ValueError("question_contract_invalid: question/scope exceeds 8192 characters")
    return result


def content_identity(item: Mapping[str, Any]) -> str:
    # Metadata timestamps are not interpretation identity.
    return semantic_hash({key: item.get(key) for key in (
        "id", "kind", "title", "status", "body", "payload", "instrument_id",
        "symbol", "timeframe", "window_start", "window_end", "source_revision",
    )})


def _assert_access(item: Mapping[str, Any], session: Any) -> None:
    # Protocol custody is the existing owner. Do not release its sealed binding.
    def ids(value: Any) -> set[str]:
        if isinstance(value, Mapping):
            found = {str(value["dataset_id"])} if value.get("dataset_id") else set()
            if value.get("target_type") in {"dataset", "frozen_dataset", "market_dataset"} and value.get("target_id"):
                found.add(str(value["target_id"]))
            return found.union(*(ids(child) for child in value.values()))
        if isinstance(value, list):
            return set().union(*(ids(child) for child in value))
        return set()
    referenced = ids(item)
    for protocol in session.scalars(select(ResearchProtocolRecord)).all():
        private = protocol.private_manifest or {}
        public = protocol.public_manifest or {}
        hidden = ids(private) - ids(public)
        if referenced & hidden:
            raise ValueError("question_evidence_inaccessible: sealed scientific evidence")


def resolve_reference(raw: Mapping[str, Any], session: Any) -> dict[str, Any]:
    exact_fields(raw, {"item_id", "kind", "role", "content_hash", "result_hash", "evidence_hash"})
    identifier = required(raw.get("item_id"), "item_id")
    kind = required(raw.get("kind"), "kind")
    role = required(raw.get("role"), "role")
    if kind not in {"research_check", "hypothesis", "observation", "study"} or role not in ROLES:
        raise ValueError("question_evidence_invalid: unsupported kind/role")
    item = repository.get_item(identifier, session=session)
    if item["kind"] != kind:
        raise ValueError("question_evidence_invalid: wrong target kind")
    _assert_access(item, session)
    resolved = {"item_id": identifier, "kind": kind, "role": role, "resolution": "resolved_at_publication"}
    if kind == "research_check":
        from .service import _validate_research_check_evidence_payload, _definition_evidence_classification
        definition, request, plan, evidence, result = _validate_research_check_evidence_payload(item["payload"])
        for field, actual in (("result_hash", result.result_hash), ("evidence_hash", evidence.evidence_hash)):
            if required(raw.get(field), field) != actual:
                raise ValueError(f"question_evidence_hash_mismatch: {field}")
            resolved[field] = actual
        binding = dict(evidence.input_binding)
        dependency = {}
        if evidence.evidence_kind == "frozen_market_data":
            from portal.backend.service.storage.repos.market_data import market_data_repo
            from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
            from market_data.contracts import build_dataset_identity_hash, dataset_series_identity_payload
            dataset = market_data_repo.get_dataset(required(binding.get("dataset_id"), "dataset_id"))
            if dataset.dataset_hash != binding.get("dataset_hash") or build_dataset_identity_hash(
                [dataset_series_identity_payload(row) for row in dataset.series]
            ) != dataset.dataset_hash:
                raise ValueError("question_evidence_hash_mismatch: Dataset")
            for entry in dataset.series:
                validate_frozen_dataset_series(store=market_data_repo,
                    entry={**dict(entry), "dataset_id": dataset.dataset_id}, allow_any_recorded_gap=True)
            dependency = {"dataset_id": dataset.dataset_id, "dataset_hash": dataset.dataset_hash}
        else:
            # Existing report-bound Check evidence remains Check-owned; never
            # reinterpret RunReportDTO as a multi-Check interpretation.
            from portal.backend.service.reports import contract as reports_contract
            from .service import _immutable_run_binding
            run_id = required(binding.get("run_id"), "run_id")
            retained = _immutable_run_binding(reports_contract.get_run_research_dataset(run_id))
            if retained != binding:
                raise ValueError("question_evidence_hash_mismatch: retained run evidence binding")
            dependency = {"run_id": run_id, "report_semantic_fingerprint": binding.get("report_semantic_fingerprint")}
        resolved.update({"evidence_classification": _definition_evidence_classification(definition),
                         "definition_hash": definition.definition_hash,
                         "request_hash": request.request_hash, "plan_hash": plan.plan_hash,
                         **dependency,
                         "binding_hash": binding.get("binding_hash"), "code_revision": evidence.code_revision,
                         "replay_evidence": "not_established_by_publication"})
    else:
        identity = content_identity(item)
        if required(raw.get("content_hash"), "content_hash") != identity:
            raise ValueError("question_evidence_hash_mismatch: content_hash")
        resolved["content_hash"] = identity
        resolved["assurance"] = "reasoning_snapshot_not_calculated_evidence"
        # Small reasoning records must remain interpretable after subsequent edits.
        snapshot = deepcopy(item)
        if kind == "study":
            snapshot["payload"] = {key: value for key, value in snapshot["payload"].items() if key != KEY}
        resolved["snapshot"] = snapshot
    links = session.scalars(select(ResearchLinkRecord).where(
        (ResearchLinkRecord.source_item_id == identifier) |
        ((ResearchLinkRecord.target_type == "research_item") & (ResearchLinkRecord.target_id == identifier))
    )).all()
    resolved["relationships"] = [link.to_dict() for link in sorted(links, key=lambda row: row.id)]
    _assert_access(resolved, session)
    return resolved


def verify_history(contract: Mapping[str, Any], item_id: str) -> None:
    if contract.get("schema_version") != SCHEMA:
        raise ValueError("question_contract_invalid: unsupported stored contract")
    adopted = question(contract.get("question"))
    previous = None
    requests = set()
    for index, revision in enumerate(contract.get("publications", []), 1):
        if not isinstance(revision, Mapping):
            raise ValueError("question_publication_integrity_error: invalid revision")
        material = {key: value for key, value in revision.items() if key != "publication_hash"}
        if (revision.get("publication_hash") != semantic_hash(material)
            or revision.get("previous_hash") != previous
            or revision.get("revision") != index
            or revision.get("question_id") != item_id
            or revision.get("question") != adopted
            or revision.get("request_id") in requests):
            raise ValueError("question_publication_integrity_error: history/hash disagreement")
        requests.add(revision.get("request_id"))
        previous = revision["publication_hash"]


def _locked(item_id: str, session: Any) -> Any:
    record = session.scalar(select(ResearchItemRecord).where(ResearchItemRecord.id == item_id).with_for_update())
    if record is None:
        raise KeyError("Research question not found")
    if record.kind != "study":
        raise ValueError("question_contract_invalid: question must be a Study memory record")
    return record


def adopt(item_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    material = question(raw)
    with db.session() as session:
        record = _locked(item_id, session)
        payload = deepcopy(record.payload or {})
        _assert_access(record.to_dict(), session)
        if KEY in payload:
            if not isinstance(payload[KEY], Mapping) or payload[KEY].get("schema_version") != SCHEMA:
                raise ValueError("question_adoption_legacy_field_collision: existing field preserved")
            verify_history(payload[KEY], item_id)
            if payload[KEY].get("question") != material:
                raise ValueError("question_adoption_conflict")
            return record.to_dict()
        payload[KEY] = {"schema_version": SCHEMA, "question": material,
                        "adopted_at": repository.utcnow().isoformat() + "Z",
                        "adoption_meaning": "explicit_adoption_not_preregistration",
                        "publications": []}
        record.payload = payload
        record.updated_at = repository.utcnow()
        session.flush()
        result = record.to_dict()
    logger.info("research_question_adopted | question_id=%s", item_id)
    return result


def publish(item_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    exact_fields(raw, {"request_id", "expected_previous_hash", "conclusion", "limitations", "scope", "references"})
    request_id = required(raw.get("request_id"), "request_id")
    if len(request_id) > 128:
        raise ValueError("question_contract_invalid: request_id exceeds 128 characters")
    material = {key: required(raw.get(key), key) for key in ("conclusion", "limitations", "scope")}
    if any(len(value) > 8192 for value in material.values()):
        raise ValueError("question_contract_invalid: interpretation text exceeds 8192 characters")
    references = raw.get("references")
    if not isinstance(references, list) or not references or len(references) > 100:
        raise ValueError("question_contract_invalid: 1..100 exact references required")
    digest = semantic_hash(dict(raw))
    with db.session() as session:
        record = _locked(item_id, session)
        payload = deepcopy(record.payload or {})
        contract = payload.get(KEY)
        if not isinstance(contract, dict) or contract.get("schema_version") != SCHEMA:
            raise ValueError("question_adoption_required")
        verify_history(contract, item_id)
        history = contract["publications"]
        for prior in history:
            if prior["request_id"] == request_id:
                if prior["request_hash"] != digest:
                    raise ValueError("question_publication_request_conflict")
                _assert_access(prior, session)
                return deepcopy(prior)
        latest = history[-1]["publication_hash"] if history else None
        if raw.get("expected_previous_hash") != latest:
            raise ValueError("question_publication_revision_conflict")
        resolved = [resolve_reference(reference, session) for reference in references]
        selected = {item_id, *(reference["item_id"] for reference in resolved)}
        for reference in resolved:
            reference["relationships"] = [link for link in reference["relationships"]
                if link.get("target_type") != "research_item" or
                (link.get("source_item_id") in selected and link.get("target_id") in selected)]
        links = session.scalars(select(ResearchLinkRecord).where(
            (ResearchLinkRecord.source_item_id == item_id) |
            ((ResearchLinkRecord.target_type == "research_item") & (ResearchLinkRecord.target_id == item_id))
        )).all()
        graph = [link.to_dict() for link in sorted(links, key=lambda row: row.id)
                 if link.to_dict().get("source_item_id") in selected
                 and link.to_dict().get("target_type") == "research_item"
                 and link.to_dict().get("target_id") in selected]
        _assert_access({"relationships": graph}, session)
        revision = {"schema_version": "research_interpretation_revision.v1", "question_id": item_id,
                    "revision": len(history) + 1, "previous_hash": latest,
                    "request_id": request_id, "request_hash": digest,
                    "question": deepcopy(contract["question"]), **material,
                    "references": resolved, "relationships": graph, "published_at": repository.utcnow().isoformat() + "Z",
                    "completion_meaning": "reference_complete_interpretation",
                    "authority": "no_statistical_causal_profitability_or_trading_certification",
                    "replay_recovery_meaning": "requires_separate_actual_execution_evidence"}
        revision["publication_hash"] = semantic_hash(revision)
        history.append(revision)
        import json
        if len(json.dumps(contract).encode()) > MAX_HISTORY_BYTES:
            raise ValueError("question_publication_history_limit: 1 MiB. Existing publications preserved; retain this question and request a reviewed history storage expansion.")
        record.payload = payload
        record.updated_at = repository.utcnow()
        session.flush()
        result = deepcopy(revision)
    logger.info("research_interpretation_published | question_id=%s revision=%s publication_hash=%s request_id=%s",
                item_id, result["revision"], result["publication_hash"], request_id)
    return result


def history(item_id: str) -> dict[str, Any]:
    with db.session() as session:
        item = repository.get_item(item_id, session=session)
        if item["kind"] != "study":
            raise ValueError("question_contract_invalid: question must be a Study memory record")
        _assert_access(item, session)
    contract = item["payload"].get(KEY)
    if not isinstance(contract, Mapping) or contract.get("schema_version") != SCHEMA:
        return {"question_id": item_id, "classification": "legacy_uncontracted", "publications": []}
    verify_history(contract, item_id)
    return {"question_id": item_id, "classification": "contracted_question", **deepcopy(contract)}
