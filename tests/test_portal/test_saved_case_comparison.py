"""Synthetic mixed native/saved custody and pinned reconstruction."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from market_data.frozen import semantic_hash
from portal.backend.service.research import catalog, planning
from portal.backend.service.research import saved_case_comparison_evaluator as adapter
from portal.backend.service.research.registry import (normalize_check_request,
    pin_check_definition_to_plan, reconstruct_pinned_check_definition)
from research_science.check import CheckRequest, ResolvedCheckPlan
from research_science.recognition import plain


def payload():
    return deepcopy(catalog.get_check_definition("event_fact_analysis", "16")["examples"][0]["request"])


def test_explicit_new_version_and_normalized_worker_reconstruction(monkeypatch):
    raw = payload()
    definition, request = normalize_check_request(raw)
    assert definition.evaluator_version == "16"
    monkeypatch.setattr(adapter, "dependencies", lambda config: [])
    plan = planning.plan_research_check(definition, request, inspect_coverage=False,
        instrument_loader=lambda key: {"id": key, "symbol": "SYNTHETIC"},
        indicator_planner=lambda *a, **k: {"indicators": [], "requirements": [], "warmup_bars": 98})
    definition, request, plan = pin_check_definition_to_plan(definition, request, plan)
    restored = reconstruct_pinned_check_definition(CheckRequest.from_dict(request.to_dict()), ResolvedCheckPlan.from_dict(plan.to_dict()))
    assert restored.to_dict() == definition.to_dict()
    assert plan.execution["saved_dependencies"] == []
    assert plan.execution["saved_dependency_limits"]["prior_market_replay"] is False
    assert plan.outcome_tail["seconds"] == 0
    assert plan.execution["warmup_floor_bars"] == 98


@pytest.mark.parametrize("mutation", [
    lambda p: p.update(mode="preview"),
    lambda p: p.update(gap_policy="continue_degraded"),
    lambda p: p["outcomes"].update(horizon_bars=11),
    lambda p: p["outcomes"]["target"].update(multiple=2),
    lambda p: p["statistics"]["comparison"]["comparison_recipe"]["model"].update(C=2),
    lambda p: p["statistics"]["comparison"]["training_sources"].append(deepcopy(p["statistics"]["comparison"]["training_sources"][0])),
    lambda p: p["statistics"]["comparison"].update(frozen_model_source={}),
    lambda p: p["scope"].update(start="2024-01-02T00:15:00Z"),
])
def test_recipe_and_population_relaxations_fail_loudly(mutation):
    raw = payload()
    mutation(raw)
    with pytest.raises(ValueError):
        normalize_check_request(raw)


def test_actual_repository_payload_flags_are_required_without_projection_duplicates(monkeypatch):
    from portal.backend.db.models import ResearchItemRecord
    record = ResearchItemRecord(id="00000000-0000-0000-0000-000000000001", kind="research_check", status="tested",
        instrument_id="synthetic", timeframe="15m", payload={"observation_eligible": True, "replayable": True},
        created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1))
    item = adapter.repository.get_item(record.id, session=SimpleNamespace(get=lambda *a: record))
    assert "observation_eligible" not in item
    ref = payload()["statistics"]["comparison"]["training_sources"][0]
    # Reaching typed envelope validation proves optional projections are not
    # falsely required. Full immutable-envelope fixtures are tested separately.
    with pytest.raises(ValueError, match="unsupported payload schema"):
        adapter.load_source(ref, kind="training", item_loader=lambda key: item)
    item["observation_eligible"] = False
    with pytest.raises(ValueError, match="eligibility"):
        adapter.load_source(ref, kind="training", item_loader=lambda key: item)
    item.pop("observation_eligible")
    item["payload"]["replayable"] = 1
    with pytest.raises(ValueError, match="literally"):
        adapter.load_source(ref, kind="training", item_loader=lambda key: item)


def test_total_saved_case_limit_remains_bounded(monkeypatch):
    config = payload()["statistics"]["comparison"]
    monkeypatch.setattr(adapter, "load_source", lambda *a, **k: {"bytes": 1, "cases": 60001})
    with pytest.raises(ValueError, match="bound"):
        adapter.dependencies(config)


def test_existing_result_and_direct_export_limits_fail_without_truncation():
    adapter.validate_result_size({"small": "valid"})
    with pytest.raises(ValueError, match="direct export"):
        adapter.validate_result_size({"large": "x" * adapter.RULES["pretty_result_bytes"]})
    with pytest.raises(ValueError, match="16MiB"):
        adapter.validate_result_size({"large": "x" * adapter.RULES["result_bytes"]})


def native_plan(raw):
    from portal.backend.service.indicators.indicator_service import plan_runtime_requirements_for_indicators
    definition, request = normalize_check_request(raw)
    key = raw["scope"]["indicator_id"]
    meta = {"id": key, "type": "reversal", "version": "v1", "dependencies": [], "enabled": True, "runtime_supported": True,
            "params": {"computation_origin": raw["outcomes"]["input_origin"], "ownership": "retain_all_provisional",
                       "reference_basis": "rising_leg_plus_retreat", "max_bars": 19104, "max_candidates": 10000}}
    plan = planning.plan_research_check(definition, request, inspect_coverage=False,
        instrument_loader=lambda key: {"id": key, "symbol": "SYNTHETIC"},
        indicator_planner=lambda ids, **kw: plan_runtime_requirements_for_indicators(ids, **kw, preloaded_metas={key: meta}))
    plan = replace(plan, plan_hash="", materialization_range={**plan.materialization_range, "as_of_commit_seq": 10})
    from tests.test_portal.test_reversal_population_evidence import native_binding
    from research_science.recognition import utc
    count = int((utc(plan.materialization_range["end_exclusive"]) - utc(plan.materialization_range["start"])).total_seconds() / 900)
    prototype = native_binding(replace(request, dataset_id="mds_" + "a" * 32, request_hash=""), plan, count)
    dataset_hash = dataset_identity(prototype["series"])[0]
    request = replace(request, dataset_id="mds_" + dataset_hash[:32], request_hash="")
    plan = replace(plan, request_hash=request.request_hash, plan_hash="")
    return pin_check_definition_to_plan(definition, request, plan)


def dataset_identity(series):
    from market_data.contracts import build_dataset_identity_hash, dataset_series_identity_payload
    entries = {semantic_hash(dataset_series_identity_payload(r)): r for r in series}
    return build_dataset_identity_hash([dataset_series_identity_payload(r) for r in entries.values()]), list(entries.values())


def native_inputs(request, plan):
    from market_data.frozen import build_frozen_market_data_read_binding
    from market_data.contracts import build_dataset_identity_hash, dataset_series_identity_payload
    from tests.test_portal.test_reversal_population_evidence import native_binding, SOURCE
    from research_science.recognition import utc
    origin = utc(plan.materialization_range["start"])
    count = int((utc(plan.materialization_range["end_exclusive"]) - origin).total_seconds() / 900)
    candles = [{"open_time": plain(origin + timedelta(minutes=15 * i)),
                "close_time": plain(origin + timedelta(minutes=15 * (i + 1))),
                "known_at": plain(origin + timedelta(minutes=15 * (i + 1))), "source_identity_key": SOURCE,
                "open": 100 + i / 100, "close": 100 + (i + 1) / 100, "high": 101 + i / 100, "low": 99 + i / 100}
               for i in range(count)]
    binding = native_binding(replace(request, dataset_id="mds_" + "a" * 32, request_hash=""), plan, count)
    primary = binding["series"][0]
    series = [{**primary, "alias": req["alias"], "requirement": dict(req)} for req in plan.market_data_requirements]
    dataset_hash = dataset_identity(series)[0]
    binding = build_frozen_market_data_read_binding(dataset_id=request.dataset_id, dataset_hash=dataset_hash,
        max_commit_seq=10, subjects=binding["subjects"],
        series=series)
    return {**request.parameters, "frozen_input_binding": binding,
            "data_quality": {"recorded_gaps": []}, "indicator_evidence": {"schema_version": "indicator_output_evidence.v1",
            "candles": candles, "outputs": [], "gap_transitions": []}}


def saved_v15():
    from research_science.check import CheckResult, CheckEvidenceBinding
    from portal.backend.db.models import ResearchItemRecord
    raw = payload()
    raw["outcomes"]["schema_version"] = adapter.barriers.SCHEMA
    raw["statistics"].pop("comparison")
    definition, request, plan = native_plan(raw)
    inputs = native_inputs(request, plan)
    result = adapter.barriers.evaluate(plan=plan, inputs=inputs)
    binding = inputs["frozen_input_binding"]
    evidence = CheckEvidenceBinding(schema_version="research.check_evidence_binding.v2", definition_hash=definition.definition_hash,
        request_hash=request.request_hash, plan_hash=plan.plan_hash, code_revision="synthetic", evidence_kind="frozen_market_data",
        input_binding=binding, indicator_graph_hash=semantic_hash({"indicators": list(plan.indicator_graph)}),
        indicator_output_hash="output", fact_input_hash="facts", gap_transition_hash="transitions",
        quality_hash=semantic_hash(dict(binding.get("quality") or {})), gaps_hash=semantic_hash({"recorded_gaps": []}))
    contract = CheckResult(schema_version="research.check_result.v2", definition_hash=definition.definition_hash,
        request_hash=request.request_hash, plan_hash=plan.plan_hash, evidence_hash=evidence.evidence_hash,
        evaluator_id="event_fact_analysis", evaluator_version="15", result={**result, "promotion_authority": False, "execution_authority": False})
    p = {"schema_version": "research_check_payload.v2", "observation_eligible": True, "replayable": True,
         "definition": definition.to_dict(), "request": request.to_dict(), "plan": plan.to_dict(),
         "evidence": evidence.to_dict(), "result": contract.to_dict()}
    record = ResearchItemRecord(id="00000000-0000-0000-0000-000000000001", kind="research_check", status="tested",
        instrument_id=request.scope["instrument_id"], timeframe="15m", payload=p,
        created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1))
    item = adapter.repository.get_item(record.id, session=SimpleNamespace(get=lambda *a: record))
    ref = {"check_id": record.id, "result_hash": contract.result_hash, "evidence_hash": evidence.evidence_hash,
           "population_hash": result["population_hash"]}
    return item, ref


def test_full_repository_envelope_source_custody_and_no_market_loading():
    item, ref = saved_v15()
    result = adapter.load_source(ref, kind="training", item_loader=lambda key: item)
    assert result["cases"] == 0
    assert result["request"].dataset_id == item["payload"]["request"]["dataset_id"]
    for key in ("result_hash", "evidence_hash", "population_hash"):
        with pytest.raises(ValueError):
            adapter.load_source({**ref, key: "f" * 64}, kind="training", item_loader=lambda key: item)


def test_one_current_native_episode_evaluation_fits_only_saved_development(monkeypatch):
    from tests.test_research_science.test_saved_case_comparison import row
    from research_science.recognition import utc
    from tests.test_portal.test_reversal_population_evidence import SOURCE
    raw = payload()
    config = raw["statistics"]["comparison"]
    origin = datetime(2023, 12, 1, tzinfo=UTC)
    rows = [row(i, label=adapter.math.CLASSES[i % 3], clock=origin + timedelta(minutes=15 * i)) for i in range(210)]
    for r in rows:
        r["features"] = {**r["comparison_features"], "recovery_candles": r["comparison_features"]["N"]}
        r["original_weight"] = 1 / 210
    saved = {"reference": config["training_sources"][0], "bytes": 1, "cases": len(rows), "binding_hash": "frozen",
             "input_identity": {"synthetic": "distinct-saved-development-dataset"},
             "source": SOURCE, "request": SimpleNamespace(scope={"instrument_id": raw["scope"]["instrument_id"]}),
             "result": {"cases": rows, "input_origin": plain(origin), "evaluation_range": {"end_exclusive": "2023-12-10T00:00:00Z"}}}
    monkeypatch.setattr(adapter, "dependencies", lambda config: [saved])
    definition, request, plan = native_plan(raw)
    inputs = native_inputs(request, plan)
    at = utc(raw["outcomes"]["input_origin"]) + timedelta(minutes=1500)
    inputs["indicator_evidence"]["outputs"] = [{"indicator_id": raw["scope"]["indicator_id"],
        "output_type": "signal", "output_name": "provisional_reversals", "event_key": "provisional_reversal_completed",
        "time": plain(at - timedelta(minutes=15)), "known_at": plain(at), "event": {
            "pattern_id": "current-one", "known_at": plain(at), "direction": "long", "metadata": {
                "recovery_candles": 3, "context": {"sigma": .01, "r": .02, "q": 1., "z_pre12": 0.},
                "recognition": {"event_id": "current-one", "formation_start": plain(at - timedelta(hours=3)),
                                "known_at": plain(at), "context": {"source_identity_key": SOURCE}, "source_events": []}}}}]
    result = adapter.evaluate(plan=plan, inputs=inputs)
    assert result["schema_version"] == adapter.RESULT_SCHEMA
    assert result["training_support"]["eligible_clocks"] == 210
    assert result["model"]["model_hash"]
    assert result["comparison"]["duration_supported"] is False
    assert len(result["scored_evidence"]["cases"]) == 1
    assert result["scored_evidence"]["cases"][0]["root_weight"] == 1
    assert result["scored_evidence"]["alternatives"]
    assert all(utc(r["decision_at"]) >= utc(raw["scope"]["start"]) for r in result["scored_evidence"]["alternatives"])
    exercise_direct_publication_export(monkeypatch, definition, request, plan, inputs, result)
    later = deepcopy(raw)
    later_config = later["statistics"]["comparison"]
    later_config["frozen_model_source"] = {"check_id": "00000000-0000-0000-0000-000000000002", "result_hash": "a" * 64,
                                           "evidence_hash": "b" * 64, "model_hash": result["model"]["model_hash"]}
    later["outcomes"]["input_origin"] = later_config["validation_episodes"][1]["origin"]
    later["scope"]["start"] = plain(utc(later["outcomes"]["input_origin"]) + timedelta(minutes=1470))
    later["scope"]["end"] = later_config["validation_episodes"][1]["end_exclusive"]
    frozen = {"reference": later_config["frozen_model_source"], "result": result, "bytes": 1, "cases": 1,
              "input_identity": {"synthetic": "distinct-first-validation-dataset"},
              "binding_hash": "different-real-validation-input", "source": SOURCE}
    monkeypatch.setattr(adapter, "dependencies", lambda config: [saved, frozen])
    monkeypatch.setattr(adapter.math, "fit_models", lambda *a, **k: pytest.fail("validation refit forbidden"))
    _, next_request, next_plan = native_plan(later)
    next_result = adapter.evaluate(plan=next_plan, inputs=native_inputs(next_request, next_plan))
    assert next_result["model"] == result["model"]


def exercise_direct_publication_export(monkeypatch, definition, request, plan, inputs, result):
    from research_science.check import CheckEvidenceBinding, CheckResult
    from portal.backend.service.research import publication, evidence_export, service
    from portal.backend.service.storage.repos.market_data import market_data_repo
    from portal.backend.service.market import backtest_dataset_service
    binding = inputs["frozen_input_binding"]
    evidence = CheckEvidenceBinding(schema_version="research.check_evidence_binding.v2", definition_hash=definition.definition_hash,
        request_hash=request.request_hash, plan_hash=plan.plan_hash, code_revision="synthetic", evidence_kind="frozen_market_data",
        input_binding=binding, indicator_graph_hash=semantic_hash({"indicators": list(plan.indicator_graph)}),
        indicator_output_hash="output", fact_input_hash="facts", gap_transition_hash="transitions",
        quality_hash=semantic_hash(dict(binding.get("quality") or {})), gaps_hash=semantic_hash({"recorded_gaps": []}))
    contract = CheckResult(schema_version="research.check_result.v2", definition_hash=definition.definition_hash,
        request_hash=request.request_hash, plan_hash=plan.plan_hash, evidence_hash=evidence.evidence_hash,
        evaluator_id="event_fact_analysis", evaluator_version="16", result={**result, "promotion_authority": False, "execution_authority": False})
    item = {"id": "00000000-0000-0000-0000-000000000003", "kind": "research_check", "status": "tested", "payload": {
        "schema_version": "research_check_payload.v2", "observation_eligible": True, "replayable": True,
        "definition": definition.to_dict(), "request": request.to_dict(), "plan": plan.to_dict(),
        "evidence": evidence.to_dict(), "result": contract.to_dict()}}
    service._validate_research_check_evidence_payload(item["payload"], item_id=item["id"])
    monkeypatch.setattr(publication, "_assert_access", lambda *a: None)
    monkeypatch.setattr(publication.repository, "get_item", lambda *a, **k: deepcopy(item))
    monkeypatch.setattr(market_data_repo, "get_dataset", lambda key: SimpleNamespace(dataset_id=request.dataset_id,
        dataset_hash=binding["dataset_hash"], series=dataset_identity(binding["series"])[1]))
    monkeypatch.setattr(backtest_dataset_service, "validate_frozen_dataset_series", lambda **kw: ({}, {}, []))
    session = SimpleNamespace(scalars=lambda *a: SimpleNamespace(all=lambda: []))
    ref = publication.resolve_reference({"item_id": item["id"], "kind": "research_check", "role": "supports",
        "result_hash": contract.result_hash, "evidence_hash": evidence.evidence_hash}, session)
    assert ref["result_hash"] == contract.result_hash
    assert "run_id" not in ref
    monkeypatch.setattr(evidence_export, "record_snapshot", lambda identifier: deepcopy(item))
    monkeypatch.setattr(evidence_export.images, "list_images", lambda identifier: {"items": []})
    bundle = evidence_export.bundle(item["id"])
    assert bundle.startswith(b"PK")
    from contextlib import contextmanager
    from hashlib import sha256
    from portal.backend.service.research import reports
    from tests.test_portal.test_research_publication import Session
    study = SimpleNamespace(id="synthetic-comparison-study", kind="study", payload={}, updated_at=None)
    study.to_dict = lambda: {"id": study.id, "kind": study.kind, "payload": deepcopy(study.payload)}
    study_session = Session(study)
    @contextmanager
    def scope():
        yield study_session
    monkeypatch.setattr(publication.db, "session", scope)
    monkeypatch.setattr(publication.repository, "get_item", lambda key, **kw: deepcopy(item) if key == item["id"] else study.to_dict())
    publication.adopt(study.id, {"question": "Synthetic saved dependency comparison?", "scope": "Synthetic fixtures"})
    published = publication.publish(study.id, {"request_id": "synthetic-comparator-publication", "expected_previous_hash": None,
        "conclusion": "Synthetic contract check only", "limitations": "No campaign outcomes", "scope": "Synthetic fixtures",
        "references": [{"item_id": item["id"], "kind": "research_check", "role": "supports",
                        "result_hash": contract.result_hash, "evidence_hash": evidence.evidence_hash}]})
    attached = reports.attach(study.id, {"title": "Synthetic comparator", "publication_hash": published["publication_hash"],
        "html": "<h1>Synthetic contract</h1><p>No scientific claim.</p>", "source_qmd": "# Synthetic contract\nNo scientific claim.",
        "build": {"quarto_version": "synthetic-attachment-contract", "source_revision": "b" * 40,
                  "evidence_bundle_sha256": sha256(bundle).hexdigest()}})
    assert attached["binding"]["references"][0]["result_hash"] == contract.result_hash
    assert reports.get_content(study.id, attached["report_id"])


def test_actual_claimed_worker_dispatch_reconstructs_new_version(monkeypatch):
    from contextlib import nullcontext
    from portal.backend.service.async_jobs import ClaimedJob
    from portal.backend.service.research import async_dispatch
    from portal.backend.workers import research_worker
    monkeypatch.setattr(adapter, "dependencies", lambda config: [])
    definition, request, plan = native_plan(payload())
    job = ClaimedJob(id="synthetic-saved-comparison", job_type=async_dispatch.JOB_TYPE_RESEARCH_CHECK_RUN,
        payload={"request": {"request": request.to_dict(), "plan": plan.to_dict()}}, attempts=1, max_attempts=1,
        partition_key=None, partition_hash=0, lock_owner="synthetic", claim_token="synthetic", claim_generation=1)
    seen = {}
    def build(serialized):
        r = CheckRequest.from_dict(serialized["request"])
        p = ResolvedCheckPlan.from_dict(serialized["plan"])
        seen["definition"] = reconstruct_pinned_check_definition(r, p).to_dict()
        return {"schema_version": "research_check_evidence_build.v1"}
    monkeypatch.setattr(research_worker, "maintain_job_heartbeat", lambda *a, **k: nullcontext())
    monkeypatch.setattr(research_worker.research_service, "build_research_check_evidence", build)
    monkeypatch.setattr(research_worker.research_service, "persist_built_research_check_evidence", lambda *a, **k: {"status": "completed"})
    monkeypatch.setattr(research_worker, "complete_job_with_owned_effect", lambda claimed, effect: effect("synthetic-session"))
    research_worker.execute_claimed_research_job(job)
    assert seen["definition"] == definition.to_dict()


def test_final_aggregation_preserves_original_clock_mass_and_block_signs():
    from tests.test_research_science.test_saved_case_comparison import row
    from research_science.recognition import utc
    schedule = payload()["statistics"]["comparison"]["validation_episodes"]
    episodes = []
    for i, episode in enumerate(schedule):
        cases, scores = [], []
        origin = utc(episode["origin"])
        for j in range(3 if i == 1 else 1):
            case = row(j, clock=origin + timedelta(hours=30, minutes=j * 15), episode=episode["origin"])
            case.update(event_id=f"{i}-{j}", recognition_origin=episode["origin"],
                        support_start=plain(origin), support_end=plain(origin + timedelta(hours=40)))
            cases.append(case)
            scores.append({"event_id": case["event_id"], "episode": episode["origin"], "root_weight": 1.,
                           "gain": -.1 if i == 1 else .2})
        episodes.append({"episode": episode["origin"], "cases": cases, "scores": scores, "alternatives": [], "pairs": []})
    result = adapter.summarize(episodes, schedule, lambda: None)
    assert result["duration_gain"] == pytest.approx(.05)
    assert result["duration_gain"] != pytest.approx((.2 - .1 + .2 + .2) / 4)
    assert result["chronological_blocks"][0]["duration_gain"] < 0
    assert result["chronological_blocks"][1]["duration_gain"] > 0
    assert all(b["complete"] for b in result["chronological_blocks"])
    assert result["conditional_intervals"]["duration"]["group_count"] == 4
    assert result["duration_supported"] is False
    assert result["opportunities"]["cases"]["class_counts"]["continuation"] == 6
