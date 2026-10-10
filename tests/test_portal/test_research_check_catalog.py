from copy import deepcopy

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller.research import router
from portal.backend.service.research import catalog, service
from portal.backend.service.research.registry import (
    CHECK_REGISTRY, normalize_check_request, normalize_check_preparation_request,
)
from tests.test_portal.test_matched_origin_check import _plan


def client():
    app = FastAPI()
    app.include_router(router, prefix="/api/research")
    return TestClient(app)


def test_list_detail_complete_exact_and_read_only():
    before = [d.to_dict() for d in CHECK_REGISTRY.definitions()]
    api = client()
    response = api.get("/api/research/checks/definitions")
    assert response.status_code == 200
    assert response.json() == catalog.list_check_definitions()
    assert len(response.json()["items"]) == len(before)
    for item in response.json()["items"]:
        d = item["definition"]
        detail = api.get(f"/api/research/checks/definitions/{d['definition_id']}/{d['definition_version']}")
        assert detail.status_code == 200
        assert detail.json() == catalog.get_check_definition(d["definition_id"], d["definition_version"])
        assert detail.json()["definition"] == d
        for example in detail.json()["examples"]:
            configured, request = normalize_check_request(example["request"])
            assert configured.definition_version.split("+")[0] == d["definition_version"]
            assert configured.to_dict() == example["configured_definition"]
            if "evidence" in detail.json()["eligibility"]["modes"]:
                normalize_check_preparation_request(example["request"])
    assert [d.to_dict() for d in CHECK_REGISTRY.definitions()] == before
    assert api.get("/api/research/checks/definitions/event_fact_analysis/999").status_code == 404
    assert api.get("/api/research/checks/definitions/not_a_method/11").status_code == 404


def test_historical_preview_and_fixed_settings_are_honest():
    historical = catalog.get_check_definition("event_fact_analysis", "3")
    assert not historical["eligibility"]["request_selectable"]
    assert historical["examples"] == []
    assert catalog.get_check_definition("raw_forward_outcome", "2")["eligibility"]["modes"] == ["preview"]
    paired = catalog.get_check_definition("event_fact_analysis", "11")
    assert paired["definition"]["evaluator_version"] == "10"
    assert paired["settings"]["fixed"]["matching"]["z_caliper"] == .25
    assert not paired["eligibility"]["execution_authority"]
    mutated = paired["examples"][0]["request"]
    mutated["outcomes"]["forward_risk"]["z_caliper"] = .5
    with pytest.raises(ValueError, match="forward_risk_invalid"):
        normalize_check_preparation_request(mutated)
    assert "z_caliper" not in catalog.get_check_definition("event_fact_analysis", "11")["examples"][0]["request"]["outcomes"]["forward_risk"]


def test_discovered_example_reaches_existing_prepare_without_execution(monkeypatch):
    api = client()
    detail = api.get("/api/research/checks/definitions/event_fact_analysis/11").json()
    request = deepcopy(detail["examples"][0]["request"])
    request["scope"]["instrument_id"] = "authorized-instrument"
    request["scope"]["indicator_id"] = "authorized-candle-stats"
    request["preparation"] = {"freeze": False}
    observed = {}

    def plan(definition, normalized, **kwargs):
        observed["definition"] = definition
        observed["request"] = normalized
        observed["plan_options"] = kwargs
        return _plan()

    def prepare(**kwargs):
        observed["prepare"] = kwargs
        return {"status": "ready", "dataset": None, "unresolved_requirements": []}

    monkeypatch.setattr(service, "plan_research_check", plan)
    monkeypatch.setattr(service, "prepare_frozen_dataset_from_requirements", prepare)
    response = api.post("/api/research/checks/prepare", json=request)
    assert response.status_code == 200
    assert observed["definition"].definition_version.startswith("11+")
    assert observed["request"].scope["indicator_id"] == "authorized-candle-stats"
    assert observed["plan_options"]["require_durable_sources"] is True
    assert observed["prepare"]["freeze"] is False
    assert response.json()["check_executed"] is False
    assert response.json()["provider_call_performed"] is False
    request["outcomes"]["forward_risk"]["z_caliper"] = .5
    observed.clear()
    rejected = api.post("/api/research/checks/prepare", json=request)
    assert rejected.status_code == 400
    assert "forward_risk_invalid" in rejected.json()["detail"]
    assert observed == {}


def test_descriptive_metadata_covers_settings_and_analytical_results():
    from portal.backend.service.research import checks, event_fact_evaluator

    for item in catalog.list_check_definitions()["items"]:
        d = item["definition"]
        detail = catalog.get_check_definition(d["definition_id"], d["definition_version"])
        if detail["eligibility"]["request_selectable"]:
            settings = detail["settings"]["configurable"]
            assert "scope" in settings
            assert detail["result_shape"]["analytical_fields"]
            assert "accepted by" not in str(settings)
    typed = catalog.get_check_definition("event_fact_analysis", "4")["settings"]["configurable"]
    assert typed["statistics.features"]["baseline_operators"] == sorted(event_fact_evaluator._BASELINE_OPERATORS)
    assert typed["statistics.features"]["enriched_operators"] == sorted(event_fact_evaluator._FACT_OPERATORS | event_fact_evaluator._STRUCTURED_FACT_OPERATORS)
    assert "strictly between 0 and 1" in typed["statistics.bootstrap"]["constraints"]
    raw = catalog.get_check_definition("raw_forward_outcome", "2")
    assert raw["settings"]["configurable"]["detector"]["field_choices"] == sorted(checks._RAW_DETECTOR_FIELDS)
    assert raw["settings"]["configurable"]["outcomes"]["defaults"]["forward_bars"] == checks._forward_bars({})
    assert catalog.get_check_definition("signal_audit", "2")["result_shape"]["payload_schema"] == checks.SIGNAL_AUDIT_SCHEMA_VERSION
    assert catalog.get_check_definition("candidate_lifecycle", "2")["result_shape"]["payload_schema"] == checks.CANDIDATE_LIFECYCLE_SCHEMA_VERSION


def test_paired_metadata_explains_real_support_missingness_and_units():
    from tests.test_portal.test_crossing_state_comparison import fixture, finish
    from portal.backend.service.research import crossing_state_comparison

    detail = catalog.get_check_definition("event_fact_analysis", "11")
    fields = detail["result_shape"]["analytical_fields"]["crossing_state_comparison"]["fields"]
    data = fixture()
    actual = finish(data, crossing_state_comparison.prepare_pairs(**data))
    thresholds = fields["support_gates"]["thresholds"]
    assert set(thresholds) == set(actual["support_gates"])
    assert {k: v.get("minimum", v.get("maximum")) for k, v in thresholds.items()} == {
        "overall_matching": .70, "every_month_matching": .50, "outcome_completeness": .95,
        "complete_pairs": 200, "every_month_pairs": 10, "current_z_balance": .10, "log_prior_rms_balance": .10,
    }
    assert actual["support_gates"]["complete_pairs"] == (actual["primary"]["pair_count"] >= thresholds["complete_pairs"]["minimum"])
    assert actual["support_gates"]["outcome_completeness"] == (actual["outcome_missingness"]["complete_fraction"] >= thresholds["outcome_completeness"]["minimum"])
    assert fields["joint_nonoverlap"]["support"] == {"minimum_pairs": 50, "minimum_months": 6}
    assert "squared" in fields["primary"]["fields"]["raw_risk_contrast"]["unit"]
    assert fields["primary"]["fields"]["ratio_contrast"]["unit"] == "dimensionless"
    assert "without rematching" in fields["outcome_missingness"]["fields"]["rematched"]
    assert "not effective sample size" in fields["dependence"]["meaning"]
    assert "unmatched_crossing_identities[]" in fields["matching"]["fields"]
    assert "leave_one_day_out[]/leave_one_week_out[]" in fields


def test_catalogue_assertion_missing_metric_matches_existing_semantics():
    from research_science.check import ScalarAssertionSpec, evaluate_scalar_assertions

    constraint = catalog.get_check_definition("event_fact_analysis", "11")["settings"]["configurable"]["assertions[]"]["constraints"]
    result = evaluate_scalar_assertions({}, [ScalarAssertionSpec("absent", "gt", 0)])
    assert result["assertions"][0]["status"] == "indeterminate"
    assert "indeterminate" in constraint
