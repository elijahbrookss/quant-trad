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
