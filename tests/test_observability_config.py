from __future__ import annotations

import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_promtail_indexes_runtime_not_run_identity():
    text = (ROOT / "docker/promtail/config.yml").read_text(encoding="utf-8")

    assert "regex: quanttrad" in text
    assert "target_label: runtime" in text
    assert "target_label: run_id" not in text
    assert "target_label: bot_id" not in text


def test_loki_has_durable_local_volume():
    compose = yaml.safe_load((ROOT / "docker/docker-compose.yml").read_text(encoding="utf-8"))

    loki_volumes = set(compose["services"]["loki"]["volumes"])
    root_volumes = set(compose["volumes"])

    assert "loki-data:/loki" in loki_volumes
    assert "loki-data" in root_volumes


def test_dev_config_does_not_enable_direct_loki_handler():
    config = yaml.safe_load((ROOT / "config/dev.yaml").read_text(encoding="utf-8"))

    assert config["logging"]["loki_url"] is None


def test_docker_capacity_sampler_is_bounded_and_observable():
    compose = yaml.safe_load(
        (ROOT / "docker/docker-compose.yml").read_text(encoding="utf-8")
    )

    service = compose["services"]["docker-stats"]
    assert service["profiles"] == ["observability"]
    assert "/var/run/docker.sock:/var/run/docker.sock:ro" in service["volumes"]
    assert "/var/lib/docker:/host-docker:ro" in service["volumes"]
    assert service["environment"]["QT_DOCKER_CAPACITY_INTERVAL_SECONDS"].endswith(
        ":-15}"
    )
    assert service["labels"]["loki.service"] == "docker-stats"


def test_capacity_dashboard_has_schema_and_relation_drilldown():
    dashboard = json.loads(
        (
            ROOT
            / "docker/grafana/provisioning/dashboards/capacity-database-growth.json"
        ).read_text(encoding="utf-8")
    )

    assert dashboard["uid"] == "quanttrad-capacity-growth"
    variables = {
        variable["name"]: variable for variable in dashboard["templating"]["list"]
    }
    assert {"schema", "relation", "container"} <= set(variables)
    assert "schema_name" in variables["schema"]["query"]
    assert "relation_name" in variables["relation"]["query"]
    assert dashboard["refresh"] == "15s"
    assert any(panel["title"] == "Relation size history" for panel in dashboard["panels"])
    assert any(panel["title"] == "Relation growth over selected range" for panel in dashboard["panels"])
