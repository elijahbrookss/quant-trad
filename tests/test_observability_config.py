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
    assert (
        "${QT_DOCKER_STORAGE_ROOT:-/var/lib/docker}:/host-docker:ro"
        in service["volumes"]
    )
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


def test_collector_reconnect_storm_rule_is_bounded_sustained_and_actionable():
    provisioning = yaml.safe_load(
        (
            ROOT
            / "docker/grafana/provisioning/alerting/collector-safety.yml"
        ).read_text(encoding="utf-8")
    )
    rules = provisioning["groups"][0]["rules"]
    rule = next(item for item in rules if item["uid"] == "qt-collector-reconnect-storm")

    query = next(item for item in rule["data"] if item["refId"] == "A")
    threshold = next(item for item in rule["data"] if item["refId"] == "C")
    sql = query["model"]["rawSql"]

    assert query["model"]["format"] == "table"
    assert "count(DISTINCT (session_id, connection_epoch))" in sql
    assert "event_type = 'provider_disconnected'" in sql
    assert "now() - interval '15 minutes'" in sql
    assert "GROUP BY definition_id" in sql
    assert threshold["model"]["conditions"][0]["evaluator"] == {
        "params": [5],
        "type": "gt",
    }
    assert rule["for"] == "5m"
    assert rule["noDataState"] == "OK"
    assert rule["execErrState"] == "Alerting"
    assert rule["labels"] == {
        "component": "market-data-collector",
        "owner": "qt-infra",
        "severity": "warning",
    }
    assert {"summary", "first_action", "recovery"} <= set(rule["annotations"])
