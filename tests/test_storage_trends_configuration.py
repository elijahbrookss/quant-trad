"""Storage graphs cannot silently activate a privileged Grafana datasource."""
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
TARGET = "/etc/grafana/provisioning/datasources/storage-metrics.yml"
DEFAULT = "${QT_GRAFANA_STORAGE_DATASOURCE_FILE:-./grafana/provisioning/datasources/storage-metrics.yml}"


@pytest.mark.parametrize("name", ["docker-compose.yml", "docker-compose.server.yml"])
def test_storage_metrics_mount_requires_explicit_configuration(name):
    services = yaml.safe_load((ROOT / "docker" / name).read_text())["services"]
    volumes = services["grafana"]["volumes"]
    selected = [v for v in volumes if isinstance(v, dict) and v.get("target") == TARGET]
    assert len(selected) == 1
    assert selected[0] == {"type": "bind", "source": DEFAULT, "target": TARGET,
                           "read_only": True, "bind": {"create_host_path": False}}
    inactive = ROOT / "docker/grafana/provisioning/datasources/storage-metrics.yml"
    assert yaml.safe_load(inactive.read_text()) == {"apiVersion": 1, "datasources": []}
    active = ROOT / "docker/grafana/storage-metrics/enabled.yml"
    assert active.is_file()
    assert "provisioning" not in active.relative_to(ROOT / "docker/grafana").parts


@pytest.mark.parametrize("name", ["docker-compose.yml", "docker-compose.server.yml"])
def test_default_capacity_dashboard_remains_the_existing_working_dashboard(name):
    services = yaml.safe_load((ROOT / "docker" / name).read_text())["services"]
    target = "/etc/grafana/provisioning/dashboards/capacity-database-growth.json"
    mounts = [v for v in services["grafana"]["volumes"]
              if isinstance(v, dict) and v.get("target") == target]
    assert mounts == [{"type": "bind",
        "source": "${QT_GRAFANA_STORAGE_DASHBOARD_FILE:-./grafana/provisioning/dashboards/capacity-database-growth.json}",
        "target": target, "read_only": True, "bind": {"create_host_path": False}}]
    assert (ROOT / "docker/grafana/storage-metrics/capacity-database-growth.json").is_file()


@pytest.mark.parametrize("name", ["docker-compose.yml", "docker-compose.server.yml"])
def test_metrics_credentials_are_loaded_only_by_grafana(name):
    services = yaml.safe_load((ROOT / "docker" / name).read_text())["services"]
    expected = {"path": "${QT_GRAFANA_STORAGE_ENV_FILE:-../grafana-storage-metrics.env}",
                "required": False}
    assert expected in services["grafana"]["env_file"]
    for service_name, service in services.items():
        if service_name != "grafana":
            assert expected not in service.get("env_file", [])
