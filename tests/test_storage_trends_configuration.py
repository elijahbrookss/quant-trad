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
    expected = {"path": "${QT_GRAFANA_STORAGE_ENV_FILE:-../../grafana-storage-metrics.env}",
                "required": False}
    assert expected in services["grafana"]["env_file"]
    for service_name, service in services.items():
        if service_name != "grafana":
            assert expected not in service.get("env_file", [])


def test_metrics_secret_default_stays_outside_checkout_and_build_context():
    # Standard sibling deployment layout; no real private file is opened.
    relative = Path("../../grafana-storage-metrics.env")
    assert (ROOT / "docker" / relative).resolve().parent == ROOT.parent
    for name in (".gitignore", ".dockerignore"):
        assert "/grafana-storage-metrics.env" in (ROOT / name).read_text().splitlines()


ACTIVATION_KEYS = (
    "QT_GRAFANA_STORAGE_DATASOURCE_FILE",
    "QT_GRAFANA_STORAGE_DASHBOARD_FILE",
    "QT_STORAGE_GRAFANA_DASHBOARD_URL",
)


def _isolated_activation_environment(tmp_path, monkeypatch):
    for key in (*ACTIVATION_KEYS, "QT_STORAGE_METRICS_DB_PASSWORD", "UNRELATED_PRIVATE_SETTING"):
        monkeypatch.delenv(key, raising=False)
    shared = tmp_path / "shared.env"
    shared.write_text("POSTGRES_USER=synthetic_app\nPOSTGRES_PASSWORD=synthetic_app_password\nPOSTGRES_DB=synthetic_metrics\n")
    private = tmp_path / "grafana-storage-metrics.env"
    return shared, private, {
        "QT_SINGLE_NODE_ENV_FILE": str(shared), "QT_SERVER_ENV_FILE": str(shared),
        "QT_GRAFANA_STORAGE_ENV_FILE": str(private),
        "QT_SINGLE_NODE_COMPOSE_FILE": str(ROOT / "docker/docker-compose.server.yml"),
        "QT_RELEASE_REVISION": "a" * 40, "QT_SOURCE_TREE_HASH": "b" * 64,
        "SOURCE_ROOT": str(ROOT),
    }


def test_private_settings_export_only_the_reviewed_public_keys(tmp_path, monkeypatch):
    from tests.test_server_promotion import shell
    shared, private, environment = _isolated_activation_environment(tmp_path, monkeypatch)
    before = shared.read_bytes()
    marker = tmp_path / "must-not-execute"
    private.write_text(
        "QT_GRAFANA_STORAGE_DATASOURCE_FILE=/synthetic/enabled.yml\n"
        "QT_GRAFANA_STORAGE_DASHBOARD_FILE=/synthetic/dashboard.json\n"
        "QT_STORAGE_GRAFANA_DASHBOARD_URL=https://grafana.example.invalid/d/capacity\n"
        f"QT_STORAGE_METRICS_DB_PASSWORD=$(touch {marker})\n"
        "UNRELATED_PRIVATE_SETTING=must-not-load\n"
    )
    result = shell(tmp_path, r"""
test -z "${QT_STORAGE_METRICS_DB_PASSWORD+x}"
test -z "${UNRELATED_PRIVATE_SETTING+x}"
printf '%s\n' "$QT_GRAFANA_STORAGE_DATASOURCE_FILE" "$QT_GRAFANA_STORAGE_DASHBOARD_FILE" "$QT_STORAGE_GRAFANA_DASHBOARD_URL"
""", env=environment)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["/synthetic/enabled.yml", "/synthetic/dashboard.json",
        "https://grafana.example.invalid/d/capacity"]
    assert not marker.exists()
    assert shared.read_bytes() == before


def test_explicit_empty_link_overrides_private_file_without_loading_credentials(tmp_path, monkeypatch):
    from tests.test_server_promotion import shell
    _, private, environment = _isolated_activation_environment(tmp_path, monkeypatch)
    private.write_text("QT_STORAGE_GRAFANA_DASHBOARD_URL=https://grafana.example.invalid/d/capacity\n"
                       "QT_STORAGE_METRICS_DB_PASSWORD=synthetic_private_password\n")
    result = shell(tmp_path, r"""
test -z "$QT_STORAGE_GRAFANA_DASHBOARD_URL"
test -z "${QT_STORAGE_METRICS_DB_PASSWORD+x}"
echo EXPLICIT_DISABLE_PRESERVED
""", env={**environment, "QT_STORAGE_GRAFANA_DASHBOARD_URL": ""})
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "EXPLICIT_DISABLE_PRESERVED"


def test_private_activation_keeps_resolved_postgres_configuration_unchanged(tmp_path, monkeypatch):
    import os
    import json
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        pytest.skip("real Compose rendering is qualified on the disposable GitHub host; Docker Desktop stays unused")
    from tests.test_server_promotion import shell
    shared, private, environment = _isolated_activation_environment(tmp_path, monkeypatch)
    with shared.open("a") as stream:
        stream.write("QT_STORAGE_DATABASE_IMAGE=sha256:" + "c" * 64 + "\n"
            "QT_ARCHIVE_SHARED_GROUP_ID=1000\nQT_DOCKER_SOCKET_GID=999\n"
            "QT_MARKET_DATA_EXPECTED_UUID=00000000-0000-0000-0000-000000000001\n"
            "QT_MARKET_DATA_WORKING_EXPECTED_UUID=00000000-0000-0000-0000-000000000002\n"
            "QT_MARKET_DATA_ROOT=/synthetic/history/archives\nQT_MARKET_DATA_WORKING_ROOT=/synthetic/working\n"
            "QT_STORAGE_HDD_ROOT=/synthetic/history\nQT_STORAGE_INVENTORY_HOST_PATH=/synthetic/inventory.json\n"
            "QT_STORAGE_MAINTENANCE_LIMITS_HOST_PATH=/synthetic/maintenance.json\n"
            "QT_STORAGE_RECOVERY_SECRETS_ROOT=/synthetic/recovery\nQT_STORAGE_NETWORK=synthetic_network\n"
            "QT_STORAGE_POSTGRES_VOLUME=synthetic_pgdata\nQT_STORAGE_RECOVERY_SOCKET_VOLUME=synthetic_socket\n")
    before_shared = shared.read_bytes()
    body = r"""
repo_root="$SOURCE_ROOT"
recorded_storage_layout() { printf ssd-hdd-v1; }
compose config --format json
"""
    before = shell(tmp_path, body, env=environment)
    assert before.returncode == 0, before.stderr
    private.write_text(
        f"QT_GRAFANA_STORAGE_DATASOURCE_FILE={ROOT}/docker/grafana/storage-metrics/enabled.yml\n"
        f"QT_GRAFANA_STORAGE_DASHBOARD_FILE={ROOT}/docker/grafana/storage-metrics/capacity-database-growth.json\n"
        "QT_STORAGE_GRAFANA_DASHBOARD_URL=https://grafana.example.invalid/d/capacity\n"
        "QT_STORAGE_METRICS_DB_URL=tsdb:5432\nQT_STORAGE_METRICS_DB_PASSWORD=synthetic_private_password\n"
    )
    private.chmod(0o600)
    after = shell(tmp_path, body, env=environment)
    assert after.returncode == 0, after.stderr
    old, new = json.loads(before.stdout), json.loads(after.stdout)
    assert old["services"]["tsdb"] == new["services"]["tsdb"]
    assert new["services"]["backend"]["environment"]["QT_STORAGE_GRAFANA_DASHBOARD_URL"] == "https://grafana.example.invalid/d/capacity"
    for name, service in new["services"].items():
        if name != "grafana":
            assert "QT_STORAGE_METRICS_DB_PASSWORD" not in service.get("environment", {})
    assert new["services"]["grafana"]["environment"]["QT_STORAGE_METRICS_DB_PASSWORD"] == "synthetic_private_password"
    assert shared.read_bytes() == before_shared
