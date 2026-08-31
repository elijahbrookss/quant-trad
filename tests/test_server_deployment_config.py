from __future__ import annotations

import os
from pathlib import Path
import re
import stat
import subprocess

import yaml


ROOT = Path(__file__).resolve().parents[1]
SERVER_COMPOSE_PATH = ROOT / "docker/docker-compose.server.yml"
IBKR_ENTRYPOINT_PATH = ROOT / "docker/ibkr-gateway/entrypoint.sh"


def _server_compose() -> dict:
    return yaml.safe_load(SERVER_COMPOSE_PATH.read_text(encoding="utf-8"))


def test_ibkr_gateway_has_no_committed_credential_defaults():
    entrypoint = IBKR_ENTRYPOINT_PATH.read_text(encoding="utf-8")

    assert re.search(r"^IbLoginId=$", entrypoint, re.MULTILINE)
    assert re.search(r"^IbPassword=$", entrypoint, re.MULTILINE)
    assert "IBC_TWS_USERNAME and IBC_TWS_PASSWORD are required" in entrypoint
    assert 'set_ini_value "IbLoginId" "${IBC_TWS_USERNAME}"' in entrypoint
    assert 'set_ini_value "IbPassword" "${IBC_TWS_PASSWORD}"' in entrypoint


def test_server_compose_runs_full_stack_and_profiles_only_broker():
    compose = _server_compose()
    services = compose["services"]

    assert {
        "tsdb",
        "backend",
        "initialize",
        "market-data-collector",
        "frontend",
        "frontend-v2",
        "pgadmin",
        "loki",
        "alloy",
        "docker-events",
        "docker-stats",
        "grafana",
        "ibkr-gateway",
    } == set(services)

    assert services["ibkr-gateway"]["profiles"] == ["broker"]
    for name, service in services.items():
        if name != "ibkr-gateway":
            assert "profiles" not in service

    assert services["frontend"]["build"]["target"] == "prod"
    assert services["frontend-v2"]["build"]["target"] == "prod-v2"
    assert services["initialize"]["restart"] == "no"
    assert services["initialize"]["depends_on"]["backend"]["condition"] == (
        "service_healthy"
    )
    for flag in (
        "QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA",
        "QT_SINGLE_NODE_ENABLE_SCHEDULED_FACTS",
        "QT_SINGLE_NODE_ENABLE_STRUCTURED_FACTS",
        "QT_SINGLE_NODE_ENABLE_TRADE_STREAMS",
        "QT_SINGLE_NODE_ENABLE_L2_STREAMS",
    ):
        assert services["initialize"]["environment"][flag] == "${" + flag + ":-true}"
    assert services["initialize"]["environment"][
        "QT_SINGLE_NODE_STRUCTURED_FACT_MANIFESTS"
    ].endswith(
        "/config/market-data/structured-facts/"
        "chainlink-nxtassets-btc-etp-reserves.json}"
    )
    assert services["market-data-collector"]["restart"] == "unless-stopped"
    assert services["market-data-collector"]["stop_grace_period"] == "5m"
    assert services["market-data-collector"]["depends_on"]["initialize"][
        "condition"
    ] == "service_completed_successfully"
    assert services["market-data-collector"]["healthcheck"]["test"][-1] == (
        "portal.backend.workers.market_data_collector_health"
    )


def test_server_compose_is_a_portable_single_node_project():
    compose_text = SERVER_COMPOSE_PATH.read_text(encoding="utf-8")

    assert "name: ${QT_COMPOSE_PROJECT_NAME:-quant-trad-single-node}" in compose_text
    assert "${QT_COMPOSE_PROJECT_NAME:-quant-trad-single-node}_quanttrad" in compose_text


def test_server_ports_are_private_and_database_is_not_published():
    services = _server_compose()["services"]

    assert "ports" not in services["tsdb"]
    assert "ports" not in services["market-data-collector"]
    assert "ports" not in services["loki"]

    published = {
        name: service["ports"]
        for name, service in services.items()
        if "ports" in service
    }
    for ports in published.values():
        assert all("127.0.0.1" in value for value in ports)


def test_server_images_are_pinned_and_application_images_are_attested():
    services = _server_compose()["services"]

    for service in services.values():
        image = service["image"]
        assert not image.endswith(":latest")

    assert services["tsdb"]["image"] == "quanttrad-postgres:2.14.2-pg15"
    assert "QT_RELEASE_REVISION" in services["backend"]["image"]
    assert "QT_RELEASE_REVISION" in services["frontend"]["image"]
    assert "QT_RELEASE_REVISION" in services["frontend-v2"]["image"]
    assert "QT_SOURCE_TREE_HASH" in services["backend"]["build"]["args"]
    assert "QT_SOURCE_TREE_HASH" in services["frontend"]["build"]["args"]
    assert "requirements.lock" in (ROOT / "portal/backend/Dockerfile").read_text(
        encoding="utf-8"
    )


def test_server_docker_authority_is_explicit_and_telemetry_is_read_only():
    services = _server_compose()["services"]

    backend_socket = next(
        volume
        for volume in services["backend"]["volumes"]
        if isinstance(volume, dict)
        and volume.get("source") == "/var/run/docker.sock"
    )
    assert backend_socket["target"] == "/var/run/docker.sock"
    assert not backend_socket.get("read_only", False)

    for name in ("alloy", "docker-events", "docker-stats"):
        assert "/var/run/docker.sock:/var/run/docker.sock:ro" in services[name][
            "volumes"
        ]


def test_server_alloy_preserves_bounded_loki_routing_labels():
    compose = _server_compose()
    alloy = (ROOT / "docker/alloy/config.alloy").read_text(encoding="utf-8")

    tsdb = compose["services"]["tsdb"]
    assert "logging_collector=off" in tsdb["command"]
    assert "log_destination=stderr" in tsdb["command"]
    assert "postgres-logs" not in SERVER_COMPOSE_PATH.read_text(encoding="utf-8")
    assert tsdb["logging"]["options"] == {"max-size": "50m", "max-file": "5"}
    assert 'discovery.docker "quanttrad"' in alloy
    assert 'loki.source.docker "quanttrad"' in alloy
    assert "discovery.relabel.quanttrad_logs.rules" in alloy
    for label in ("job", "service", "runtime", "container", "compose_service"):
        assert f'target_label  = "{label}"' in alloy
    assert "run_id" not in alloy
    assert "bot_id" not in alloy


def test_server_frontends_proxy_api_and_fall_back_to_spa_entrypoint():
    nginx = (ROOT / "portal/frontend/nginx.conf").read_text(encoding="utf-8")

    assert "location /api/" in nginx
    assert "proxy_pass http://backend.quanttrad:8000;" in nginx
    assert "proxy_buffering off;" in nginx
    assert "try_files $uri $uri/ /index.html;" in nginx


def test_deploy_helper_never_runs_migrations_and_verifies_every_app_image():
    deploy = (ROOT / "scripts/automation/server_deploy.sh").read_text(
        encoding="utf-8"
    )

    assert "manual_migration" not in deploy
    assert "migrate" not in deploy
    assert "compose up --detach --remove-orphans --wait" in deploy
    assert "QT_IMAGE_SOURCE_TREE_HASH" in deploy
    assert "doctor)" in deploy
    assert "rollback)" in deploy
    assert "verify_initializer" in deploy
    assert "previous_revision" in deploy
    assert "credentials-coinbase)" in deploy
    assert "qt)" in deploy
    assert 'compose exec -T backend /app/scripts/qt "$@"' in deploy
    assert "will be enrolled without credentials" in deploy
    assert "credentials are optional" in deploy
    assert "load provider credentials before judging" not in deploy
    assert 'compose exec -T backend /app/scripts/qt setup provider coinbase "$@"' in deploy
    assert '$deployment_root/secrets.env' in deploy
    assert '$repo_root/secrets.env' not in deploy
    assert "env_value QT_REBUILD_DATABASE_IMAGE" in deploy
    for service in (
        "backend",
        "market-data-collector",
        "frontend",
        "frontend-v2",
    ):
        assert f"verify_release_image {service}" in deploy


def test_ci_builds_the_rendered_production_image_targets():
    workflow = (ROOT / ".github/workflows/test.yaml").read_text(encoding="utf-8")

    assert "Build production images with source attestation" in workflow
    assert "build tsdb backend frontend frontend-v2" in workflow
    assert "POSTGRES_HOST_AUTH_METHOD: trust" in workflow
    assert "POSTGRES_PASSWORD:" not in workflow
    assert "postgresql+psycopg2://quanttrad@127.0.0.1:5432/" in workflow


def test_deploy_helper_generates_private_operator_environment_once(tmp_path):
    target = tmp_path / "secrets.env"
    environment = {
        **os.environ,
        "QT_SINGLE_NODE_ENV_FILE": str(target),
    }
    command = [
        "bash",
        str(ROOT / "scripts/automation/server_deploy.sh"),
        "init-env",
    ]

    first = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert first.returncode == 0, first.stderr
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    values = dict(
        line.split("=", 1)
        for line in target.read_text(encoding="utf-8").splitlines()
    )
    assert re.fullmatch(r"[A-Za-z0-9_-]{24,}", values["POSTGRES_PASSWORD"])
    assert re.fullmatch(
        r"[A-Za-z0-9_-]{43}=",
        values["QT_SECURITY_PROVIDER_CREDENTIAL_KEY"],
    )
    assert values["PGADMIN_DEFAULT_EMAIL"] == "admin@quanttrad.dev"
    assert values["QT_SINGLE_NODE_ENABLE_L2_STREAMS"] == "true"
    assert values["QT_SINGLE_NODE_ENABLE_STRUCTURED_FACTS"] == "true"
    assert values["CHAINLINK_ARBITRUM_RPC_URL"].startswith("https://")
    assert not any(key.startswith("COINBASE_API_") for key in values)

    before = target.read_bytes()
    second = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert second.returncode != 0
    assert target.read_bytes() == before


def test_host_bootstrap_supports_a_configurable_single_node_root():
    bootstrap = (ROOT / "scripts/automation/server_host_bootstrap.sh").read_text(
        encoding="utf-8"
    )

    assert "QT_SINGLE_NODE_ROOT" in bootstrap
    assert '"$install_root/market-structure"' in bootstrap
    assert '"$install_root/deploy-state"' in bootstrap


def test_stream_definition_storage_does_not_whitelist_domain_channels():
    repository = (
        ROOT / "portal/backend/service/storage/repos/market_structure.py"
    ).read_text(encoding="utf-8")

    assert "supported ordered channels" not in repository
    assert "one to sixteen channels are required" in repository
