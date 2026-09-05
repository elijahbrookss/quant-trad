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
ALERT_COMPOSE_PATH = ROOT / "docker/docker-compose.alert-email.yml"
ALERT_PROVISIONING_PATH = ROOT / "docker/grafana/server-alerting/operator-email.yml"
ALERT_CLEANUP_PROVISIONING_PATH = (
    ROOT
    / "docker/grafana/server-alerting/cleanup-provisioning/alerting/operator-email-cleanup.yml"
)
BOOK_ROLLUP_MIGRATION_PATH = (
    ROOT / "scripts/db/manual_migration_book_operational_rollups_v1.sql"
)


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


def test_archive_mounts_never_create_fallback_paths_and_expose_only_udev_metadata():
    services = _server_compose()["services"]
    for name in ("backend", "initialize", "market-data-collector"):
        service = services[name]
        assert service["environment"]["QT_MARKET_DATA_EXPECTED_UUID"] == "${QT_MARKET_DATA_EXPECTED_UUID:-}"
        assert service["environment"]["QT_STORAGE_UDEV_ROOT"] == "/run/qt-host-udev/data"
        volumes = {v["target"]: v for v in service["volumes"] if isinstance(v, dict)}
        assert volumes["/app/logs/market-structure"]["bind"]["create_host_path"] is False
        assert volumes["/run/qt-host-udev"]["source"] == "/run/udev"
        assert volumes["/run/qt-host-udev"]["read_only"] is True
        assert volumes["/run/qt-host-udev"]["bind"]["create_host_path"] is False
        assert not service.get("privileged", False)
        assert "devices" not in service


def test_archive_writers_admit_storage_before_starting_workers():
    for source, next_action in (
        ("portal/backend/run_backend.py", "signal.signal("),
        ("portal/backend/workers/market_data_collector.py", "signal.signal("),
        ("portal/backend/workers/single_node_initializer.py", "print("),
    ):
        main = (ROOT / source).read_text().split("def main() -> int:", 1)[1]
        assert main.index("require_configured_archive_mount()") < main.index(next_action)


def test_deploy_rechecks_mount_after_build_before_replacing_services():
    deploy = (ROOT / "scripts/automation/server_deploy.sh").read_text()
    assert "validate-storage)" in deploy
    after_build = deploy.split("  build_release_images\n", 1)[1]
    assert after_build.index("validate_storage_root") < after_build.index("compose up")


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


def test_server_postgres_has_explicit_isolated_shared_memory_headroom():
    tsdb = _server_compose()["services"]["tsdb"]

    assert tsdb["shm_size"] == "1g"
    assert "ipc" not in tsdb


def test_book_rollup_migration_disables_query_and_maintenance_parallelism():
    migration = BOOK_ROLLUP_MIGRATION_PATH.read_text(encoding="utf-8")

    assert "SET LOCAL max_parallel_workers_per_gather = 0;" in migration
    assert "SET LOCAL max_parallel_maintenance_workers = 0;" in migration
    for exact_trigger_guard in (
        "trigger.tgconstraint = 0",
        "trigger.tgnargs = 0",
        "trigger.tgqual IS NULL",
        "trigger.tgoldtable IS NULL",
        "trigger.tgnewtable IS NULL",
        "pg_get_triggerdef(trigger.oid, false)",
        "procedure.prorettype = 'trigger'::regtype",
        "procedure.proparallel = 'u'",
        "NOT procedure.prosecdef",
        "procedure.proconfig IS NULL",
        "btrim(",
        "procedure.prosrc",
        "'[[:space:]]+'",
        "has_function_privilege",
    ):
        assert exact_trigger_guard in migration


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


def test_server_email_alerting_is_an_optional_native_grafana_overlay():
    base = _server_compose()["services"]["grafana"]
    overlay = yaml.safe_load(ALERT_COMPOSE_PATH.read_text(encoding="utf-8"))[
        "services"
    ]["grafana"]
    environment = overlay["environment"]

    base_volume_targets = {
        volume["target"]
        for volume in base["volumes"]
        if isinstance(volume, dict)
    }
    assert "/etc/grafana/provisioning" not in base_volume_targets
    assert {
        "/etc/grafana/provisioning/datasources",
        "/etc/grafana/provisioning/dashboards",
        "/etc/grafana/provisioning/alerting/collector-safety.yml",
        "/etc/grafana/provisioning/alerting/platform-safety.yml",
    }.issubset(base_volume_targets)
    for volume in base["volumes"]:
        if isinstance(volume, dict):
            assert volume["bind"]["create_host_path"] is False
    assert not any("operator-email.yml" in str(volume) for volume in base["volumes"])
    assert environment["GF_SMTP_ENABLED"] == "true"
    assert "QT_ALERT_SMTP_HOST" in environment["GF_SMTP_HOST"]
    assert "QT_ALERT_SMTP_PASSWORD" in environment["GF_SMTP_PASSWORD"]
    assert environment["GF_SMTP_SKIP_VERIFY"] == "false"
    assert environment["GF_SMTP_STARTTLS_POLICY"] == "MandatoryStartTLS"
    assert overlay["volumes"][0]["target"].endswith("/operator-email.yml")


def test_server_email_contact_point_and_policy_use_operator_contract():
    alerting = yaml.safe_load(ALERT_PROVISIONING_PATH.read_text(encoding="utf-8"))
    contact_point = alerting["contactPoints"][0]
    receiver = contact_point["receivers"][0]
    policy = alerting["policies"][0]

    assert contact_point["name"] == "qt-operator-email"
    assert receiver["uid"] == "qt-operator-email"
    assert receiver["type"] == "email"
    assert receiver["settings"]["addresses"] == "$QT_ALERT_EMAILS"
    assert receiver["settings"]["singleEmail"] is False
    assert receiver["disableResolveMessage"] is False
    assert policy["receiver"] == "qt-operator-email"
    assert policy["group_by"] == ["grafana_folder", "alertname", "severity"]
    assert policy["group_wait"] == "30s"
    assert policy["repeat_interval"] == "4h"


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
    assert "validate-alerts)" in deploy
    assert "validate_alerting_environment" in deploy
    assert 'compose_file_args+=(--file "$alerting_compose_file")' in deploy
    assert "QT_ALERT_SMTP_PASSWORD" in deploy
    assert "apply-alerts)" in deploy
    assert "--no-deps --force-recreate --wait" in deploy
    assert 'deployed_revision="$(state_value current_revision)"' in deploy
    assert "does not match deployed revision" in deploy
    assert "preview-alerts)" in deploy
    assert "restore-alerts)" in deploy
    assert 'alert_preview_state_file="$state_root/alert-preview.env"' in deploy
    assert "preview must run from a separate worktree" in deploy
    assert "Production release remains recorded at" in deploy
    assert "require_no_alert_preview" in deploy
    assert "Operator email alerting: enabled" in deploy
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
    assert values["QT_ALERTS_ENABLED"] == "false"
    assert values["QT_ALERT_EMAILS"] == ""
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


def _validate_alert_environment(tmp_path: Path, values: dict[str, str]):
    tmp_path.mkdir(parents=True, exist_ok=True)
    target = tmp_path / "secrets.env"
    target.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )
    target.chmod(0o600)
    return subprocess.run(
        [
            "bash",
            str(ROOT / "scripts/automation/server_deploy.sh"),
            "validate-alerts",
        ],
        cwd=ROOT,
        env={**os.environ, "QT_SINGLE_NODE_ENV_FILE": str(target)},
        check=False,
        capture_output=True,
        text=True,
    )


def test_alert_validation_accepts_disabled_single_and_multiple_recipient_modes(
    tmp_path,
):
    disabled = _validate_alert_environment(
        tmp_path / "disabled",
        {"QT_ALERTS_ENABLED": "false"},
    )
    assert disabled.returncode == 0, disabled.stderr

    base = {
        "QT_ALERTS_ENABLED": "true",
        "QT_ALERT_SMTP_HOST": "smtp.provider.test:587",
        "QT_ALERT_SMTP_USER": "token-user",
        "QT_ALERT_SMTP_PASSWORD": "provider-secret",
        "QT_ALERT_EMAIL_FROM": "alerts@quanttrad.test",
    }
    for name, recipients in (
        ("single", "owner@quanttrad.test"),
        ("multiple", "owner@quanttrad.test,backup@quanttrad.test"),
    ):
        result = _validate_alert_environment(
            tmp_path / name,
            {**base, "QT_ALERT_EMAILS": recipients},
        )
        assert result.returncode == 0, result.stderr
        assert "provider-secret" not in result.stdout
        assert "provider-secret" not in result.stderr


def test_alert_validation_rejects_incomplete_or_invalid_enabled_configuration(
    tmp_path,
):
    valid = {
        "QT_ALERTS_ENABLED": "true",
        "QT_ALERT_EMAILS": "owner@quanttrad.test",
        "QT_ALERT_SMTP_HOST": "smtp.provider.test:587",
        "QT_ALERT_SMTP_USER": "token-user",
        "QT_ALERT_SMTP_PASSWORD": "provider-secret",
        "QT_ALERT_EMAIL_FROM": "alerts@quanttrad.test",
    }
    cases = {
        "missing-recipient": {**valid, "QT_ALERT_EMAILS": ""},
        "invalid-recipient": {**valid, "QT_ALERT_EMAILS": "not-an-email"},
        "missing-secret": {**valid, "QT_ALERT_SMTP_PASSWORD": ""},
        "invalid-host": {**valid, "QT_ALERT_SMTP_HOST": "smtp.provider.test"},
        "invalid-port": {**valid, "QT_ALERT_SMTP_HOST": "smtp.provider.test:70000"},
        "duplicate": {
            **valid,
            "QT_ALERT_EMAILS": "owner@quanttrad.test,owner@quanttrad.test",
        },
    }
    for name, values in cases.items():
        result = _validate_alert_environment(tmp_path / name, values)
        assert result.returncode != 0, name
        assert "provider-secret" not in result.stdout
        assert "provider-secret" not in result.stderr


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


def test_email_alerting_integration_stack_is_isolated_and_pinned() -> None:
    compose_text = (ROOT / "docker" / "test" / "grafana-email-alerting.compose.yml").read_text()
    cleanup_compose_text = (
        ROOT / "docker" / "test" / "grafana-email-alerting-cleanup.compose.yml"
    ).read_text()
    rule_text = (ROOT / "docker" / "test" / "grafana-email-test-rule.yml").read_text()

    assert "ghcr.io/axllent/mailpit:v1.30.6@sha256:" in compose_text
    assert "grafana/grafana:13.2.0" in compose_text
    assert "127.0.0.1:${QT_ALERT_TEST_MAILPIT_PORT" in compose_text
    assert "QT_ALERT_EMAILS: owner@quanttrad.test,backup@quanttrad.test" in compose_text
    assert "grafana-email-test-rule.yml" in compose_text
    assert "operator-email-cleanup.yml" in cleanup_compose_text
    assert "grafana-data:/var/lib/grafana" in compose_text
    assert "grafana-data:/var/lib/grafana" in cleanup_compose_text
    assert "expression: 1 == 1" in rule_text
    assert "severity: test" in rule_text


def test_email_alerting_integration_script_proves_both_recipients() -> None:
    script_text = (ROOT / "scripts" / "ci" / "test_grafana_email_alerting.sh").read_text()
    workflow_text = (ROOT / ".github" / "workflows" / "test.yaml").read_text()

    assert "/api/v1/messages" in script_text
    assert "owner@quanttrad.test" in script_text
    assert "backup@quanttrad.test" in script_text
    assert "/api/v1/provisioning/contact-points" in script_text
    assert "/api/v1/provisioning/policies" in script_text
    assert "alerting cleanup proof" in script_text
    assert "down --volumes --remove-orphans" in script_text
    assert "bash scripts/ci/test_grafana_email_alerting.sh" in workflow_text


def test_disposable_email_rule_is_not_in_production_provisioning() -> None:
    base_compose = (ROOT / "docker" / "docker-compose.server.yml").read_text()
    alert_overlay = (ROOT / "docker" / "docker-compose.alert-email.yml").read_text()

    assert "grafana-email-test-rule" not in base_compose
    assert "grafana-email-test-rule" not in alert_overlay


def test_alert_preview_is_grafana_only_reversible_and_fail_closed() -> None:
    deploy = (ROOT / "scripts" / "automation" / "server_deploy.sh").read_text()
    preview_body = deploy.split("preview_alerting_configuration() {", 1)[1].split(
        "restore_alerting_preview() {", 1
    )[0]
    restore_body = deploy.split("restore_alerting_preview() {", 1)[1].split(
        "deploy_release() {", 1
    )[0]
    recreate_body = deploy.split("recreate_grafana_from_repo() {", 1)[1].split(
        "restore_grafana_to_base() {", 1
    )[0]

    assert 'test "$alerts_enabled" = "true"' in preview_body
    assert 'test "$deployed_revision" != "$QT_RELEASE_REVISION"' in preview_body
    assert "record_alert_preview" in preview_body
    assert 'recreate_grafana_from_repo "$repo_root"' in preview_body
    assert 'restore_grafana_to_base "$base_root"' in preview_body
    assert (
        'if ! compose_from_repo_root "$source_root" "$extra_compose_file" up'
        in recreate_body
    )
    assert "return 1" in recreate_body
    assert "backend" not in preview_body
    assert "market-data-collector" not in preview_body
    assert 'test "$current_revision" = "$base_revision"' in restore_body
    assert 'restore_grafana_to_base "$base_root"' in restore_body
    assert 'rm -f -- "$alert_preview_state_file"' in restore_body


def test_alert_preview_cleanup_deletes_preview_only_grafana_resources() -> None:
    cleanup = yaml.safe_load(ALERT_CLEANUP_PROVISIONING_PATH.read_text())
    assert cleanup["resetPolicies"] == [1]
    assert cleanup["deleteContactPoints"] == [
        {"orgId": 1, "uid": "qt-operator-email"}
    ]
    assert cleanup["deleteRules"] == [
        {"orgId": 1, "uid": "qt-database-unavailable"},
        {"orgId": 1, "uid": "qt-collector-reconnect-storm"},
    ]


def test_alert_preview_cleanup_replaces_legacy_provisioning_root_mount() -> None:
    cleanup_overlay = yaml.safe_load(
        (ROOT / "docker/docker-compose.alert-preview-cleanup.yml").read_text()
    )["services"]["grafana"]
    cleanup_volume = cleanup_overlay["volumes"][0]

    assert "QT_ALERT_CLEANUP_PROVISIONING_ROOT" in cleanup_volume["source"]
    assert cleanup_volume["target"] == "/etc/grafana/provisioning"
    assert cleanup_volume["read_only"] is True
    assert cleanup_volume["bind"]["create_host_path"] is False

    deploy = (ROOT / "scripts/automation/server_deploy.sh").read_text()
    assert 'QT_ALERT_CLEANUP_PROVISIONING_ROOT="$cleanup_provisioning_root"' in deploy
