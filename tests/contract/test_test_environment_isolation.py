from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import yaml


_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_FILE = _ROOT / "docker" / "docker-compose.test.yml"
_RUNNER = _ROOT / "scripts" / "ci" / "run_test_suite.sh"


def test_database_compose_route_has_no_checkout_secret_or_host_boundary() -> None:
    payload = yaml.safe_load(_COMPOSE_FILE.read_text(encoding="utf-8"))
    test_service = payload["services"]["test"]
    database_service = payload["services"]["timescaledb"]

    assert "env_file" not in test_service
    assert "env_file" not in database_service
    assert "volumes" not in test_service
    assert "ports" not in database_service
    assert database_service["image"] == "timescale/timescaledb:2.14.2-pg15"
    assert payload["networks"]["default"]["internal"] is True

    expected_database_environment = {
        "POSTGRES_USER": "${QT_TEST_POSTGRES_USER:?required}",
        "POSTGRES_PASSWORD": "${QT_TEST_POSTGRES_PASSWORD:?required}",
        "POSTGRES_DB": "${QT_TEST_POSTGRES_DB:?required}",
    }
    assert database_service["environment"] == expected_database_environment
    assert test_service["environment"] == {
        **expected_database_environment,
        "QT_DISABLE_DOTENV": "1",
        "QT_LOGGING_LOKI_URL": "",
        "QT_LOGGING_DEBUG": "false",
    }

    dockerignore = (_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in dockerignore
    assert ".env.*" in dockerignore
    assert "secrets.env" in dockerignore


def test_database_runner_uses_unique_identity_and_cleans_successful_and_failed_runs(
    tmp_path: Path,
) -> None:
    docker_log = tmp_path / "docker.log"
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "args=\"$*\"\n"
        "args=\"${args//$'\\n'/ }\"\n"
        "printf '%s\\t%s\\t%s\\t%s\\n' \"$args\" \"$QT_TEST_POSTGRES_USER\" "
        "\"$QT_TEST_POSTGRES_PASSWORD\" \"$QT_TEST_POSTGRES_DB\" >> \"$QT_TEST_DOCKER_LOG\"\n"
        "if [[ \" $* \" == *\" run \"* ]]; then exit \"${QT_TEST_FAKE_RUN_STATUS:-0}\"; fi\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    environment = {
        **os.environ,
        "PATH": (
            f"{tmp_path}{os.pathsep}{Path(sys.executable).parent}"
            f"{os.pathsep}{os.environ['PATH']}"
        ),
        "QT_TEST_DOCKER_LOG": str(docker_log),
        "SOURCE_REVISION": "a" * 40,
        "SOURCE_TREE_HASH": "b" * 64,
        "AMBIENT_SENTINEL_SECRET": "must-not-appear-in-docker-arguments",
        "QT_TEST_POSTGRES_USER": "ambient-user-must-be-replaced",
        "QT_TEST_POSTGRES_PASSWORD": "ambient-password-must-be-replaced",
        "QT_TEST_POSTGRES_DB": "ambient-database-must-be-replaced",
    }

    for expected_status in (0, 23):
        completed = subprocess.run(
            ["bash", str(_RUNNER), "db"],
            cwd=_ROOT,
            env={**environment, "QT_TEST_FAKE_RUN_STATUS": str(expected_status)},
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == expected_status, completed.stdout + completed.stderr

    rows = [
        line.split("\t")
        for line in docker_log.read_text(encoding="utf-8").splitlines()
    ]
    assert len(rows) == 6
    invocations = (rows[:3], rows[3:])
    identities: list[tuple[str, str, str, str]] = []
    for invocation in invocations:
        commands = [row[0].split() for row in invocation]
        projects = {
            command[command.index("--project-name") + 1]
            for command in commands
        }
        assert len(projects) == 1
        assert "build" in commands[0]
        assert "run" in commands[1]
        assert commands[2][-5:] == [
            "down",
            "--volumes",
            "--remove-orphans",
            "--rmi",
            "local",
        ]
        assert all(
            "must-not-appear-in-docker-arguments" not in row[0]
            for row in invocation
        )
        project = projects.pop()
        user, password, database = invocation[0][1:]
        assert all(row[1:] == [user, password, database] for row in invocation)
        assert project.startswith("qt-test-")
        assert user.startswith("qt_test_")
        assert database == user
        assert len(password) == 48
        assert "ambient" not in user
        assert "ambient" not in password
        assert "ambient" not in database
        identities.append((project, user, password, database))

    assert identities[0] != identities[1]
