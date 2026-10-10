from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest
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


@pytest.mark.parametrize("pytest_args", [(), ("tests/test_market_data", "-k", "cutover and not archived")])
def test_database_runner_uses_unique_identity_and_cleans_successful_and_failed_runs(
    tmp_path: Path, pytest_args: tuple[str, ...],
) -> None:
    docker_log = tmp_path / "docker.log"
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "args=\"$*\"\n"
        "args=\"${args//$'\\n'/ }\"\n"
        "printf '%s\\t%s\\t%s\\t%s\\n' \"$args\" \"$QT_TEST_POSTGRES_USER\" "
        "\"$QT_TEST_POSTGRES_PASSWORD\" \"$QT_TEST_POSTGRES_DB\" >> \"$QT_TEST_DOCKER_LOG\"\n"
        "if [[ \" $* \" == *\" ps --all --quiet timescaledb \"* ]]; then printf '%s\\n' " + repr("c" * 64) + "; fi\n"
        "if [[ \" $* \" == *\" run \"* ]]; then exit \"${QT_TEST_FAKE_RUN_STATUS:-0}\"; fi\n",
        encoding="utf-8",
    )
    fake_docker.write_text(fake_docker.read_text() +
        'if [[ " $* " == *" down "* ]]; then exit "${QT_TEST_FAKE_CLEANUP_STATUS:-0}"; fi\n')
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

    invocations = []
    offset = 0
    for run_status, cleanup_status in ((0, 0), (23, 0), (0, 31), (23, 31)):
        expected_status = run_status or cleanup_status
        completed = subprocess.run(
            ["bash", str(_RUNNER), "db", *pytest_args],
            cwd=_ROOT,
            env={**environment, "QT_TEST_FAKE_RUN_STATUS": str(run_status),
                 "QT_TEST_FAKE_CLEANUP_STATUS": str(cleanup_status)},
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == expected_status, completed.stdout + completed.stderr
        rows = [line.split("\t") for line in docker_log.read_text(encoding="utf-8").splitlines()]
        stdout = completed.stdout
        scope = "focused-db" if pytest_args else "all-db"
        assert f"scope={scope}" in stdout
        assert f"broad_db={str(not pytest_args).lower()}" in stdout
        assert "source_revision=" + "a" * 40 in stdout
        assert "source_tree_hash=" + "b" * 64 in stdout
        for phase in ("build", "execution", "cleanup"):
            assert f"event=start phase={phase}" in stdout
            assert f"event=end phase={phase}" in stdout
        assert f"status={run_status}" in stdout
        assert f"status={cleanup_status}" in stdout
        assert "must-not-appear" not in stdout + completed.stderr
        assert all(row[2] not in stdout + completed.stderr for row in rows[offset:])
        invocations.append((run_status, rows[offset:]))
        offset = len(rows)

    identities: list[tuple[str, str, str, str]] = []
    for status, invocation in invocations:
        commands = [row[0].split() for row in invocation]
        projects = {
            command[command.index("--project-name") + 1]
            for command in commands if command[0] == "compose"
        }
        assert len(projects) == 1
        assert "build" in commands[0]
        assert "run" in commands[1]
        if pytest_args:
            assert "tests/test_market_data -k cutover\\ and\\ not\\ archived" in invocation[1][0]
        assert commands[-1][-5:] == [
            "down",
            "--volumes",
            "--remove-orphans",
            "--rmi",
            "local",
        ]
        diagnostics = commands[2:-1]
        if status:
            assert diagnostics[0][-5:] == ["logs", "--no-color", "--tail", "120", "timescaledb"]
            assert diagnostics[1][-4:] == ["ps", "--all", "--quiet", "timescaledb"]
            assert diagnostics[2] == ["inspect", "--format", "{{json", ".State}}", "c" * 64]
        else:
            assert not diagnostics
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

    assert len(set(identities)) == len(identities)


@pytest.mark.parametrize("database_name, namespace_exists", [
    ("qt_migration_online_" + "a" * 16, False),
    ("not_the_owned_host_database", False),
    ("qt_migration_online_" + "a" * 16, True),
])
def test_online_host_fixture_retains_private_database_factory_boundary(
    monkeypatch, database_name, namespace_exists,
):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from tests.test_market_data import test_storage_online_entrypoint_db as entrypoint
    request = object()
    disposed = []
    @contextmanager
    def begin():
        yield SimpleNamespace(scalar=lambda _: "market" if namespace_exists else None,
                              execute=lambda _: None)
    engine = SimpleNamespace(begin=begin, dispose=lambda: disposed.append(True))
    monkeypatch.setenv("QT_ONLINE_HOST_FIXTURE", "1")
    monkeypatch.setattr(entrypoint, "_isolated_parent_dsn",
                        lambda: "postgresql://synthetic:synthetic@timescaledb/" + database_name)
    monkeypatch.setattr(entrypoint, "create_engine", lambda _: engine)
    def tier_storage(actual_monkeypatch, actual_request):
        assert actual_monkeypatch is monkeypatch and actual_request is request
        with entrypoint.tiers.storage_behavior_database(actual_request) as dsn:
            yield dsn
    monkeypatch.setattr(entrypoint.tiers, "storage", SimpleNamespace(__wrapped__=tier_storage))
    fixture = entrypoint.storage.__wrapped__(monkeypatch, request)
    if database_name.startswith("qt_migration_online_") and not namespace_exists:
        assert next(fixture).endswith("/" + database_name)
        fixture.close()
        assert disposed == [True]
    else:
        with pytest.raises(AssertionError):
            next(fixture)


@pytest.mark.parametrize("identity_failure", ["", "git", "hash"])
def test_ci_pilot_wiring_binds_checkout_before_disposable_tests(tmp_path, identity_failure):
    workflow = yaml.safe_load((_ROOT / ".github/workflows/test.yaml").read_text())
    steps = workflow["jobs"]["clean-database-bootstrap"]["steps"]
    step = next(step for step in steps if step["name"] == "Run PostgreSQL-backed contract tests")
    assert step["env"]["QT_SCHEMA_TEMPLATE_PILOT"] == "1"
    assert "QT_SCHEMA_TEMPLATE_PILOT" not in workflow.get("env", {})
    for job in workflow["jobs"].values():
        assert "QT_SCHEMA_TEMPLATE_PILOT" not in job.get("env", {})
        for other in job["steps"]:
            if other is not step:
                assert "QT_SCHEMA_TEMPLATE_PILOT" not in other.get("env", {})
    revision, tree_hash = "a" * 40, "b" * 64
    observed = tmp_path / "pytest-observed"
    git = tmp_path / "git"
    git.write_text("#!/usr/bin/env bash\n"
        '[[ "$*" == "rev-parse HEAD" && "$IDENTITY_FAILURE" != "git" ]] || exit 19\n'
        + "printf '%s\\n' " + revision + "\n")
    python = tmp_path / "python"
    python.write_text("#!/usr/bin/env bash\n"
        'if [[ "$1" == "scripts/provenance/source_tree_hash.py" ]]; then\n'
        '  [[ "$2" == "--git-revision" && "$3" == "' + revision + '" && "$IDENTITY_FAILURE" != "hash" ]] || exit 23\n'
        + "  printf '%s\\n' " + tree_hash + "\n"
        'elif [[ "$1 $2" == "-m pytest" ]]; then\n'
        '  printf "%s\\n" "$SOURCE_REVISION" "$SOURCE_TREE_HASH" "$QT_SCHEMA_TEMPLATE_PILOT" "$QT_DB_TEST_ISOLATED" "$RUN_DB_TESTS" "$@" > "$OBSERVED"\n'
        'else exit 29; fi\n')
    git.chmod(0o755)
    python.chmod(0o755)
    result = subprocess.run(["bash", "-ec", step["run"]], cwd=_ROOT,
        env={**os.environ, **step["env"], "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
             "IDENTITY_FAILURE": identity_failure, "OBSERVED": str(observed)},
        capture_output=True, text=True, timeout=10)
    if identity_failure:
        assert result.returncode != 0
        assert not observed.exists()
    else:
        assert result.returncode == 0, result.stderr
        assert observed.read_text().splitlines() == [revision, tree_hash, "1", "1", "1",
            "-m", "pytest", "-q", "-m", "db",
            "--ignore=tests/test_portal/test_clean_database_bootstrap_db.py",
            "--ignore=tests/test_market_data/test_header_namespace_db.py"]
