#!/usr/bin/env python3
"""Fixed pause rehearsal on synthetic, isolated Docker processes (no database).

No real environment/credentials, host mounts, application volumes, providers or
server connections. Does not qualify application drain, schema, or performance.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.automation import storage_handoff_pause as pause


def run(args, *, env, timeout=60):
    result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"disposable_pause_command_failed: operation={args[0]} exit={result.returncode}")
    return result.stdout.strip()


def main():
    project = "qt-storage-pause-" + uuid.uuid4().hex[:12]
    network = project + "_quanttrad"
    revision = "a" * 40
    # Sanitize QT/DB/Compose inputs; every Docker operation targets fresh resources.
    env = {k: v for k, v in os.environ.items() if not k.startswith(("QT_", "PG_", "POSTGRES_", "COMPOSE_"))}
    image = run(["docker", "image", "inspect", "python:3.12.3-slim", "--format", "{{.Id}}"], env=env)
    containers = []
    network_created = False
    actual_docker = pause._docker
    with tempfile.TemporaryDirectory(prefix="qt-storage-pause-") as directory:
        state = Path(directory)
        (state / "release.env").write_text(f"current_revision={revision}\n")
        try:
            run(["docker", "network", "create", "--internal", network], env=env)
            network_created = True
            for service in pause.STOP + pause.PASSIVE:
                command = ["docker", "run", "--detach", "--pull=never", "--init", "--network", network,
                    "--name", project + "-" + service, "--read-only", "--user", "65534:65534",
                    "--memory", "32m", "--cpus", "0.1", "--pids-limit", "32", "--restart", "unless-stopped",
                    "--label", "com.docker.compose.project=" + project,
                    "--label", "com.docker.compose.service=" + service,
                    "--label", "com.docker.compose.oneoff=False", image,
                    "sh", "-c", "trap 'echo stopped; exit 0' TERM; echo ready; while :; do sleep 1 & wait $!; done"]
                containers.append(run(command, env=env))
            # No external work starts before every disposable fixture is ready.
            for container in containers:
                logs = run(["docker", "logs", "--tail", "2", container], env=env)
                if "ready" not in logs:
                    raise RuntimeError("disposable_pause_fixture_not_ready")
            lost = False
            def boundary(*args, **kwargs):
                nonlocal lost
                result = actual_docker(*args, **kwargs)
                if args[0] == "stop" and not lost:
                    lost = True
                    raise TimeoutError("injected lost stop reply")
                return result
            pause._docker = boundary
            try:
                with pause.paused_storage_clients(state, project=project, source_revision=revision):
                    raise AssertionError("lost reply was not injected")
            except TimeoutError:
                if not lost:
                    raise
            if json.loads((state / pause.HOLD).read_text())["phase"] != "pausing":
                raise AssertionError("interrupted pause lost its hold")
            pause._docker = actual_docker
            with pause.paused_storage_clients(state, project=project, source_revision=revision):
                rows = pause._inventory(project)
                if any(rows[x]["running"] for x in pause.STOP) or not all(rows[x]["running"] for x in pause.PASSIVE):
                    raise AssertionError("fixed client/passive placement did not hold")
            for service in pause.STOP:
                if "stopped" not in run(["docker", "logs", "--tail", "2", rows[service]["id"]], env=env):
                    raise AssertionError("disposable signal drain was not observed")
            dispatch_env = {**env, "QT_SINGLE_NODE_STATE_ROOT": str(state),
                            "QT_SINGLE_NODE_ENV_FILE": str(state / "absent-synthetic.env")}
            for action in ("deploy", "recover", "rollback", "qt"):
                result = subprocess.run(["bash", str(ROOT / "scripts/automation/server_deploy.sh"), action],
                                        env=dispatch_env, text=True, capture_output=True, timeout=20)
                if result.returncode == 0 or "storage handoff hold" not in result.stderr:
                    raise AssertionError("held deployment action was not refused")
            print(json.dumps(dict(schema_version="qt.disposable_storage_pause_rehearsal.v1",
                project=project, image=image, lost_stop_reply_recovered=True,
                fixed_clients_stopped=True, passive_services_running=True,
                held_deployment_refused=True, real_database_tested=False,
                live_server_accessed=False)), flush=True)
        finally:
            pause._docker = actual_docker
            # Exact IDs created in this run only; never prune shared resources.
            errors = []
            for container in containers:
                try:
                    run(["docker", "rm", "--force", container], env=env)
                except Exception as exc:
                    errors.append(str(exc))
            if network_created:
                try:
                    run(["docker", "network", "rm", network], env=env)
                except Exception as exc:
                    errors.append(str(exc))
            if errors:
                raise RuntimeError("disposable_pause_cleanup_failed: " + "; ".join(errors))
            print("Disposable storage-pause resources removed.", flush=True)


def runtime_recipe_rehearsal():
    """Real Compose normalization against synthetic inspected runtime bindings.

    No containers are started. This qualifies the serialized recipe boundary,
    not application activation, runtime health or source database correctness.
    """
    import pytest
    from tests import test_storage_handoff_pause as fixture
    env={k:v for k,v in os.environ.items() if not k.startswith(("QT_","PG_","POSTGRES_","COMPOSE_"))}
    with tempfile.TemporaryDirectory(prefix="qt-runtime-recipe-") as folder, pytest.MonkeyPatch.context() as monkeypatch:
        root=Path(folder)
        database=fixture.database_setup.__wrapped__(root,monkeypatch)
        operator=fixture.operator_setup.__wrapped__(database,root,monkeypatch)
        state,database,worker,model,check=fixture.runtime_recipe_setup.__wrapped__(operator)
        synthetic="postgresql+psycopg2://fixture:literal$secret@tsdb:5432/fixture"
        collector=database.details[database.rows["market-data-collector"]["id"]]
        collector["config"]["Env"]=["PG_DSN="+synthetic]
        for name in pause._RUNTIME_WRITERS:
            model["services"][name]["environment"]["PG_DSN"]=synthetic.replace("$","$$")
        def render(path):
            return json.loads(run(["docker","compose","--file",str(path),"config","--format","json"],env=env))
        # Keep the prepared DB recipe untouched; only the candidate snapshot
        # passes through Compose normalization before comparing its meaning.
        path=state/pause.RUNTIME_RECIPE
        path.write_text(json.dumps(model));path.chmod(0o600)
        normalized=render(path);model.clear();model.update(normalized)
        result=check()
        assert result["recipe_sha256"]==pause._digest(normalized)
        assert model["services"]["market-data-collector"]["environment"]["PG_DSN"]==synthetic.replace("$","$$")
        again=render(path);assert again==normalized
        hashes=run(["docker","compose","--file",str(path),"config","--hash","*"],env=env)
        values=dict(line.split() for line in hashes.splitlines())
        assert set(values)==set(model["services"])
        assert all(len(value)==64 and int(value,16)>=0 for value in values.values())
        assert (state/pause.HOLD).exists()
        assert not any(database.rows[name]["running"] for name in pause.STOP)
        print("PASS: actual Compose rendering preserves the fixed candidate recipe, existing database topology and literal-dollar synthetic credentials; admission retains the client hold; no containers were created")


if __name__ == "__main__":
    if sys.argv[1:]==["--runtime-recipe"]:
        runtime_recipe_rehearsal()
    elif not sys.argv[1:]:
        main()
    else:
        raise SystemExit("usage: test_storage_handoff_pause.py [--runtime-recipe]")
