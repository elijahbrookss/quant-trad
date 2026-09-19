#!/usr/bin/env python3
"""Rehearse the fixed host hold with owned PostgreSQL and synthetic clients.

No production configuration, credentials, provider egress, or physical disks.
The history bind uses a disposable directory and synthetic UUID evidence.
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


def run(args, *, env, timeout=300, ok=True):
    result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout)
    if ok and result.returncode:
        # All inputs in this fixture are synthetic; no environment is printed.
        raise RuntimeError(f"disposable_database_hold_command_failed: {args[0]} exit={result.returncode}\n{result.stderr[-4000:]}")
    return result


def main():
    project = "qt-database-hold-"+uuid.uuid4().hex[:12]
    network = project+"_quanttrad"
    volume = project+"-postgres"
    env = {key: value for key, value in os.environ.items() if not key.startswith(("QT_", "PG_", "POSTGRES_", "COMPOSE_"))}
    print("Owned disposable project: "+project, flush=True)
    pg_image = run(["docker", "image", "inspect", "quanttrad-postgres:2.14.2-pg15", "--format", "{{.Id}}"], env=env).stdout.strip()
    client_image = run(["docker", "image", "inspect", "python:3.12.3-slim", "--format", "{{.Id}}"], env=env).stdout.strip()
    created_network = created_volume = False
    with tempfile.TemporaryDirectory(prefix="qt-database-hold-") as directory:
        root = Path(directory)
        state = root/"state"; state.mkdir()
        history = root/"history"; history.mkdir(); history.chmod(0o777)
        (history/"preparation-proof").write_text("retained-history-path")
        udev = root/"udev"; udev.mkdir()
        device = history.stat().st_dev
        (udev/f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=fixture-history\n")
        env["QT_STORAGE_UDEV_ROOT"] = str(udev)
        revision = "a"*40
        (state/"release.env").write_text("current_revision="+revision+"\n")
        service = {
            "image": pg_image, "pull_policy": "never", "hostname": "tsdb.quanttrad",
            "command": ["postgres", "-c", "shared_buffers=64MB", "-c", "max_connections=20"],
            "environment": {"POSTGRES_DB": "quanttrad", "POSTGRES_USER": "quanttrad",
                            "POSTGRES_PASSWORD": uuid.uuid4().hex, "PGDATA": "/var/lib/postgresql/data/pgdata"},
            "init": True, "restart": "no", "shm_size": 1073741824,
            "healthcheck": {"test": pause._TCP_PROBE, "interval": "1s", "timeout": "2s", "retries": 120, "start_period": "10s"},
            "volumes": [{"type": "volume", "source": "postgres-data", "target": "/var/lib/postgresql/data"}],
            "networks": {"quanttrad": {"aliases": ["tsdb.quanttrad"]}},
        }
        model = {"name": project, "services": {"tsdb": service},
                 "volumes": {"postgres-data": {"name": volume, "external": True}},
                 "networks": {"quanttrad": {"name": network, "external": True}}}
        source = root/"source.compose.json"; source.write_text(json.dumps(model)); source.chmod(0o600)
        recipe = json.loads(json.dumps(model))
        recipe["services"]["tsdb"]["volumes"].append({"type": "bind", "source": str(history),
            "target": "/qt-history", "bind": {"create_host_path": False}})
        path = state/pause.DATABASE_RECIPE; path.write_text(json.dumps(recipe)); path.chmod(0o600)
        compose = ["docker", "compose", "--project-name", project, "--file", str(source)]
        def cid():
            return run(["docker", "ps", "-aq", "--no-trunc", "--filter", "label=com.docker.compose.project="+project,
                        "--filter", "label=com.docker.compose.service=tsdb"], env=env).stdout.strip()
        def query(sql):
            return run(["docker", "exec", cid(), "sh", "-ec",
                'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -Atc "$1"',
                "fixture", sql], env=env).stdout.strip()
        old_udev = os.environ.get("QT_STORAGE_UDEV_ROOT")
        try:
            run(["docker", "network", "create", "--internal", network], env=env); created_network = True
            run(["docker", "volume", "create", volume], env=env); created_volume = True
            run(compose+["up", "--detach", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "180"], env=env)
            original = cid()
            original_details = pause._database_details(original)
            cluster = query("SELECT system_identifier FROM pg_control_system()")
            query("CREATE TABLE public.qt_host_hold_proof(value text PRIMARY KEY); INSERT INTO public.qt_host_hold_proof VALUES ('retained')")
            for name in pause.STOP+tuple(value for value in pause.PASSIVE if value != "tsdb"):
                run(["docker", "run", "--detach", "--pull", "never", "--network", network,
                    "--name", project+"-"+name, "--read-only", "--user", "65534:65534", "--init",
                    "--memory", "32m", "--cpus", "0.1", "--pids-limit", "32", "--restart", "unless-stopped",
                    "--label", "com.docker.compose.project="+project, "--label", "com.docker.compose.service="+name,
                    "--label", "com.docker.compose.oneoff=False", client_image, "sh", "-c",
                    "trap 'exit 0' TERM; while :; do sleep 1 & wait $!; done"], env=env)
            child_code = "\n".join([
                "import os,signal,sys",
                "from pathlib import Path",
                "from scripts.automation import storage_handoff_pause as pause",
                "actual=pause._docker",
                "def die_after_create(*args,**kwargs):",
                "    result=actual(*args,**kwargs)",
                "    if args[0]=='compose' and 'create' in args:",
                "        print('CREATED_BEFORE_PROCESS_DEATH',flush=True)",
                "        os.kill(os.getpid(),signal.SIGKILL)",
                "    return result",
                "pause._docker=die_after_create",
                "with pause.paused_storage_clients(Path(sys.argv[1]),project=sys.argv[2],source_revision='a'*40,prepare_database=True,history_uuid='fixture-history'):",
                "    raise AssertionError('interruption was not injected')",
            ])
            print("Interrupting the owning host controller after database creation", flush=True)
            child = run([sys.executable, "-c", child_code, str(state), project], env=env, timeout=500, ok=False)
            if child.returncode != -9 or "CREATED_BEFORE_PROCESS_DEATH" not in child.stdout:
                raise RuntimeError("disposable_host_interruption_failed: "+child.stderr[-6000:])
            held = json.loads((state/pause.HOLD).read_text())
            assert held["phase"] == "preparing_database" and held["database_preparation"]["source_stopped"]
            replacement = cid(); assert replacement != original
            assert run(["docker", "inspect", "--format", "{{.State.Status}}", replacement], env=env).stdout.strip() == "created"
            os.environ["QT_STORAGE_UDEV_ROOT"] = str(udev)
            with pause.paused_storage_clients(state, project=project, source_revision=revision,
                                               prepare_database=True, history_uuid="fixture-history") as receipt:
                assert receipt["phase"] == "database_prepared" and cid() == replacement
                assert receipt["database_preparation"]["cluster_identifier"] == cluster
                assert query("SELECT value FROM public.qt_host_hold_proof") == "retained"
                rows = pause._inventory(project)
                assert not any(rows[name]["running"] for name in pause.STOP)
                assert all(rows[name]["running"] for name in pause.PASSIVE)
                assert run(["docker", "exec", replacement, "cat", "/qt-history/preparation-proof"], env=env).stdout.strip() == "retained-history-path"
            with pause.paused_storage_clients(state, project=project, source_revision=revision,
                                               prepare_database=True, history_uuid="fixture-history"):
                assert cid() == replacement
            for action in ("deploy", "recover", "rollback"):
                result = run(["bash", "scripts/automation/server_deploy.sh", action],
                    env={**env, "QT_SINGLE_NODE_STATE_ROOT": str(state), "QT_SINGLE_NODE_ENV_FILE": str(root/"absent.env")}, ok=False)
                assert result.returncode and "storage handoff hold" in result.stderr
            assert (state/pause.HOLD).exists()
            print("PASS: durable database replacement intent survives owning-process death; exact replacement resumes with original cluster/data and history bind; clients stay paused and ordinary deployment stays blocked", flush=True)
        except BaseException:
            target = cid()
            if target:
                if "original_details" in locals():
                    current = pause._database_details(target)
                    # Diagnostic keys only: never print the resolved environment.
                    changes = {group: [key for key in set(original_details[group]) | set(current[group])
                               if original_details[group].get(key) != current[group].get(key)]
                               for group in ("config", "host")}
                    print("Fixture changed Docker fields: "+json.dumps(changes), flush=True)
                    print("Fixture networks: "+json.dumps({"source": pause._database_networks(original_details),
                                                           "replacement": pause._database_networks(current)}), flush=True)
                    print("Fixture mounts: "+json.dumps({"source": original_details["mounts"], "replacement": current["mounts"]}), flush=True)
                print(run(["docker", "logs", "--tail", "35", target], env=env, ok=False).stdout[-7000:], flush=True)
            raise
        finally:
            if old_udev is None:
                os.environ.pop("QT_STORAGE_UDEV_ROOT", None)
            else:
                os.environ["QT_STORAGE_UDEV_ROOT"] = old_udev
            ids = run(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project="+project], env=env).stdout.split()
            if ids:
                run(["docker", "rm", "--force", *ids], env=env)
            if created_volume:
                run(["docker", "volume", "rm", volume], env=env)
            if created_network:
                run(["docker", "network", "rm", network], env=env)
            print("Owned disposable host-hold resources removed.", flush=True)


if __name__ == "__main__":
    main()
