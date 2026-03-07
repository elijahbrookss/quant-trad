from __future__ import annotations

import subprocess

import pytest

from portal.backend.service.bots.runner import DockerBotRunner


def _cp(cmd: list[str], *, returncode: int = 0, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(cmd, returncode, stdout=stdout, stderr=stderr)


def test_start_bot_uses_configured_network_when_present(monkeypatch):
    runner = DockerBotRunner(image="quanttrad-backend:dev", network="quanttrad")
    observed_run_networks: list[str] = []

    def _fake_run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
        if cmd[:3] == ["docker", "rm", "-f"]:
            return _cp(cmd, returncode=0)
        if cmd[:3] == ["docker", "network", "inspect"]:
            return _cp(cmd, returncode=0)
        if cmd[:3] == ["docker", "run", "-d"]:
            observed_run_networks.append(cmd[cmd.index("--network") + 1])
            return _cp(cmd, returncode=0, stdout="container-123\n")
        pytest.fail(f"unexpected docker command: {cmd}")

    monkeypatch.setattr(runner, "_run_docker", _fake_run)
    container_id = runner.start_bot(bot={"id": "bot-1", "snapshot_interval_ms": 1000})

    assert container_id == "container-123"
    assert observed_run_networks == ["quanttrad"]


def test_start_bot_raises_when_configured_network_missing(monkeypatch):
    runner = DockerBotRunner(image="quanttrad-backend:dev", network="quanttrad")

    def _fake_run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
        if cmd[:3] == ["docker", "rm", "-f"]:
            return _cp(cmd, returncode=0)
        if cmd[:3] == ["docker", "network", "inspect"]:
            return _cp(cmd, returncode=1, stderr="No such network")
        pytest.fail(f"unexpected docker command: {cmd}")

    monkeypatch.setattr(runner, "_run_docker", _fake_run)

    with pytest.raises(RuntimeError, match="BOT_RUNTIME_NETWORK not found"):
        runner.start_bot(bot={"id": "bot-1", "snapshot_interval_ms": 1000})


def test_from_env_defaults_to_compose_shared_network(monkeypatch):
    monkeypatch.setenv("BOT_RUNTIME_IMAGE", "quanttrad-backend:dev")
    monkeypatch.delenv("BOT_RUNTIME_NETWORK", raising=False)

    runner = DockerBotRunner.from_env()

    assert runner.network == "quant-trad_quanttrad"
