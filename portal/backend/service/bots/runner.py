from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Protocol

from core.settings import get_settings

logger = logging.getLogger(__name__)
DEFAULT_BOT_RUNTIME_NETWORK = "quant-trad_quanttrad"
_SETTINGS = get_settings()
_BOT_RUNTIME_SETTINGS = _SETTINGS.bot_runtime
_DATABASE_SETTINGS = _SETTINGS.database
_SECURITY_SETTINGS = _SETTINGS.security


class BotRunner(Protocol):
    def start_bot(self, *, bot: Mapping[str, object]) -> str: ...

    def stop_bot(self, *, bot_id: str) -> None: ...


@dataclass
class DockerBotRunner:
    image: str
    network: str
    project: str = "quant-trad-bots"

    @classmethod
    def from_env(cls) -> "DockerBotRunner":
        image = str(_BOT_RUNTIME_SETTINGS.image or "").strip()
        if not image:
            raise RuntimeError("QT_BOT_RUNTIME_IMAGE is required for docker bot runner")
        network = str(_BOT_RUNTIME_SETTINGS.network or DEFAULT_BOT_RUNTIME_NETWORK).strip()
        if not network:
            raise RuntimeError("QT_BOT_RUNTIME_NETWORK is required for docker bot runner")
        return cls(image=image, network=network)

    @staticmethod
    def container_name_for(bot_id: str, project: str = "quant-trad-bots") -> str:
        return f"{project}-{bot_id}"

    def _container_name(self, bot_id: str) -> str:
        return self.container_name_for(bot_id, project=self.project)

    @staticmethod
    def _run_docker(cmd: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:
            raise RuntimeError(
                "docker_cli_missing: docker binary not found in backend runtime. "
                "Install docker CLI in the backend image and mount /var/run/docker.sock."
            ) from exc

    def _network_exists(self, network_name: str) -> bool:
        proc = self._run_docker(["docker", "network", "inspect", network_name])
        return proc.returncode == 0

    def _resolve_runtime_network(self) -> str:
        if self._network_exists(self.network):
            return self.network

        raise RuntimeError(
            "QT_BOT_RUNTIME_NETWORK not found. "
            f"requested={self.network} "
            "Expected shared compose network from docker/docker-compose.yml is "
            f"{DEFAULT_BOT_RUNTIME_NETWORK}. "
            "Set QT_BOT_RUNTIME_NETWORK explicitly to the exact docker network name in use."
        )

    @staticmethod
    def _runtime_process_env(bot_id: str) -> Dict[str, str]:
        env_map = {key: str(value) for key, value in os.environ.items() if key.startswith("QT_")}
        if _DATABASE_SETTINGS.dsn:
            env_map["PG_DSN"] = str(_DATABASE_SETTINGS.dsn)
        provider_key = str(_SECURITY_SETTINGS.provider_credential_key or "").strip()
        if provider_key:
            env_map["QT_SECURITY_PROVIDER_CREDENTIAL_KEY"] = provider_key
        env_map["QT_BOT_RUNTIME_BOT_ID"] = str(bot_id)
        return env_map

    @classmethod
    def inspect_bot_container(
        cls,
        bot_id: str,
        *,
        project: str = "quant-trad-bots",
    ) -> Dict[str, Any]:
        container_name = cls.container_name_for(str(bot_id or "").strip(), project=project)
        if not container_name or container_name.endswith("-"):
            raise RuntimeError("bot id is required to inspect docker runtime")

        proc = cls._run_docker(["docker", "inspect", container_name])
        stderr = str(proc.stderr or "").strip()
        stdout = str(proc.stdout or "").strip()
        if proc.returncode != 0:
            missing_markers = ("No such object", "No such container")
            missing = any(marker in stderr or marker in stdout for marker in missing_markers)
            return {
                "name": container_name,
                "status": "missing" if missing else "unknown",
                "running": False,
                "id": None,
                "started_at": None,
                "finished_at": None,
                "exit_code": None,
                "error": stderr or stdout or None,
            }

        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"docker inspect returned invalid json for {container_name}: {exc}"
            ) from exc
        if not isinstance(payload, list) or not payload:
            raise RuntimeError(f"docker inspect returned empty payload for {container_name}")
        container = payload[0] if isinstance(payload[0], dict) else {}
        state = container.get("State") if isinstance(container.get("State"), dict) else {}
        status = str(state.get("Status") or "").strip().lower() or "unknown"
        return {
            "name": container_name,
            "status": status,
            "running": bool(state.get("Running")),
            "id": str(container.get("Id") or "").strip() or None,
            "started_at": str(state.get("StartedAt") or "").strip() or None,
            "finished_at": str(state.get("FinishedAt") or "").strip() or None,
            "exit_code": state.get("ExitCode"),
            "error": str(state.get("Error") or "").strip() or None,
        }

    def start_bot(self, *, bot: Mapping[str, object]) -> str:
        bot_id = str(bot.get("id") or "").strip()
        if not bot_id:
            raise RuntimeError("bot id is required to start docker runtime")
        snapshot_interval = bot.get("snapshot_interval_ms")
        if not isinstance(snapshot_interval, int) or snapshot_interval <= 0:
            raise RuntimeError("snapshot_interval_ms is required and must be a positive integer")
        provider_credential_key = str(_SECURITY_SETTINGS.provider_credential_key or "").strip()
        if not provider_credential_key:
            raise RuntimeError(
                "QT_SECURITY_PROVIDER_CREDENTIAL_KEY is required for bot runtime containers. "
                "Set it on the backend service environment before starting bots."
            )
        name = self._container_name(bot_id)
        self.stop_bot(bot_id=bot_id)
        network = self._resolve_runtime_network()
        runtime_env = self._runtime_process_env(bot_id)
        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--network",
            network,
        ]
        for key, value in sorted(runtime_env.items()):
            cmd.extend(["-e", f"{key}={value}"])
        cmd.extend(
            [
                self.image,
                "python",
                "-m",
                "portal.backend.service.bots.container_runtime",
            ]
        )
        logger.info("docker_bot_runner_start | bot_id=%s | image=%s | network=%s", bot_id, self.image, network)
        proc = self._run_docker(cmd)
        if proc.returncode != 0:
            raise RuntimeError(f"docker start failed: {proc.stderr.strip() or proc.stdout.strip()}")
        container_id = (proc.stdout or "").strip()
        if not container_id:
            raise RuntimeError("docker start returned empty container id")
        return container_id

    def stop_bot(self, *, bot_id: str) -> None:
        name = self._container_name(bot_id)
        cmd = ["docker", "rm", "-f", name]
        proc = self._run_docker(cmd)
        if proc.returncode != 0 and "No such container" not in (proc.stderr or ""):
            raise RuntimeError(f"docker stop failed: {proc.stderr.strip()}")
