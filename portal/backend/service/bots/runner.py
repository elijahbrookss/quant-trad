from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass
from typing import Mapping, Protocol

logger = logging.getLogger(__name__)


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
        image = os.getenv("BOT_RUNTIME_IMAGE", "").strip()
        if not image:
            raise RuntimeError("BOT_RUNTIME_IMAGE is required for docker bot runner")
        network = os.getenv("BOT_RUNTIME_NETWORK", "quanttrad").strip()
        if not network:
            raise RuntimeError("BOT_RUNTIME_NETWORK is required for docker bot runner")
        return cls(image=image, network=network)

    def _container_name(self, bot_id: str) -> str:
        return f"{self.project}-{bot_id}"

    def start_bot(self, *, bot: Mapping[str, object]) -> str:
        bot_id = str(bot.get("id") or "").strip()
        if not bot_id:
            raise RuntimeError("bot id is required to start docker runtime")
        snapshot_interval = bot.get("snapshot_interval_ms")
        if not isinstance(snapshot_interval, int) or snapshot_interval <= 0:
            raise RuntimeError("snapshot_interval_ms is required and must be a positive integer")
        name = self._container_name(bot_id)
        self.stop_bot(bot_id=bot_id)
        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--network",
            self.network,
            "-e",
            f"PG_DSN={os.getenv('PG_DSN','')}",
            "-e",
            f"BOT_ID={bot_id}",
            "-e",
            f"SNAPSHOT_INTERVAL_MS={snapshot_interval}",
            "-e",
            f"BACKEND_TELEMETRY_WS_URL={os.getenv('BACKEND_TELEMETRY_WS_URL','ws://backend.quanttrad:8000/api/bots/ws/telemetry/ingest')}",
        ]
        bot_env = bot.get("bot_env")
        if isinstance(bot_env, Mapping):
            for key, value in bot_env.items():
                env_key = str(key or "").strip()
                if not env_key:
                    continue
                cmd.extend(["-e", f"{env_key}={'' if value is None else str(value)}"])
        cmd.extend(
            [
                self.image,
                "python",
                "-m",
                "portal.backend.service.bots.container_runtime",
            ]
        )
        logger.info("docker_bot_runner_start | bot_id=%s | image=%s", bot_id, self.image)
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise RuntimeError(f"docker start failed: {proc.stderr.strip() or proc.stdout.strip()}")
        container_id = (proc.stdout or "").strip()
        if not container_id:
            raise RuntimeError("docker start returned empty container id")
        return container_id

    def stop_bot(self, *, bot_id: str) -> None:
        name = self._container_name(bot_id)
        cmd = ["docker", "rm", "-f", name]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0 and "No such container" not in (proc.stderr or ""):
            raise RuntimeError(f"docker stop failed: {proc.stderr.strip()}")
