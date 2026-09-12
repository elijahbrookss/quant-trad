#!/usr/bin/env python3
"""Freeze rendered Compose configuration to retained copies of running images."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import uuid


def output(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=60).stdout.strip()


def pin(path: Path) -> None:
    config = json.loads(path.read_text())
    project = config["name"]
    prefix = uuid.uuid4().hex
    # Compose omits disabled profiles from its default rendered service model.
    for index, (name, service) in enumerate(config["services"].items()):
        ids = output("docker", "ps", "--all", "--quiet", "--filter", f"label=com.docker.compose.project={project}",
                     "--filter", f"label=com.docker.compose.service={name}",
                     "--filter", "label=com.docker.compose.oneoff=False").splitlines()
        if len(ids) != 1:
            raise ValueError(f"recovery requires exactly one existing container for {name}, found {len(ids)}")
        state = json.loads(output("docker", "inspect", "--format", "{{json .State}}", ids[0]))
        if name == "initialize":
            if state["Status"] != "exited" or state["ExitCode"] != 0:
                raise ValueError("current initializer has not succeeded")
        elif not state["Running"] or state.get("Health", {}).get("Status", "healthy") != "healthy":
            raise ValueError(f"current service is not healthy: {name}")
        image_id = output("docker", "inspect", "--format", "{{.Image}}", ids[0])
        tag = f"quanttrad-recovery:{prefix}-{index}"
        subprocess.run(["docker", "tag", image_id, tag], check=True, timeout=60)
        service["image"] = tag
        service.pop("build", None)
        service["pull_policy"] = "never"
    path.write_text(json.dumps(config, indent=2) + "\n")


if __name__ == "__main__":
    pin(Path(sys.argv[1]))
