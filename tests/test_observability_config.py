from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_promtail_indexes_runtime_not_run_identity():
    text = (ROOT / "docker/promtail/config.yml").read_text(encoding="utf-8")

    assert "regex: quanttrad" in text
    assert "target_label: runtime" in text
    assert "target_label: run_id" not in text
    assert "target_label: bot_id" not in text


def test_loki_has_durable_local_volume():
    compose = yaml.safe_load((ROOT / "docker/docker-compose.yml").read_text(encoding="utf-8"))

    loki_volumes = set(compose["services"]["loki"]["volumes"])
    root_volumes = set(compose["volumes"])

    assert "loki-data:/loki" in loki_volumes
    assert "loki-data" in root_volumes


def test_dev_config_does_not_enable_direct_loki_handler():
    config = yaml.safe_load((ROOT / "config/dev.yaml").read_text(encoding="utf-8"))

    assert config["logging"]["loki_url"] is None
