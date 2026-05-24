from __future__ import annotations

import logging

from portal.backend.service.bots.runner_observability import (
    bot_id_from_container_name,
    build_clock_gap_diagnostic,
    docker_lifecycle_level,
    is_quant_trad_container_event,
    normalize_docker_container_event,
)


def test_clock_gap_diagnostic_detects_wall_or_monotonic_runner_pause() -> None:
    diagnostic = build_clock_gap_diagnostic(
        runner_id="backend.quanttrad",
        wall_delta_seconds=3677.0,
        monotonic_delta_seconds=5.0,
        expected_interval_seconds=5.0,
        threshold_seconds=30.0,
        detected_at="2026-05-19T07:57:54Z",
    )

    assert diagnostic is not None
    assert diagnostic["runner_id"] == "backend.quanttrad"
    assert diagnostic["gap_seconds"] == 3672.0
    assert diagnostic["wall_gap_seconds"] == 3672.0
    assert diagnostic["monotonic_gap_seconds"] == 0.0


def test_clock_gap_diagnostic_ignores_normal_scheduler_jitter() -> None:
    diagnostic = build_clock_gap_diagnostic(
        runner_id="backend.quanttrad",
        wall_delta_seconds=5.4,
        monotonic_delta_seconds=5.4,
        expected_interval_seconds=5.0,
        threshold_seconds=30.0,
    )

    assert diagnostic is None


def test_docker_container_event_normalizes_quant_trad_bot_lifecycle() -> None:
    event = normalize_docker_container_event(
        {
            "Type": "container",
            "Action": "die",
            "Actor": {
                "ID": "abcdef1234567890",
                "Attributes": {
                    "name": "some-runtime-name",
                    "image": "quanttrad-backend:dev",
                    "exitCode": "137",
                    "loki.job": "quanttrad",
                    "quanttrad.runtime": "bot",
                    "quanttrad.bot_id": "7bd70fd4-dd70-421d-8dfe-e0530d42b758",
                    "quanttrad.run_id": "run-1",
                },
            },
            "time": 1770000000,
        }
    )

    assert event is not None
    assert event["container_id"] == "abcdef123456"
    assert event["bot_id"] == "7bd70fd4-dd70-421d-8dfe-e0530d42b758"
    assert event["run_id"] == "run-1"
    assert event["container_family"] == "bot"
    assert event["exit_code"] == 137
    assert is_quant_trad_container_event(event) is True
    assert docker_lifecycle_level(event) == logging.WARNING


def test_bot_id_from_container_name_accepts_runtime_prefix_only() -> None:
    assert bot_id_from_container_name("/quant-trad-bots-bot-1") == "bot-1"
    assert bot_id_from_container_name("quant-trad-backend-1") is None
