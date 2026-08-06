from __future__ import annotations

import re
from pathlib import Path

import yaml


ACTIVE_CAPABILITY_PATHS = (
    Path("src/research_science"),
    Path("src/market_data/stream_enrollment.py"),
    Path("portal/backend/db/market_data_models.py"),
    Path("portal/backend/controller/market_data.py"),
    Path("portal/backend/service/market/collector_safety.py"),
    Path("portal/backend/service/market/collector_supervisor.py"),
    Path("portal/backend/service/market/continuous_stream_collector.py"),
    Path("portal/backend/service/market/market_structure_service.py"),
    Path("portal/backend/service/storage/repos/market_structure.py"),
    Path("cli/market_structure_proof.py"),
    Path("config/market_data"),
)

BANNED_RUNTIME_TERMS = re.compile(
    r"autonomous_research_campaign|"
    r"\bcampaign_id\b|"
    r"\bphase[_ -]?\d|"
    r"\bpost[_ -]?phase|"
    r"production_admitted|"
    r"production_admission|"
    r"continuous-admit",
    re.IGNORECASE,
)


def _files() -> list[Path]:
    selected: list[Path] = []
    for path in ACTIVE_CAPABILITY_PATHS:
        if path.is_file():
            selected.append(path)
        else:
            selected.extend(
                candidate
                for candidate in path.rglob("*")
                if candidate.suffix in {".py", ".json", ".yaml", ".yml"}
            )
    return sorted(selected)


def test_active_capability_contracts_do_not_encode_planning_nomenclature() -> None:
    violations: list[str] = []
    for path in _files():
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if BANNED_RUNTIME_TERMS.search(line):
                violations.append(f"{path}:{line_number}:{line.strip()}")

    assert not violations, "\n".join(violations)


def test_collector_safety_alerts_are_provisioned_against_canonical_tables() -> None:
    path = Path("docker/grafana/provisioning/alerting/collector-safety.yml")
    parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
    rules = parsed["groups"][0]["rules"]
    sql = "\n".join(
        str(item.get("model", {}).get("rawSql") or "")
        for rule in rules
        for item in rule["data"]
    )

    assert {rule["labels"]["severity"] for rule in rules} == {
        "warning",
        "critical",
    }
    assert "market.collector_safety_state" in sql
    assert "market.collector_safety_events" in sql
