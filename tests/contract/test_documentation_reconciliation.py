from __future__ import annotations

import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]

HISTORICAL_RECORDS = (
    "docs/engineering/canonical-fact-migration-backup.md",
    "docs/engineering/canonical-fact-migration-discovery.md",
    "docs/engineering/canonical-fact-migration-validation.md",
    "docs/engineering/collector-operations-discovery.md",
    "docs/engineering/collector-operations-validation.md",
    "docs/engineering/frontend-v2-operator-validation.md",
    "docs/plans/backtest-dataset-boundary.md",
    "docs/plans/platform-baseline-cleanup.md",
    "docs/research-campaigns/CHAINLINK_ETH_USD_BREAKOUT_V2_DOSSIER.md",
    "docs/research-campaigns/CHAINLINK_ETH_USD_BREAKOUT_V3_SIX_MONTH_DOSSIER.md",
)


def _read(repo_path: str) -> str:
    return (ROOT / repo_path).read_text(encoding="utf-8")


def _yaml(repo_path: str) -> dict:
    loaded = yaml.safe_load(_read(repo_path))
    assert isinstance(loaded, dict), f"{repo_path} must contain a YAML mapping"
    return loaded


def _frontmatter(repo_path: str) -> dict:
    text = _read(repo_path)
    if not text.startswith("---\n"):
        return {}
    match = re.match(r"\A---\n(.*?)\n---\n", text, flags=re.DOTALL)
    assert match is not None, f"{repo_path} has unterminated frontmatter"
    loaded = yaml.safe_load(match.group(1))
    assert isinstance(loaded, dict), f"{repo_path} frontmatter must be a mapping"
    return loaded


def _squash(text: str) -> str:
    return " ".join(text.split()).lower()


def test_manual_adr_index_matches_accepted_primary_records() -> None:
    index = _read("docs/architecture/decisions/README.md")
    rows = {
        adr_id: (target, status.strip())
        for adr_id, target, status in re.findall(
            r"^\| \[(\d{4})\]\(([^)]+)\) \| .*? \| ([^|]+) \|$",
            index,
            flags=re.MULTILINE,
        )
    }

    expected = {
        "0048": (
            "0048-gate-agent-mutation-and-research-promotion.md",
            "Accepted",
        ),
        "0064": (
            "0064-use-one-code-owned-collector-operations-contract.md",
            "Accepted",
        ),
    }
    for adr_id, (target, status) in expected.items():
        assert rows[adr_id] == (target, status)
        repo_path = f"docs/architecture/decisions/{target}"
        assert (ROOT / repo_path).is_file()
        assert _frontmatter(repo_path)["status"] == "accepted"


def test_historical_research_index_preserves_unavailable_history_and_v3_lineage() -> None:
    docs_home = _read("docs/index.md")
    history = _read("docs/research-campaigns/README.md")
    normalized = _squash(history)

    dead_targets = {
        "BTC_PERP_MARKET_STRUCTURE_CAMPAIGN_V1.md",
        "BTC_PERP_MARKET_STRUCTURE_CAMPAIGN_V2.md",
    }
    linked_targets = set(re.findall(r"\[[^]]+\]\(([^)]+)\)", history))
    assert linked_targets.isdisjoint(dead_targets)
    assert "research-campaigns/" not in docs_home

    v3_target = "BTC_PERP_MARKET_STRUCTURE_CAMPAIGN_V3_DOSSIER.md"
    assert v3_target in linked_targets
    assert (ROOT / "docs/research-campaigns" / v3_target).is_file()
    assert _frontmatter(f"docs/research-campaigns/{v3_target}")["status"] == "historical"
    assert "v1 and v2 source documents" in normalized
    assert "unavailable in this repository" in normalized
    assert "lineage gap" in normalized
    assert "does not reconstruct v1 or v2" in normalized


def test_architecture_navigation_and_unverified_asset_lineage_are_current() -> None:
    architecture = _read("docs/architecture/README.md")
    boundary_map = architecture.split("## Boundary Map", 1)[1].split(
        "## Decision Records", 1
    )[0]
    rows = {
        boundary.strip(): target
        for boundary, target in re.findall(
            r"^\| ([^|]+) \| \[[^]]+\]\(([^)]+)\) \|",
            boundary_map,
            flags=re.MULTILINE,
        )
    }
    expected = {
        "CLI setup": "cli/CLI_SETUP_BOUNDARY.md",
        "Operator console": "frontend/OPERATOR_CONSOLE_V2.md",
        "Research memory": "research-memory/RESEARCH_MEMORY_BOUNDARY.md",
    }
    for boundary, target in expected.items():
        assert rows[boundary] == target
        repo_path = f"docs/architecture/{target}"
        assert (ROOT / repo_path).is_file()
        assert _frontmatter(repo_path)["status"] == "active"

    asset_path = ROOT / "docs/assets/quant-trad-platform-flow.svg"
    asset_lineage = _squash(_read("docs/assets/README.md"))
    assert asset_path.is_file()
    assert "quant-trad-platform-flow.svg" in asset_lineage
    assert "source, generator, and reviewed lineage are unknown" in asset_lineage
    assert "retained, unreferenced as architecture evidence, and unverified" in asset_lineage
    assert (
        "does not establish that it is generated, current, canonical, or authoritative"
        in asset_lineage
    )


def test_only_completed_campaign_and_validation_records_were_historicized() -> None:
    assert len(HISTORICAL_RECORDS) == 10
    for repo_path in HISTORICAL_RECORDS:
        assert _frontmatter(repo_path)["status"] == "historical"
        assert re.search(
            r"> Historical (?:evidence|campaign) record[.:]",
            _read(repo_path),
        ), repo_path

    active_notice = "docs/research-campaigns/CHAINLINK_RESEARCH_BOUNDARY_LIMITATIONS.md"
    assert _frontmatter(active_notice).get("status") != "historical"
    notice = _read(active_notice)
    assert "## Canonical Rerun Contract" in notice
    assert "Any new result must report its actual population" in notice


def test_ci_topology_document_matches_the_exact_four_workflow_jobs() -> None:
    workflow = _yaml(".github/workflows/test.yaml")
    jobs = workflow["jobs"]
    expected_jobs = (
        "pr-suite",
        "frontend",
        "deployment-contract",
        "clean-database-bootstrap",
    )
    assert tuple(jobs) == expected_jobs

    bootstrap_steps = tuple(
        step["name"] for step in jobs["clean-database-bootstrap"]["steps"]
    )
    assert "Prove clean current-schema bootstrap" in bootstrap_steps
    assert "Run PostgreSQL-backed contract tests" in bootstrap_steps
    assert bootstrap_steps.index("Prove clean current-schema bootstrap") < bootstrap_steps.index(
        "Run PostgreSQL-backed contract tests"
    )

    topology = _read("docs/engineering/testing/ci-test-topology.md")
    documented_jobs = tuple(
        re.findall(r"^\| [1-4] \| `([^`]+)` \|", topology, flags=re.MULTILINE)
    )
    assert documented_jobs == expected_jobs
    normalized = _squash(topology)
    assert "the workflow defines exactly four jobs" in normalized
    assert "two sequential steps in this fourth job" in normalized
    assert "they are not separate workflow jobs" in normalized


def test_observability_topologies_and_dashboard_operations_stay_aligned() -> None:
    local_services = _yaml("docker/docker-compose.yml")["services"]
    server_services = _yaml("docker/docker-compose.server.yml")["services"]
    shipper_names = {"promtail", "alloy"}
    assert set(local_services).intersection(shipper_names) == {"promtail"}
    assert set(server_services).intersection(shipper_names) == {"alloy"}
    assert "grafana/promtail" in local_services["promtail"]["image"]
    assert "./promtail/config.yml:/etc/promtail/config.yml:ro" in local_services[
        "promtail"
    ]["volumes"]
    assert "grafana/alloy" in server_services["alloy"]["image"]
    assert "./alloy/config.alloy:/etc/alloy/config.alloy:ro" in server_services[
        "alloy"
    ]["volumes"]

    promtail = _yaml("docker/promtail/config.yml")
    assert [client["url"] for client in promtail["clients"]] == [
        "http://loki:3100/loki/api/v1/push"
    ]
    alloy = _read("docker/alloy/config.alloy")
    assert 'loki.source.docker "quanttrad"' in alloy
    assert 'loki.write "local"' in alloy
    assert 'url = "http://loki:3100/loki/api/v1/push"' in alloy

    provider = _yaml("docker/grafana/provisioning/dashboards/dashboard.yml")[
        "providers"
    ][0]
    assert provider["allowUiUpdates"] is False
    assert provider["updateIntervalSeconds"] == 10

    make_targets = set(
        re.findall(r"^([A-Za-z0-9_.-]+):", _read("Makefile"), flags=re.MULTILINE)
    )
    assert make_targets.isdisjoint({"grafana-backup", "grafana-restore"})

    expectations = {
        "docs/engineering/observability.md": (
            "one out-of-process shipper",
            "native-linux server uses grafana alloy",
            "local development composition uses its supported promtail service",
            "must not post directly to loki from the hot path",
            "make stack_profiles=observability stack-restart",
            "bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>",
        ),
        "docs/architecture/observability/OBSERVABILITY_BOUNDARY.md": (
            "one out-of-process shipper",
            "alloy owns this role on the native-linux server",
            "promtail is the supported local-development shipper",
            "must not activate a second or in-process ingress path",
            "make stack_profiles=observability stack-restart",
            "bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>",
        ),
        "docs/architecture/data/CONTINUOUS_COLLECTOR_RUNTIME.md": (
            "alloy is the native-server topology's only normal docker-to-loki shipper",
            "local development instead uses its supported promtail service",
            "in-process loki hot-path fallback",
        ),
        "docker/grafana/provisioning/dashboards/README.md": (
            "bash scripts/backup-grafana-dashboards.sh",
            "the restore source is the reviewed json in this directory",
            "make stack_profiles=observability stack-restart",
            "the local composition uses the supported promtail shipper",
            "bash scripts/automation/server_deploy.sh deploy <reviewed-commit-sha>",
            "uses alloy as its only normal docker-to-loki shipper",
        ),
    }
    for repo_path, required_facts in expectations.items():
        normalized = _squash(_read(repo_path))
        missing = [fact for fact in required_facts if fact not in normalized]
        assert not missing, f"{repo_path} is missing reconciled facts: {missing}"


def test_forward_correction_preserves_gc026_lineage_without_claiming_result() -> None:
    frozen_candidates = _read(
        "docs/plans/documentation-reconciliation/guarantee-candidates.md"
    )
    frozen_row = next(
        line for line in frozen_candidates.splitlines() if line.startswith("| `QT-GC-026`")
    )
    frozen_locator = "docs/architecture/data/MARKET_STRUCTURE_DATA_PLANE.md:380,1051"
    assert f"A: `{frozen_locator}`" in frozen_row

    ledger = _read(
        "docs/plans/documentation-reconciliation/phase-3-forward-corrections.md"
    )
    normalized = _squash(ledger)
    assert frozen_locator.lower() in normalized
    assert (
        "docs/architecture/decisions/"
        "0053-use-tiered-market-structure-archive-and-replay-boundary.md"
        "@fb5814de:128-132"
    ) in normalized
    assert (
        "docs/architecture/data/market_structure_data_plane.md@fb5814de:1044"
    ) in normalized
    assert "trade coverage discrimination" in normalized
    assert "describes evidence that must be produced; it is not a proof result" in normalized
    assert "does not execute or satisfy `qt-proof-410`" in normalized
    assert "does not execute or close `qt-rem-410`" in normalized
    assert "does not create an attestation" in normalized
    assert "does not reclassify or activate `qt-guar-proven-zero-trade-coverage`" in normalized
