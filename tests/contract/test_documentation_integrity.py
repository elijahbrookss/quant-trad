from __future__ import annotations

import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]

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


def test_selected_accepted_adrs_are_indexed_and_linked() -> None:
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
        "0048": "0048-gate-agent-mutation-and-research-promotion.md",
        "0064": "0064-use-one-code-owned-collector-operations-contract.md",
        "0065": "0065-use-explicit-frozen-check-admission-for-new-research-observations.md",
        "0066": "0066-scale-validation-to-consequence-and-trust-boundaries.md",
    }
    for adr_id, target in expected.items():
        assert rows[adr_id] == (target, "Accepted")
        repo_path = f"docs/architecture/decisions/{target}"
        assert (ROOT / repo_path).is_file()
        assert _frontmatter(repo_path)["status"] == "accepted"


def test_architecture_navigation_and_asset_lineage_are_current() -> None:
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


def test_ci_topology_matches_workflow_jobs() -> None:
    workflow = _yaml(".github/workflows/test.yaml")
    jobs = workflow["jobs"]
    expected_jobs = (
        "pr-suite",
        "frontend",
        "deployment-contract",
        "clean-database-bootstrap",
    )
    assert set(jobs) == set(expected_jobs)

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


def test_observability_docs_match_supported_shippers_and_dashboard_operations() -> None:
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

    makefile = _read("Makefile")
    assert re.search(
        r"^(?:grafana-backup|grafana-restore)\s*:",
        makefile,
        flags=re.MULTILINE,
    ) is None

    provider = _yaml("docker/grafana/provisioning/dashboards/dashboard.yml")[
        "providers"
    ][0]
    assert provider["allowUiUpdates"] is False
    assert provider["updateIntervalSeconds"] == 10

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
        assert not missing, f"{repo_path} is missing required documented facts: {missing}"
