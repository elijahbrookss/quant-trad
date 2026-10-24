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
    from scripts.docs import build_architecture_index as architecture_index

    navigation = _read("docs/architecture/decisions/README.md")
    assert "ARCHITECTURE_COMPONENT_INDEX.md" in navigation
    catalog = architecture_index.build_catalog(ROOT)
    indexed = {entry.repo_path: entry for entry in catalog.components}

    expected = {
        "0048": "0048-gate-agent-mutation-and-research-promotion.md",
        "0064": "0064-use-one-code-owned-collector-operations-contract.md",
        "0065": "0065-use-explicit-frozen-check-admission-for-new-research-observations.md",
        "0066": "0066-scale-validation-to-consequence-and-trust-boundaries.md",
    }
    for target in expected.values():
        repo_path = f"docs/architecture/decisions/{target}"
        assert repo_path in indexed
        assert (ROOT / repo_path).is_file()
        assert _frontmatter(repo_path)["status"] == "accepted"


def test_architecture_navigation_and_diagram_sources_are_current() -> None:
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

    # Unknown, unused exports are removed rather than maintained as architecture.
    assert not (ROOT / "docs/assets/quant-trad-platform-flow.svg").exists()
    architecture_root = ROOT / "docs/architecture"
    for svg in architecture_root.rglob("*.svg"):
        assert svg.with_suffix(".mmd").is_file(), f"orphan diagram export: {svg}"


def test_ci_topology_matches_workflow_jobs() -> None:
    workflow = _yaml(".github/workflows/test.yaml")
    jobs = workflow["jobs"]
    expected_jobs = (
        "pr-suite",
        "frontend",
        "deployment-contract",
        "clean-database-bootstrap",
        "deployment-rehearsal",
        "committed-recovery",
    )
    assert workflow["permissions"] == {"contents":"read"}
    for job in jobs.values():
        for step in job.get("steps",[]):
            if step.get("uses")=="actions/checkout@v4":
                assert step["with"]["persist-credentials"] is False
    proof_jobs = {"deployment-proof-" + name for name in ("build", "alerts", "lifecycle", "restore", "core", "online")}
    assert set(jobs) == set(expected_jobs) | {"changes", "docs", "ci-required"} | proof_jobs
    assert jobs["ci-required"]["if"] == "${{ always() }}"
    assert set(jobs["ci-required"]["needs"]) == set(expected_jobs) | {"changes", "docs"}
    for job in set(expected_jobs) - {"deployment-contract"}:
        assert jobs[job]["needs"] == "changes"
    assert set(jobs["deployment-contract"]["needs"]) == proof_jobs | {"changes"}

    bootstrap_steps = tuple(
        step["name"] for step in jobs["clean-database-bootstrap"]["steps"]
    )
    assert "Prove clean current-schema bootstrap" in bootstrap_steps
    assert "Run PostgreSQL-backed contract tests" in bootstrap_steps
    assert "Verify PostgreSQL filesystem namespace" in bootstrap_steps
    assert bootstrap_steps.index("Prove clean current-schema bootstrap") < bootstrap_steps.index(
        "Run PostgreSQL-backed contract tests"
    )

    assert bootstrap_steps.index("Prove clean current-schema bootstrap") < bootstrap_steps.index(
        "Verify PostgreSQL filesystem namespace"
    ) < bootstrap_steps.index("Run PostgreSQL-backed contract tests")
    commands = {step["name"]: step.get("run", "") for step in jobs["clean-database-bootstrap"]["steps"]}
    namespace_test = "tests/test_market_data/test_header_namespace_db.py"
    assert f"./scripts/ci/run_test_suite.sh db {namespace_test}" in commands["Verify PostgreSQL filesystem namespace"]
    assert f"--ignore={namespace_test}" in commands["Run PostgreSQL-backed contract tests"]
    reader_floor_step = next(
        step for step in jobs["clean-database-bootstrap"]["steps"]
        if step.get("name") == "Prove frozen baseline refuses archive-generation reader floor"
    )
    reader_floor = reader_floor_step["run"]
    assert reader_floor_step["env"]["QT_ARCHIVE_GENERATION_BASELINE_REVISION"] == (
        "91d5a0564c0aa9e04159d6faaef670ae809fed60"
    )
    assert bootstrap_steps.index("Run PostgreSQL-backed contract tests") < bootstrap_steps.index(
        "Prove frozen baseline refuses archive-generation reader floor"
    )
    assert "--target production" in reader_floor
    assert "prove_archive_generation_reader_floor.py" in reader_floor
    assert "--expect ready" in reader_floor and "--expect refused" in reader_floor
    assert "PYTHONPATH=src:. python -m scripts.db.manual_migration_fact_archive_generations_v1" in reader_floor
    assert "--network host" in reader_floor and "--read-only" in reader_floor
    assert "QT_EXPECTED_SOURCE_REVISION" in reader_floor
    clean_checkout = next(
        step for step in jobs["clean-database-bootstrap"]["steps"]
        if step.get("name") == "Checkout repository"
    )
    assert clean_checkout["with"]["fetch-depth"] == 0

    runtime_matrix = jobs["committed-recovery"]["strategy"]["matrix"]["runtime"]
    assert runtime_matrix == ["current", "24357ff387776822f676ae2a8b1cce7f209c313e"]
    recovery_steps = {step.get("name"): step for step in jobs["committed-recovery"]["steps"]}
    attestation = recovery_steps["Attest and pack disposable qualification evidence"]["run"]
    assert "GITHUB_SHA" in attestation and "GITHUB_RUN_ATTEMPT" in attestation
    assert "qualification-source.json" in attestation and ".qt-source-attestation.json" in attestation
    import json
    selections = json.loads(_read('scripts/ci/online_proof_selection.json'))
    assert "tests/test_market_data/test_storage_end_to_end_db.py" in selections
    assert (
        "tests/test_market_data/test_archive_reference_placement_db.py::"
        "test_generation_migration_inherits_named_reference_tablespace"
    ) in selections
    assert jobs['deployment-proof-online']['strategy']['matrix']['shard'] == list(range(6))
    assert jobs['deployment-proof-online']['strategy']['max-parallel'] == 4
    assert any(step.get('name') == 'Attest complete deployment proof coverage'
               for step in jobs['deployment-contract']['steps'])

    topology = _read("docs/engineering/testing/ci-test-topology.md")
    documented_jobs = tuple(
        re.findall(r"^\| [1-6] \| `([^`]+)` \|", topology, flags=re.MULTILINE)
    )
    assert documented_jobs == expected_jobs
    normalized = _squash(topology)
    assert "the workflow defines fifteen jobs" in normalized
    assert "five sequential proof steps in this fourth job" in normalized
    assert "prove encrypted incremental database and archive recovery" in normalized
    assert "Prove encrypted incremental database and archive recovery" in bootstrap_steps
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


def test_repository_documentation_links_and_anchors_resolve() -> None:
    from urllib.parse import unquote, urlsplit

    pages = list((ROOT / "docs").rglob("*.md")) + [
        ROOT / "README.md", ROOT / "AGENTS.md",
        ROOT / "portal/frontend/README.md",
        ROOT / "docker/grafana/provisioning/dashboards/README.md",
        *sorted((ROOT / "src/indicators").rglob("*.md")),
    ]
    errors = []
    for page in pages:
        text = re.sub(r"```.*?```|~~~.*?~~~", "", page.read_text(encoding="utf-8"), flags=re.DOTALL)
        targets = re.findall(r"\[[^]\n]+\]\(([^)\n]+)\)", text)
        targets += re.findall(r'^\s*\[[^]]+\]:\s*(\S+)', text, flags=re.MULTILINE)
        for target in targets:
            target = target.split(' "', 1)[0].strip("<>")
            parsed = urlsplit(target)
            if parsed.scheme or target.startswith("//"):
                continue
            resolved = (page.parent / unquote(parsed.path)).resolve() if parsed.path else page
            context = f"{page.relative_to(ROOT)}: {target}"
            if not resolved.exists():
                errors.append(context + " (missing target)")
                continue
            if not parsed.fragment or resolved.suffix != ".md":
                continue
            body = re.sub(r"```.*?```|~~~.*?~~~", "", resolved.read_text(encoding="utf-8"), flags=re.DOTALL)
            anchors = set(re.findall(r'<a\s+(?:id|name)=["\']([^"\']+)', body))
            counts = {}
            for heading in re.findall(r"^#{1,6} (.+?)\s*#*\s*$", body, flags=re.MULTILINE):
                heading = re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", heading)
                slug = re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-")
                number = counts.get(slug, 0)
                counts[slug] = number + 1
                anchors.add(f"{slug}-{number}" if number else slug)
            if unquote(parsed.fragment) not in anchors:
                errors.append(context + " (missing anchor)")
    assert not errors, "\n".join(errors)
