from __future__ import annotations

from importlib import metadata
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from scripts.provenance import source_tree_hash


ROOT = Path(__file__).resolve().parents[2]


def _requirements(path: Path) -> list[Requirement]:
    return [
        Requirement(line)
        for raw in path.read_text(encoding="utf-8").splitlines()
        if (line := raw.strip()) and not line.startswith("#")
    ]


def test_deployment_lock_is_exact_complete_and_installed() -> None:
    direct = _requirements(ROOT / "requirements.txt")
    locked = _requirements(ROOT / "requirements.lock")
    locked_by_name = {canonicalize_name(row.name): row for row in locked}

    assert len(locked_by_name) == len(locked)
    assert {canonicalize_name(row.name) for row in direct} <= set(locked_by_name)
    for requirement in direct:
        locked_requirement = locked_by_name[canonicalize_name(requirement.name)]
        locked_version = next(iter(locked_requirement.specifier)).version
        if str(requirement.specifier):
            assert requirement.specifier.contains(locked_version, prereleases=True)
    for name, requirement in locked_by_name.items():
        assert len(requirement.specifier) == 1
        specifier = next(iter(requirement.specifier))
        assert specifier.operator == "=="
        installed = metadata.version(name)
        assert installed == specifier.version


def test_deployment_lock_is_part_of_runtime_source_attestation() -> None:
    assert "requirements.lock" in source_tree_hash.ROOT_FILES
