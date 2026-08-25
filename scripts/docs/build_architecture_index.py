#!/usr/bin/env python3
"""Validate architecture metadata and build its deterministic human index."""

from __future__ import annotations

import argparse
import posixpath
import re
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Sequence

ROOT = Path(__file__).resolve().parents[2]
ARCHITECTURE_DIR = PurePosixPath("docs/architecture")
INDEX_REPO_PATH = ARCHITECTURE_DIR / "ARCHITECTURE_COMPONENT_INDEX.md"
LEGACY_ALLOWLIST_REPO_PATH = ARCHITECTURE_DIR / "ARCHITECTURE_METADATA_V1_ALLOWLIST.txt"

EXCLUDED_ARCHITECTURE_DOCS = frozenset(
    {
        "README.md",
        "decisions/README.md",
        "ARCHITECTURE_COMPONENT_INDEX.md",
    }
)

LEGACY_KEYS = frozenset(
    {
        "component",
        "subsystem",
        "layer",
        "doc_type",
        "status",
        "tags",
        "code_paths",
    }
)
V2_KEYS = LEGACY_KEYS | frozenset(
    {
        "metadata_version",
        "semantic_owner",
        "required_reviewers",
        "module_contracts",
    }
)
MODULE_CONTRACT_KEYS = frozenset(
    {
        "module_contract_version",
        "contract_kind",
        "owning_component",
        "component_scope",
        "semantic_owner",
        "status",
    }
)

DOC_TYPES = frozenset({"adr", "architecture", "validation"})
COMPONENT_STATUSES = frozenset(
    {"accepted", "active", "draft", "historical", "superseded"}
)
MODULE_CONTRACT_STATUSES = frozenset(
    {"active", "draft", "historical", "superseded"}
)
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
KEY_RE = re.compile(r"^([a-z][a-z0-9_]*):(?:[ \t]*(.*))?$")
DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:")

MetadataValue = str | list[str]


class ArchitectureMetadataError(ValueError):
    """One or more deterministic architecture metadata validation errors."""

    def __init__(self, errors: Sequence[str]):
        self.errors = tuple(sorted(set(errors)))
        super().__init__("\n".join(self.errors))


@dataclass(frozen=True)
class ComponentEntry:
    repo_path: str
    component: str
    subsystem: str
    layer: str
    doc_type: str
    status: str
    tags: tuple[str, ...]
    code_paths: tuple[str, ...]
    metadata_version: int
    semantic_owner: str | None
    required_reviewers: tuple[str, ...]
    module_contract_paths: tuple[str, ...]


@dataclass(frozen=True)
class ModuleContractEntry:
    repo_path: str
    owning_component: str
    component_scope: str
    semantic_owner: str
    status: str


@dataclass(frozen=True)
class ArchitectureCatalog:
    components: tuple[ComponentEntry, ...]
    module_contracts: tuple[ModuleContractEntry, ...]
    legacy_paths: tuple[str, ...]


def _filesystem_path(root: Path, repo_path: str | PurePosixPath) -> Path:
    return root.joinpath(*PurePosixPath(repo_path).parts)


def _repo_path(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _parse_frontmatter(path: Path, repo_path: str) -> dict[str, MetadataValue]:
    """Parse QT's constrained scalar/block-list frontmatter without YAML coercion."""

    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "---":
        raise ArchitectureMetadataError(
            [f"{repo_path}: missing opening frontmatter fence"]
        )

    data: dict[str, MetadataValue] = {}
    current_list_key: str | None = None
    errors: list[str] = []
    closed = False

    for line_number, line in enumerate(lines[1:], start=2):
        if line == "---":
            closed = True
            break
        if not line:
            continue

        if line.startswith("  - "):
            if current_list_key is None:
                errors.append(
                    f"{repo_path}:{line_number}: list item has no list field"
                )
                continue
            item = line[4:].strip()
            if not item:
                errors.append(
                    f"{repo_path}:{line_number}: list item must not be empty"
                )
                continue
            value = data[current_list_key]
            assert isinstance(value, list)
            value.append(item)
            continue

        match = KEY_RE.fullmatch(line)
        if match is None:
            errors.append(f"{repo_path}:{line_number}: malformed frontmatter line")
            current_list_key = None
            continue

        key = match.group(1)
        raw_value = (match.group(2) or "").strip()
        if key in data:
            errors.append(
                f"{repo_path}:{line_number}: duplicate frontmatter key: {key}"
            )
            current_list_key = None
            continue
        if raw_value == "" or raw_value == "[]":
            data[key] = []
            current_list_key = key if raw_value == "" else None
        else:
            data[key] = raw_value
            current_list_key = None

    if not closed:
        errors.append(f"{repo_path}: missing closing frontmatter fence")
    if errors:
        raise ArchitectureMetadataError(errors)
    return data


def _validate_key_set(
    metadata: dict[str, MetadataValue],
    expected: frozenset[str],
    repo_path: str,
    errors: list[str],
) -> None:
    actual = set(metadata)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing:
        errors.append(
            f"{repo_path}: missing frontmatter keys: {','.join(missing)}"
        )
    if unknown:
        errors.append(
            f"{repo_path}: unknown frontmatter keys: {','.join(unknown)}"
        )


def _scalar(
    metadata: dict[str, MetadataValue],
    key: str,
    repo_path: str,
    errors: list[str],
) -> str:
    value = metadata.get(key)
    if not isinstance(value, str) or not value:
        errors.append(f"{repo_path}: {key} must be a nonempty scalar")
        return ""
    return value


def _string_list(
    metadata: dict[str, MetadataValue],
    key: str,
    repo_path: str,
    errors: list[str],
    *,
    allow_empty: bool,
) -> list[str]:
    value = metadata.get(key)
    if not isinstance(value, list):
        errors.append(f"{repo_path}: {key} must be a block list")
        return []
    if not allow_empty and not value:
        errors.append(f"{repo_path}: {key} must not be empty")
    duplicates = sorted({item for item in value if value.count(item) > 1})
    if duplicates:
        errors.append(
            f"{repo_path}: {key} contains duplicates: {','.join(duplicates)}"
        )
    return value


def _validate_slug(
    value: str, field: str, repo_path: str, errors: list[str]
) -> None:
    if value and SLUG_RE.fullmatch(value) is None:
        errors.append(
            f"{repo_path}: {field} must be a kebab-case slug: {value}"
        )


def _validate_repo_path(
    value: str,
    field: str,
    repo_path: str,
    root: Path,
    errors: list[str],
    *,
    require_markdown: bool = False,
) -> bool:
    if not value:
        return False
    pure = PurePosixPath(value)
    if (
        "\\" in value
        or DRIVE_PATH_RE.match(value)
        or pure.is_absolute()
        or any(part in {".", ".."} for part in pure.parts)
        or pure.as_posix() != value
    ):
        errors.append(
            f"{repo_path}: {field} must be a normalized "
            f"repository-relative path: {value}"
        )
        return False
    if require_markdown and pure.suffix.lower() != ".md":
        errors.append(
            f"{repo_path}: {field} must reference a Markdown file: {value}"
        )
        return False

    root_resolved = root.resolve()
    resolved = _filesystem_path(root, pure).resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError:
        errors.append(f"{repo_path}: {field} escapes the repository: {value}")
        return False
    if not resolved.exists():
        errors.append(f"{repo_path}: {field} does not exist: {value}")
        return False
    if require_markdown and not resolved.is_file():
        errors.append(
            f"{repo_path}: {field} must reference a Markdown file: {value}"
        )
        return False
    return True


def _load_legacy_allowlist(root: Path, errors: list[str]) -> tuple[str, ...]:
    path = _filesystem_path(root, LEGACY_ALLOWLIST_REPO_PATH)
    repo_path = LEGACY_ALLOWLIST_REPO_PATH.as_posix()
    if not path.is_file():
        errors.append(f"{repo_path}: legacy metadata allowlist is missing")
        return ()

    entries: list[str] = []
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        value = raw_line.strip()
        if not value or value.startswith("#"):
            continue
        if value in entries:
            errors.append(
                f"{repo_path}:{line_number}: duplicate legacy path: {value}"
            )
            continue
        pure = PurePosixPath(value)
        if (
            "\\" in value
            or pure.is_absolute()
            or any(part in {".", ".."} for part in pure.parts)
            or pure.as_posix() != value
            or not value.startswith(f"{ARCHITECTURE_DIR.as_posix()}/")
            or pure.suffix.lower() != ".md"
        ):
            errors.append(
                f"{repo_path}:{line_number}: invalid legacy path: {value}"
            )
            continue
        entries.append(value)

    if entries != sorted(entries):
        errors.append(f"{repo_path}: legacy paths must be sorted")
    return tuple(entries)


def _component_paths(root: Path) -> list[Path]:
    architecture_dir = _filesystem_path(root, ARCHITECTURE_DIR)
    paths: list[Path] = []
    for path in architecture_dir.rglob("*.md"):
        relative = path.relative_to(architecture_dir).as_posix()
        if relative in EXCLUDED_ARCHITECTURE_DOCS:
            continue
        paths.append(path)
    return sorted(
        paths,
        key=lambda item: item.relative_to(architecture_dir).as_posix(),
    )


def _validate_component(
    path: Path, root: Path, errors: list[str]
) -> ComponentEntry | None:
    repo_path = _repo_path(path, root)
    try:
        metadata = _parse_frontmatter(path, repo_path)
    except ArchitectureMetadataError as exc:
        errors.extend(exc.errors)
        return None

    version_value = metadata.get("metadata_version")
    if version_value is None:
        metadata_version = 1
        expected_keys = LEGACY_KEYS
    else:
        metadata_version = 2
        expected_keys = V2_KEYS
        if not isinstance(version_value, str) or version_value != "2":
            errors.append(
                f"{repo_path}: metadata_version must be the scalar 2"
            )

    _validate_key_set(metadata, expected_keys, repo_path, errors)

    component = _scalar(metadata, "component", repo_path, errors)
    subsystem = _scalar(metadata, "subsystem", repo_path, errors)
    layer = _scalar(metadata, "layer", repo_path, errors)
    doc_type = _scalar(metadata, "doc_type", repo_path, errors)
    status = _scalar(metadata, "status", repo_path, errors)
    tags = _string_list(
        metadata, "tags", repo_path, errors, allow_empty=False
    )
    code_paths = _string_list(
        metadata, "code_paths", repo_path, errors, allow_empty=False
    )

    for field, value in (
        ("component", component),
        ("subsystem", subsystem),
        ("layer", layer),
    ):
        _validate_slug(value, field, repo_path, errors)
    if doc_type and doc_type not in DOC_TYPES:
        errors.append(f"{repo_path}: unsupported doc_type: {doc_type}")
    if status and status not in COMPONENT_STATUSES:
        errors.append(f"{repo_path}: unsupported status: {status}")
    for tag in tags:
        _validate_slug(tag, "tags item", repo_path, errors)
    for code_path in code_paths:
        _validate_repo_path(
            code_path, "code_paths item", repo_path, root, errors
        )

    semantic_owner: str | None = None
    required_reviewers: list[str] = []
    module_contract_paths: list[str] = []
    if metadata_version == 2:
        semantic_owner = _scalar(
            metadata, "semantic_owner", repo_path, errors
        )
        required_reviewers = _string_list(
            metadata,
            "required_reviewers",
            repo_path,
            errors,
            allow_empty=False,
        )
        module_contract_paths = _string_list(
            metadata,
            "module_contracts",
            repo_path,
            errors,
            allow_empty=True,
        )
        _validate_slug(
            semantic_owner, "semantic_owner", repo_path, errors
        )
        for reviewer in required_reviewers:
            _validate_slug(
                reviewer, "required_reviewers item", repo_path, errors
            )
        if required_reviewers != sorted(required_reviewers):
            errors.append(
                f"{repo_path}: required_reviewers must be sorted"
            )
        if module_contract_paths != sorted(module_contract_paths):
            errors.append(f"{repo_path}: module_contracts must be sorted")
        if module_contract_paths and (
            doc_type != "architecture" or status != "active"
        ):
            errors.append(
                f"{repo_path}: only active architecture docs may own "
                "current module contracts"
            )
        for module_path in module_contract_paths:
            _validate_repo_path(
                module_path,
                "module_contracts item",
                repo_path,
                root,
                errors,
                require_markdown=True,
            )
            module_pure = PurePosixPath(module_path)
            in_scope = False
            for code_path in code_paths:
                code_pure = PurePosixPath(code_path)
                if module_pure == code_pure:
                    in_scope = True
                    break
                if (
                    _filesystem_path(root, code_pure).is_dir()
                    and code_pure in module_pure.parents
                ):
                    in_scope = True
                    break
            if not in_scope:
                errors.append(
                    f"{repo_path}: module contract is outside declared "
                    f"code_paths: {module_path}"
                )

    return ComponentEntry(
        repo_path=repo_path,
        component=component,
        subsystem=subsystem,
        layer=layer,
        doc_type=doc_type,
        status=status,
        tags=tuple(tags),
        code_paths=tuple(code_paths),
        metadata_version=metadata_version,
        semantic_owner=semantic_owner,
        required_reviewers=tuple(required_reviewers),
        module_contract_paths=tuple(module_contract_paths),
    )


def _validate_module_contracts(
    components: Sequence[ComponentEntry],
    root: Path,
    errors: list[str],
) -> tuple[ModuleContractEntry, ...]:
    owners_by_path: dict[str, list[ComponentEntry]] = {}
    for component in components:
        for contract_path in component.module_contract_paths:
            owners_by_path.setdefault(contract_path, []).append(component)

    entries: list[ModuleContractEntry] = []
    for contract_path in sorted(owners_by_path):
        owners = owners_by_path[contract_path]
        if len(owners) != 1:
            owner_names = ",".join(
                sorted(owner.component for owner in owners)
            )
            errors.append(
                f"{contract_path}: module contract must have one owning "
                f"component; found {owner_names}"
            )
            continue
        owner = owners[0]
        path = _filesystem_path(root, contract_path)
        if not path.is_file():
            continue
        try:
            metadata = _parse_frontmatter(path, contract_path)
        except ArchitectureMetadataError as exc:
            errors.extend(exc.errors)
            continue

        _validate_key_set(
            metadata, MODULE_CONTRACT_KEYS, contract_path, errors
        )
        version = _scalar(
            metadata, "module_contract_version", contract_path, errors
        )
        contract_kind = _scalar(
            metadata, "contract_kind", contract_path, errors
        )
        owning_component = _scalar(
            metadata, "owning_component", contract_path, errors
        )
        component_scope = _scalar(
            metadata, "component_scope", contract_path, errors
        )
        semantic_owner = _scalar(
            metadata, "semantic_owner", contract_path, errors
        )
        status = _scalar(metadata, "status", contract_path, errors)

        if version and version != "1":
            errors.append(
                f"{contract_path}: module_contract_version must be "
                "the scalar 1"
            )
        if contract_kind and contract_kind != "source-module":
            errors.append(
                f"{contract_path}: contract_kind must be source-module"
            )
        _validate_slug(
            component_scope, "component_scope", contract_path, errors
        )
        _validate_slug(
            semantic_owner, "semantic_owner", contract_path, errors
        )
        if owning_component != owner.component:
            errors.append(
                f"{contract_path}: owning_component must match "
                f"{owner.component}: {owning_component}"
            )
        if semantic_owner != owner.semantic_owner:
            errors.append(
                f"{contract_path}: semantic_owner must match "
                f"{owner.semantic_owner}: {semantic_owner}"
            )
        if status and status not in MODULE_CONTRACT_STATUSES:
            errors.append(
                f"{contract_path}: unsupported module contract status: "
                f"{status}"
            )

        entries.append(
            ModuleContractEntry(
                repo_path=contract_path,
                owning_component=owning_component,
                component_scope=component_scope,
                semantic_owner=semantic_owner,
                status=status,
            )
        )
    return tuple(entries)


def build_catalog(root: Path = ROOT) -> ArchitectureCatalog:
    """Validate the complete component set and return its deterministic catalog."""

    root = root.resolve()
    errors: list[str] = []
    legacy_allowlist = _load_legacy_allowlist(root, errors)
    components: list[ComponentEntry] = []
    for path in _component_paths(root):
        entry = _validate_component(path, root, errors)
        if entry is not None:
            components.append(entry)

    components_by_slug: dict[str, list[str]] = {}
    for entry in components:
        components_by_slug.setdefault(entry.component, []).append(
            entry.repo_path
        )
    for component, paths in sorted(components_by_slug.items()):
        if component and len(paths) > 1:
            errors.append(
                f"duplicate component slug {component}: "
                f"{','.join(sorted(paths))}"
            )

    legacy_paths = tuple(
        sorted(
            entry.repo_path
            for entry in components
            if entry.metadata_version == 1
        )
    )
    legacy_set = set(legacy_paths)
    allowlist_set = set(legacy_allowlist)
    for path in sorted(legacy_set - allowlist_set):
        errors.append(f"{path}: legacy metadata path is not allowlisted")
    for path in sorted(allowlist_set - legacy_set):
        if any(
            entry.repo_path == path and entry.metadata_version == 2
            for entry in components
        ):
            errors.append(
                f"{path}: version 2 document remains in the legacy allowlist"
            )
        else:
            errors.append(f"{path}: stale legacy allowlist entry")

    module_contracts = _validate_module_contracts(
        components, root, errors
    )
    if errors:
        raise ArchitectureMetadataError(errors)

    return ArchitectureCatalog(
        components=tuple(
            sorted(
                components,
                key=lambda item: (item.subsystem, item.component),
            )
        ),
        module_contracts=tuple(
            sorted(module_contracts, key=lambda item: item.repo_path)
        ),
        legacy_paths=legacy_paths,
    )


def _architecture_relative(repo_path: str) -> str:
    prefix = f"{ARCHITECTURE_DIR.as_posix()}/"
    if not repo_path.startswith(prefix):
        raise ValueError(f"not an architecture path: {repo_path}")
    return repo_path[len(prefix) :]


def _reviewer_text(reviewers: Sequence[str]) -> str:
    return ", ".join(f"`{reviewer}`" for reviewer in reviewers)


def render_index(catalog: ArchitectureCatalog) -> str:
    """Render the complete generated human view with stable ordering and LF endings."""

    components = catalog.components
    v2_components = [
        entry for entry in components if entry.metadata_version == 2
    ]

    lines: list[str] = [
        "# Architecture Component Index",
        "",
        "Generated by `python scripts/docs/build_architecture_index.py`.",
        "",
        "This is a derived discovery and review-routing view. Architecture prose remains",
        "explanatory; reviewer roles are requirements rather than proof of approval; and",
        "source-module contract discovery does not override platform contracts or activate",
        "a guarantee.",
        "",
        "## Metadata Rollout",
        "",
        "| State | Count |",
        "|---|---:|",
        f"| Metadata version 2 | {len(v2_components)} |",
        "| [Legacy allowlisted](ARCHITECTURE_METADATA_V1_ALLOWLIST.txt) | "
        f"{len(catalog.legacy_paths)} |",
        f"| **Total component docs** | **{len(components)}** |",
        "",
        "Legacy `subsystem` values are not inferred as semantic owners. Each legacy row",
        "remains unresolved until its complete owner-reviewed metadata migration.",
        "",
        "## Components",
        "",
        "| Component | Subsystem | Layer | Status | Metadata | Doc |",
        "|---|---|---|---|---|---|",
    ]
    for entry in components:
        relative = _architecture_relative(entry.repo_path)
        metadata_state = (
            "v2" if entry.metadata_version == 2 else "legacy"
        )
        lines.append(
            f"| {entry.component} | {entry.subsystem} | {entry.layer} | "
            f"{entry.status} | {metadata_state} | "
            f"[{PurePosixPath(relative).name}]({relative}) |"
        )

    lines.extend(
        [
            "",
            "## Semantic Ownership And Required Review",
            "",
            "These rows reproduce version 2 routing metadata. A listed role is not an",
            "authenticated identity or evidence that review occurred.",
            "",
        ]
    )
    if v2_components:
        lines.extend(
            [
                "| Component | Semantic Owner | Required Reviewers | Doc |",
                "|---|---|---|---|",
            ]
        )
        for entry in v2_components:
            relative = _architecture_relative(entry.repo_path)
            lines.append(
                f"| {entry.component} | `{entry.semantic_owner}` | "
                f"{_reviewer_text(entry.required_reviewers)} | "
                f"[{PurePosixPath(relative).name}]({relative}) |"
            )
    else:
        lines.append(
            "No component has completed metadata version 2 migration."
        )

    lines.extend(
        [
            "",
            "## Structurally Discovered Source-Module Contracts",
            "",
            "A row proves only that the owning component link, declared scope, owner, and",
            "lifecycle metadata are structurally valid. Required review and guarantee",
            "activation remain separate decisions.",
            "",
        ]
    )
    if catalog.module_contracts:
        lines.extend(
            [
                "| Contract | Component Scope | Semantic Owner | Owning "
                "Component | Lifecycle | Eligibility |",
                "|---|---|---|---|---|---|",
            ]
        )
        for contract in catalog.module_contracts:
            link = posixpath.relpath(
                contract.repo_path, ARCHITECTURE_DIR.as_posix()
            )
            eligibility = (
                "current after required review"
                if contract.status == "active"
                else "lineage only"
            )
            lines.append(
                f"| [{PurePosixPath(contract.repo_path).name}]({link}) | "
                f"`{contract.component_scope}` | "
                f"`{contract.semantic_owner}` | "
                f"{contract.owning_component} | {contract.status} | "
                f"{eligibility} |"
            )
    else:
        lines.append(
            "No source-module contract is structurally discovered yet."
        )

    lines.extend(
        [
            "",
            "## Code Path Coverage",
            "",
            "Shared mappings are preserved as navigation and coverage; they do not assign",
            "exclusive file ownership.",
            "",
            "| Code Path | Component | Doc |",
            "|---|---|---|",
        ]
    )
    for entry in sorted(components, key=lambda item: item.component):
        relative = _architecture_relative(entry.repo_path)
        for code_path in entry.code_paths:
            lines.append(
                f"| `{code_path}` | {entry.component} | "
                f"[{PurePosixPath(relative).name}]({relative}) |"
            )

    lines.extend(
        [
            "",
            "## Tag Coverage",
            "",
            "| Tag | Components |",
            "|---|---|",
        ]
    )
    tag_map: dict[str, set[str]] = {}
    for entry in components:
        for tag in entry.tags:
            tag_map.setdefault(tag, set()).add(entry.component)
    for tag in sorted(tag_map):
        lines.append(
            f"| `{tag}` | {', '.join(sorted(tag_map[tag]))} |"
        )

    return "\n".join(lines) + "\n"


def _index_path(root: Path) -> Path:
    return _filesystem_path(root, INDEX_REPO_PATH)


def main(
    argv: Sequence[str] | None = None, *, root: Path = ROOT
) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "validate metadata and fail if the checked-in index is not "
            "byte-for-byte current"
        ),
    )
    args = parser.parse_args(argv)

    try:
        catalog = build_catalog(root)
    except ArchitectureMetadataError as exc:
        print("architecture metadata validation failed:", file=sys.stderr)
        for error in exc.errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    rendered = render_index(catalog).encode("utf-8")
    index_path = _index_path(root)
    if args.check:
        if not index_path.is_file() or index_path.read_bytes() != rendered:
            print(
                f"stale generated index: {INDEX_REPO_PATH.as_posix()}; "
                "run python scripts/docs/build_architecture_index.py",
                file=sys.stderr,
            )
            return 1
        print(
            f"validated {INDEX_REPO_PATH.as_posix()} with "
            f"{len(catalog.components)} entries"
        )
        return 0

    index_path.write_bytes(rendered)
    print(
        f"wrote {INDEX_REPO_PATH.as_posix()} with "
        f"{len(catalog.components)} entries"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
