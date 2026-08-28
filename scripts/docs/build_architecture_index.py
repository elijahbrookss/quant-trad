#!/usr/bin/env python3
"""Validate architecture metadata and build its deterministic component index."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Sequence

ROOT = Path(__file__).resolve().parents[2]
ARCHITECTURE_DIR = PurePosixPath("docs/architecture")
INDEX_REPO_PATH = ARCHITECTURE_DIR / "ARCHITECTURE_COMPONENT_INDEX.md"

EXCLUDED_ARCHITECTURE_DOCS = frozenset(
    {
        "README.md",
        "decisions/README.md",
        "ARCHITECTURE_COMPONENT_INDEX.md",
    }
)
FRONTMATTER_KEYS = frozenset(
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
DOC_TYPES = frozenset({"adr", "architecture", "validation"})
COMPONENT_STATUSES = frozenset(
    {"accepted", "active", "draft", "historical", "superseded"}
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


@dataclass(frozen=True)
class ArchitectureCatalog:
    components: tuple[ComponentEntry, ...]


def _filesystem_path(root: Path, repo_path: str | PurePosixPath) -> Path:
    return root.joinpath(*PurePosixPath(repo_path).parts)


def _repo_path(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _parse_frontmatter(path: Path, repo_path: str) -> dict[str, MetadataValue]:
    """Parse QT's constrained scalar/block-list frontmatter without coercion."""

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
    metadata: dict[str, MetadataValue], repo_path: str, errors: list[str]
) -> None:
    actual = set(metadata)
    missing = sorted(FRONTMATTER_KEYS - actual)
    unknown = sorted(actual - FRONTMATTER_KEYS)
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
) -> list[str]:
    value = metadata.get(key)
    if not isinstance(value, list):
        errors.append(f"{repo_path}: {key} must be a block list")
        return []
    if not value:
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
) -> None:
    if not value:
        return
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
        return

    root_resolved = root.resolve()
    resolved = _filesystem_path(root, pure).resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError:
        errors.append(f"{repo_path}: {field} escapes the repository: {value}")
        return
    if not resolved.exists():
        errors.append(f"{repo_path}: {field} does not exist: {value}")


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

    _validate_key_set(metadata, repo_path, errors)
    component = _scalar(metadata, "component", repo_path, errors)
    subsystem = _scalar(metadata, "subsystem", repo_path, errors)
    layer = _scalar(metadata, "layer", repo_path, errors)
    doc_type = _scalar(metadata, "doc_type", repo_path, errors)
    status = _scalar(metadata, "status", repo_path, errors)
    tags = _string_list(metadata, "tags", repo_path, errors)
    code_paths = _string_list(metadata, "code_paths", repo_path, errors)

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

    return ComponentEntry(
        repo_path=repo_path,
        component=component,
        subsystem=subsystem,
        layer=layer,
        doc_type=doc_type,
        status=status,
        tags=tuple(tags),
        code_paths=tuple(code_paths),
    )


def build_catalog(root: Path = ROOT) -> ArchitectureCatalog:
    """Validate the complete component set and return a stable catalog."""

    root = root.resolve()
    errors: list[str] = []
    components: list[ComponentEntry] = []
    for path in _component_paths(root):
        entry = _validate_component(path, root, errors)
        if entry is not None:
            components.append(entry)

    components_by_slug: dict[str, list[str]] = {}
    for entry in components:
        components_by_slug.setdefault(entry.component, []).append(entry.repo_path)
    for component, paths in sorted(components_by_slug.items()):
        if component and len(paths) > 1:
            errors.append(
                f"duplicate component slug {component}: "
                f"{','.join(sorted(paths))}"
            )

    if errors:
        raise ArchitectureMetadataError(errors)
    return ArchitectureCatalog(
        components=tuple(
            sorted(
                components,
                key=lambda item: (item.subsystem, item.component),
            )
        )
    )


def _architecture_relative(repo_path: str) -> str:
    prefix = f"{ARCHITECTURE_DIR.as_posix()}/"
    if not repo_path.startswith(prefix):
        raise ValueError(f"not an architecture path: {repo_path}")
    return repo_path[len(prefix) :]


def render_index(catalog: ArchitectureCatalog) -> str:
    """Render the complete component index with stable ordering and LF endings."""

    lines: list[str] = [
        "# Architecture Component Index",
        "",
        "Generated by `python scripts/docs/build_architecture_index.py`.",
        "",
        "## Components",
        "",
        "| Component | Subsystem | Layer | Status | Doc |",
        "|---|---|---|---|---|",
    ]
    for entry in catalog.components:
        relative = _architecture_relative(entry.repo_path)
        lines.append(
            f"| {entry.component} | {entry.subsystem} | {entry.layer} | "
            f"{entry.status} | "
            f"[{PurePosixPath(relative).name}]({relative}) |"
        )

    lines.extend(
        [
            "",
            "## Code Path Coverage",
            "",
            "| Code Path | Component | Doc |",
            "|---|---|---|",
        ]
    )
    for entry in sorted(catalog.components, key=lambda item: item.component):
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
    for entry in catalog.components:
        for tag in entry.tags:
            tag_map.setdefault(tag, set()).add(entry.component)
    for tag in sorted(tag_map):
        lines.append(f"| `{tag}` | {', '.join(sorted(tag_map[tag]))} |")

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
