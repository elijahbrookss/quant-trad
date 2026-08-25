from __future__ import annotations

from pathlib import Path, PurePosixPath

import pytest

from scripts.docs import build_architecture_index as architecture_index


def _write(root: Path, repo_path: str, content: str = "") -> Path:
    path = root.joinpath(*PurePosixPath(repo_path).parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_allowlist(root: Path, paths: tuple[str, ...] = ()) -> None:
    content = "# test transition debt\n"
    if paths:
        content += "\n".join(paths) + "\n"
    _write(
        root,
        "docs/architecture/ARCHITECTURE_METADATA_V1_ALLOWLIST.txt",
        content,
    )


def _legacy_doc(
    *,
    component: str = "example-component",
    code_path: str = "src/example",
) -> str:
    return f"""\
---
component: {component}
subsystem: example
layer: boundary
doc_type: architecture
status: active
tags:
  - example
code_paths:
  - {code_path}
---
# Example
"""


def _v2_doc(
    *,
    component: str = "example-component",
    owner: str = "example",
    reviewers: tuple[str, ...] = ("example-owner",),
    code_paths: tuple[str, ...] = ("src/example",),
    module_contracts: tuple[str, ...] = (),
) -> str:
    reviewer_lines = "\n".join(
        f"  - {reviewer}" for reviewer in reviewers
    )
    code_path_lines = "\n".join(
        f"  - {code_path}" for code_path in code_paths
    )
    if module_contracts:
        module_lines = "module_contracts:\n" + "\n".join(
            f"  - {path}" for path in module_contracts
        )
    else:
        module_lines = "module_contracts: []"
    return f"""\
---
metadata_version: 2
component: {component}
subsystem: example
layer: boundary
doc_type: architecture
status: active
semantic_owner: {owner}
required_reviewers:
{reviewer_lines}
tags:
  - example
code_paths:
{code_path_lines}
{module_lines}
---
# Example
"""


def _module_contract(
    *,
    component: str = "example-component",
    owner: str = "example",
    scope: str = "example-scope",
    status: str = "active",
) -> str:
    return f"""\
---
module_contract_version: 1
contract_kind: source-module
owning_component: {component}
component_scope: {scope}
semantic_owner: {owner}
status: {status}
---
# Example Contract
"""


def _valid_v2_root(tmp_path: Path) -> Path:
    root = tmp_path
    (root / "src/example").mkdir(parents=True)
    _write_allowlist(root)
    _write(
        root,
        "docs/architecture/example/EXAMPLE.md",
        _v2_doc(),
    )
    return root


def test_legacy_allowlist_admits_only_the_exact_existing_path(
    tmp_path: Path,
) -> None:
    root = tmp_path
    (root / "src/example").mkdir(parents=True)
    repo_path = "docs/architecture/example/EXAMPLE.md"
    _write(root, repo_path, _legacy_doc())
    _write_allowlist(root, (repo_path,))

    catalog = architecture_index.build_catalog(root)
    assert catalog.legacy_paths == (repo_path,)

    _write_allowlist(root)
    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match="legacy metadata path is not allowlisted",
    ):
        architecture_index.build_catalog(root)


def test_v2_migration_is_atomic_and_leaves_the_legacy_allowlist(
    tmp_path: Path,
) -> None:
    root = _valid_v2_root(tmp_path)
    repo_path = "docs/architecture/example/EXAMPLE.md"
    _write_allowlist(root, (repo_path,))

    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match="version 2 document remains in the legacy allowlist",
    ):
        architecture_index.build_catalog(root)

    _write_allowlist(root)
    catalog = architecture_index.build_catalog(root)
    assert catalog.components[0].metadata_version == 2
    assert catalog.legacy_paths == ()

    partial = _v2_doc().replace(
        "required_reviewers:\n  - example-owner\n", ""
    )
    _write(root, repo_path, partial)
    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match="missing frontmatter keys: required_reviewers",
    ):
        architecture_index.build_catalog(root)


@pytest.mark.parametrize(
    ("reviewers", "message"),
    [
        (
            ("testing-owner", "example-owner"),
            "required_reviewers must be sorted",
        ),
        (
            ("example-owner", "example-owner"),
            "required_reviewers contains duplicates",
        ),
    ],
)
def test_v2_reviewer_routes_are_sorted_and_unique(
    tmp_path: Path,
    reviewers: tuple[str, ...],
    message: str,
) -> None:
    root = _valid_v2_root(tmp_path)
    _write(
        root,
        "docs/architecture/example/EXAMPLE.md",
        _v2_doc(reviewers=reviewers),
    )

    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match=message,
    ):
        architecture_index.build_catalog(root)


@pytest.mark.parametrize(
    ("transform", "message"),
    [
        (
            lambda text: text.replace(
                "component: example-component\n",
                "component: example-component\ncomponent: duplicate\n",
            ),
            "duplicate frontmatter key: component",
        ),
        (
            lambda text: text.replace(
                "status: active\n",
                "status: active\nunknown_key: value\n",
                1,
            ),
            "unknown frontmatter keys: unknown_key",
        ),
        (
            lambda text: text.replace(
                "metadata_version: 2\n", "metadata_version: 1\n"
            ),
            "metadata_version must be the scalar 2",
        ),
    ],
)
def test_frontmatter_rejects_duplicate_unknown_or_unsupported_fields(
    tmp_path: Path,
    transform,
    message: str,
) -> None:
    root = _valid_v2_root(tmp_path)
    _write(
        root,
        "docs/architecture/example/EXAMPLE.md",
        transform(_v2_doc()),
    )

    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match=message,
    ):
        architecture_index.build_catalog(root)


@pytest.mark.parametrize(
    ("code_path", "message"),
    [
        ("src/missing", "code_paths item does not exist"),
        ("../outside", "normalized repository-relative path"),
        (r"src\example", "normalized repository-relative path"),
    ],
)
def test_code_paths_are_existing_normalized_repository_paths(
    tmp_path: Path,
    code_path: str,
    message: str,
) -> None:
    root = _valid_v2_root(tmp_path)
    _write(
        root,
        "docs/architecture/example/EXAMPLE.md",
        _v2_doc(code_paths=(code_path,)),
    )

    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match=message,
    ):
        architecture_index.build_catalog(root)


def test_shared_code_paths_remain_valid_coverage(
    tmp_path: Path,
) -> None:
    root = tmp_path
    (root / "src/shared").mkdir(parents=True)
    _write_allowlist(root)
    _write(
        root,
        "docs/architecture/example/ZETA.md",
        _v2_doc(
            component="zeta-component",
            owner="zeta",
            reviewers=("zeta-owner",),
            code_paths=("src/shared",),
        ),
    )
    _write(
        root,
        "docs/architecture/example/ALPHA.md",
        _v2_doc(
            component="alpha-component",
            owner="alpha",
            reviewers=("alpha-owner",),
            code_paths=("src/shared",),
        ),
    )

    catalog = architecture_index.build_catalog(root)
    assert [entry.component for entry in catalog.components] == [
        "alpha-component",
        "zeta-component",
    ]
    assert architecture_index.render_index(catalog).count(
        "| `src/shared` |"
    ) == 2


def test_linked_module_contract_requires_matching_scope_and_owner(
    tmp_path: Path,
) -> None:
    root = tmp_path
    contract_path = "src/example/docs/timing_contract.md"
    (root / "src/example/docs").mkdir(parents=True)
    _write_allowlist(root)
    _write(root, contract_path, _module_contract(status="historical"))
    _write(
        root,
        "docs/architecture/example/EXAMPLE.md",
        _v2_doc(module_contracts=(contract_path,)),
    )

    catalog = architecture_index.build_catalog(root)
    assert len(catalog.module_contracts) == 1
    rendered = architecture_index.render_index(catalog)
    assert "timing_contract.md" in rendered
    assert "lineage only" in rendered

    _write(root, contract_path, _module_contract(owner="other-owner"))
    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match="semantic_owner must match example",
    ):
        architecture_index.build_catalog(root)


def test_linked_module_contract_must_be_a_markdown_file(
    tmp_path: Path,
) -> None:
    root = tmp_path
    contract_path = "src/example/docs/timing_contract.md"
    (root / contract_path).mkdir(parents=True)
    _write_allowlist(root)
    _write(
        root,
        "docs/architecture/example/EXAMPLE.md",
        _v2_doc(module_contracts=(contract_path,)),
    )

    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match="must reference a Markdown file",
    ):
        architecture_index.build_catalog(root)


def test_unlinked_or_multiply_linked_module_contract_is_not_authority(
    tmp_path: Path,
) -> None:
    root = tmp_path
    contract_path = "src/example/docs/timing_contract.md"
    (root / "src/example/docs").mkdir(parents=True)
    _write_allowlist(root)
    _write(root, contract_path, _module_contract())
    _write(
        root,
        "docs/architecture/example/EXAMPLE.md",
        _v2_doc(),
    )

    catalog = architecture_index.build_catalog(root)
    assert catalog.module_contracts == ()

    _write(
        root,
        "docs/architecture/example/EXAMPLE.md",
        _v2_doc(module_contracts=(contract_path,)),
    )
    _write(
        root,
        "docs/architecture/example/SECOND.md",
        _v2_doc(
            component="second-component",
            owner="second",
            reviewers=("second-owner",),
            module_contracts=(contract_path,),
        ),
    )
    with pytest.raises(
        architecture_index.ArchitectureMetadataError,
        match="module contract must have one owning component",
    ):
        architecture_index.build_catalog(root)


def test_check_mode_is_deterministic_and_never_repairs_stale_output(
    tmp_path: Path,
) -> None:
    root = _valid_v2_root(tmp_path)
    index_path = (
        root
        / "docs"
        / "architecture"
        / "ARCHITECTURE_COMPONENT_INDEX.md"
    )

    assert architecture_index.main(["--check"], root=root) == 1
    assert not index_path.exists()

    assert architecture_index.main([], root=root) == 0
    expected = index_path.read_bytes()
    assert architecture_index.main(["--check"], root=root) == 0

    index_path.write_bytes(expected + b"stale\n")
    stale = index_path.read_bytes()
    assert architecture_index.main(["--check"], root=root) == 1
    assert index_path.read_bytes() == stale
