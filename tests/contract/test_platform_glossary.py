from __future__ import annotations

import copy
import hashlib
import runpy
from pathlib import Path, PurePosixPath

import pytest

from scripts.docs import build_architecture_index as architecture_index
from scripts.docs import glossary


ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def bundle() -> glossary.GlossaryBundle:
    return glossary.validate_repository(ROOT)


def _row(data: dict, collection: str, row_id: str) -> dict:
    return next(row for row in data[collection] if row["id"] == row_id)


def _validate(bundle: glossary.GlossaryBundle, data: dict) -> dict:
    return glossary.validate_disposition_data(
        data,
        root=bundle.root,
        proposal_terms=bundle.proposal_terms,
        inventory=bundle.inventory,
        architecture_catalog=bundle.architecture_catalog,
    )


def _write(root: Path, repo_path: str, content: str) -> Path:
    path = root.joinpath(*PurePosixPath(repo_path).parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_repository_preserves_frozen_denominators_and_exact_row_schemas(
    bundle: glossary.GlossaryBundle,
) -> None:
    proposal = bundle.root / bundle.data["proposal"]["path"]
    assert hashlib.sha256(proposal.read_bytes()).hexdigest() == glossary.PROPOSAL_SHA256
    assert tuple(term["id"] for term in bundle.data["terms"]) == glossary.TERM_IDS
    assert tuple(alias["id"] for alias in bundle.data["aliases"]) == glossary.ALIAS_IDS
    assert {term["id"] for term in bundle.data["terms"] if term["disposition"] == "deferred"} == glossary.DEFERRED_TERM_IDS
    assert sum(term["disposition"] == "adopted" for term in bundle.data["terms"]) == 53
    assert sum(alias["disposition"] == "ratified" for alias in bundle.data["aliases"]) == 20
    assert all(set(term) == glossary.TERM_KEYS for term in bundle.data["terms"])
    assert all(set(alias) == glossary.ALIAS_KEYS for alias in bundle.data["aliases"])


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ('{"value": 1, "value": 2}\n', "strict_json_duplicate_key:value"),
        ('{"value": NaN}\n', "strict_json_non_finite_number:NaN"),
    ],
)
def test_strict_json_rejects_ambiguous_values(
    tmp_path: Path, content: str, message: str
) -> None:
    path = _write(tmp_path, "ledger.json", content)
    with pytest.raises(glossary.GlossaryValidationError, match=message):
        glossary.load_json_strict(path)


def test_frozen_proposal_is_bound_by_exact_lf_sha256(tmp_path: Path) -> None:
    frozen = glossary.PROPOSAL_PATH.read_bytes()
    path = tmp_path / "proposed-glossary.md"
    path.write_bytes(frozen.replace(b"Proposed", b"Changed", 1))

    with pytest.raises(glossary.GlossaryValidationError, match="proposal:sha256_mismatch"):
        glossary.parse_frozen_proposal(path)


def test_unknown_row_key_is_rejected(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    data["terms"][0]["unreviewed_extension"] = True

    with pytest.raises(glossary.GlossaryValidationError, match="unknown_keys"):
        _validate(bundle, data)


def test_alias_lineage_must_equal_the_frozen_inventory(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    data["aliases"][0]["source_finding"] += " altered"

    with pytest.raises(glossary.GlossaryValidationError, match="frozen_alias_lineage_mismatch"):
        _validate(bundle, data)


@pytest.mark.parametrize(
    ("collection", "row_id", "reviewer"),
    [
        ("terms", "QT-TERM-006", "research-orchestration"),
        ("aliases", "QT-ALIAS-004", "research-orchestration"),
        ("terms", "QT-TERM-027", "decision-layer"),
        ("aliases", "QT-ALIAS-010", "identity"),
        ("terms", "QT-TERM-042", "identity"),
        ("terms", "QT-TERM-001", "platform-contract"),
    ],
)
def test_approved_cross_boundary_reviewer_sets_cannot_regress(
    bundle: glossary.GlossaryBundle,
    collection: str,
    row_id: str,
    reviewer: str,
) -> None:
    data = copy.deepcopy(bundle.data)
    _row(data, collection, row_id)["required_reviewers"].remove(reviewer)

    message = (
        "must_include_platform-contract"
        if reviewer == "platform-contract"
        else f"missing_approved_reviewers:{reviewer}"
    )
    with pytest.raises(glossary.GlossaryValidationError, match=message):
        _validate(bundle, data)


@pytest.mark.parametrize("term_id", ["QT-TERM-031", "QT-TERM-042"])
def test_owner_corrected_adopted_labels_cannot_regress(
    bundle: glossary.GlossaryBundle,
    term_id: str,
) -> None:
    data = copy.deepcopy(bundle.data)
    _row(data, "terms", term_id)["adopted_label"] += " altered"

    with pytest.raises(glossary.GlossaryValidationError, match="expected_approved_label"):
        _validate(bundle, data)


@pytest.mark.parametrize("term_id", ["QT-TERM-007", "QT-TERM-053"])
def test_approved_authority_locators_cannot_be_dropped(
    bundle: glossary.GlossaryBundle,
    term_id: str,
) -> None:
    data = copy.deepcopy(bundle.data)
    term = _row(data, "terms", term_id)
    required = glossary.REQUIRED_TERM_AUTHORITY_LOCATORS[term_id]
    term["authority_refs"] = [
        ref
        for ref in term["authority_refs"]
        if glossary._authority_signature(ref) not in required
    ]

    with pytest.raises(glossary.GlossaryValidationError, match="missing_approved_locator"):
        _validate(bundle, data)


def test_term_lifecycle_must_equal_the_frozen_proposal(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    term = _row(data, "terms", "QT-TERM-001")
    term["term_lifecycle"] = (
        "historical" if term["term_lifecycle"] != "historical" else "current"
    )

    with pytest.raises(
        glossary.GlossaryValidationError,
        match="frozen_proposal_lifecycle_mismatch",
    ):
        _validate(bundle, data)


def test_only_035_and_055_may_remain_deferred(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    term = _row(data, "terms", "QT-TERM-034")
    deferred = _row(data, "terms", "QT-TERM-035")
    term.update(
        disposition="deferred",
        adopted_label=None,
        authority_refs=[],
        reviewed_evidence_refs=copy.deepcopy(deferred["reviewed_evidence_refs"]),
        definition=None,
        usage_boundary=None,
        conflict_dispositions=copy.deepcopy(deferred["conflict_dispositions"]),
        normative_anchor=None,
        defer_reason="No reviewed primary authority owns this fixture term.",
        revisit_condition="Review an eligible primary authority.",
    )

    with pytest.raises(glossary.GlossaryValidationError, match="deferred_set_mismatch"):
        _validate(bundle, data)


def test_open_blocking_conflict_prevents_adoption(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    term = next(
        row
        for row in data["terms"]
        if row["disposition"] == "adopted" and row["conflict_dispositions"]
    )
    term["conflict_dispositions"][0]["disposition"] = "open_blocking"

    with pytest.raises(
        glossary.GlossaryValidationError,
        match="adopted_forbidden_with_open_blocking_conflict",
    ):
        _validate(bundle, data)


def test_authority_heading_must_resolve_exactly_once(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    _row(data, "terms", "QT-TERM-001")["authority_refs"][0]["locator"][
        "value"
    ] = "A heading that is not present"

    with pytest.raises(glossary.GlossaryValidationError, match="heading_must_resolve_once"):
        _validate(bundle, data)


def test_supporting_evidence_cannot_activate_an_adopted_term(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    refs = _row(data, "terms", "QT-TERM-002")["authority_refs"]
    for ref in refs:
        ref["authority_kind"] = "supporting_architecture"
        ref["role"] = "supporting"

    with pytest.raises(
        glossary.GlossaryValidationError,
        match="requires_eligible_primary_authority",
    ):
        _validate(bundle, data)


def test_one_physical_authority_locator_cannot_be_repeated_with_another_role(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    refs = _row(data, "terms", "QT-TERM-002")["authority_refs"]
    repeated = copy.deepcopy(refs[0])
    repeated["role"] = "supporting"
    refs.append(repeated)

    with pytest.raises(glossary.GlossaryValidationError, match="duplicate_references"):
        _validate(bundle, data)


def test_alias_crosswalk_is_bidirectional(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    term = next(row for row in data["terms"] if row["alias_refs"])
    term["alias_refs"].pop()

    with pytest.raises(glossary.GlossaryValidationError, match="not_bidirectional"):
        _validate(bundle, data)


def test_alias_normalization_allows_one_reviewed_family_but_not_cross_row_collision(
    bundle: glossary.GlossaryBundle,
) -> None:
    family = _row(bundle.data, "aliases", "QT-ALIAS-013")
    assert glossary._semantic_label_key("market-profile") == glossary._semantic_label_key(
        "market_profile"
    )
    assert {"market-profile", "market_profile"}.issubset(family["labels"])

    data = copy.deepcopy(bundle.data)
    data["aliases"][1]["labels"] = [data["aliases"][0]["labels"][0]]
    with pytest.raises(
        glossary.GlossaryValidationError,
        match="cross_row_normalized_collision",
    ):
        _validate(bundle, data)


def test_automatic_alias_replacement_targets_a_term_directly(
    bundle: glossary.GlossaryBundle,
) -> None:
    data = copy.deepcopy(bundle.data)
    alias = _row(data, "aliases", "QT-ALIAS-013")
    alias["automatic_replacement"] = True
    alias["replacement_term_id"] = "QT-ALIAS-001"

    with pytest.raises(glossary.GlossaryValidationError, match="must_target_term_directly"):
        _validate(bundle, data)

    data = copy.deepcopy(bundle.data)
    alias = _row(data, "aliases", "QT-ALIAS-013")
    alias["automatic_replacement"] = True
    alias["replacement_term_id"] = "QT-TERM-038"
    alias["term_refs"].append("QT-TERM-039")
    with pytest.raises(
        glossary.GlossaryValidationError,
        match="automatic_replacement_requires_one_target",
    ):
        _validate(bundle, data)


def _module_catalog(
    module_path: str,
    *,
    module_status: str = "active",
    module_owner: str = "data",
    component_owner: str = "data",
    component_reviewers: tuple[str, ...] = ("data", "platform-contract"),
) -> architecture_index.ArchitectureCatalog:
    component = architecture_index.ComponentEntry(
        repo_path="docs/architecture/data/README.md",
        component="data",
        subsystem="data",
        layer="boundary",
        doc_type="architecture",
        status="active",
        tags=("data",),
        code_paths=("src/data",),
        metadata_version=2,
        semantic_owner=component_owner,
        required_reviewers=component_reviewers,
        module_contract_paths=(module_path,),
    )
    module = architecture_index.ModuleContractEntry(
        repo_path=module_path,
        owning_component="data",
        component_scope="data-ingress",
        semantic_owner=module_owner,
        status=module_status,
    )
    return architecture_index.ArchitectureCatalog((component,), (module,), ())


def test_source_module_contract_requires_drr02_discovery_owner_and_reviewers(
    tmp_path: Path,
) -> None:
    module_path = "src/data/CONTRACT.md"
    _write(tmp_path, module_path, "# Data Ingress Authority\n")
    ref = {
        "path": module_path,
        "locator": {"kind": "heading", "value": "Data Ingress Authority"},
        "authority_kind": "source_module_contract",
        "role": "primary",
    }
    valid = _module_catalog(module_path)
    assert glossary._validate_authority_ref(
        ref,
        root=tmp_path,
        where="fixture.ref",
        owner="data",
        reviewers=("data", "platform-contract"),
        catalog=valid,
        read_order_paths=(),
        evidence_only=False,
    ) == (module_path, "Data Ingress Authority", "source_module_contract", "primary")

    cases = [
        (_module_catalog(module_path, module_status="draft"), "not_active_or_discovered"),
        (_module_catalog(module_path, module_owner="identity"), "source_module_owner_mismatch"),
        (
            _module_catalog(
                module_path,
                component_reviewers=("data", "identity", "platform-contract"),
            ),
            "source_module_missing_required_reviewers:identity",
        ),
        (architecture_index.ArchitectureCatalog(valid.components, (), ()), "not_active_or_discovered"),
    ]
    for catalog, message in cases:
        with pytest.raises(glossary.GlossaryValidationError, match=message):
            glossary._validate_authority_ref(
                ref,
                root=tmp_path,
                where="fixture.ref",
                owner="data",
                reviewers=("data", "platform-contract"),
                catalog=catalog,
                read_order_paths=(),
                evidence_only=False,
            )


def test_generated_views_are_deterministic_current_lf_bytes(
    bundle: glossary.GlossaryBundle,
) -> None:
    rendered_once = glossary._rendered_views(bundle)
    rendered_twice = glossary._rendered_views(bundle)
    assert rendered_once == rendered_twice
    assert list(rendered_once) == bundle.data["generated_views"]
    for repo_path, expected in rendered_once.items():
        actual = bundle.root.joinpath(*PurePosixPath(repo_path).parts).read_bytes()
        assert actual == expected
        assert actual.endswith(b"\n")
        assert b"\r" not in actual
    glossary.check_generated(bundle)
    disposition_view = glossary.render_disposition_view(bundle)
    assert "binds the approved\ndeferred set to `QT-TERM-035` and `QT-TERM-055`" in disposition_view


def test_deferred_terms_are_accounted_for_but_not_normatively_defined(
    bundle: glossary.GlossaryBundle,
) -> None:
    text = glossary.render_platform_glossary(bundle)
    for term_id in glossary.DEFERRED_TERM_IDS:
        assert f'<a id="qt-term-{term_id[-3:]}"></a>' not in text
        assert f"### `{term_id}` — " not in text
        assert text.count(f"`{term_id}`") == 1
    assert all(phrase not in text for phrase in glossary.FORBIDDEN_ADOPTED_OUTPUT_PHRASES)
    glossary._validate_normative_output(bundle, text.encode("utf-8"))


def test_required_navigation_build_targets_and_docs_profile_are_wired(
    bundle: glossary.GlossaryBundle,
) -> None:
    glossary.validate_navigation(bundle.root)
    makefile = (bundle.root / "Makefile").read_text(encoding="utf-8")
    assert "glossary-render: venv" in makefile
    assert "validate-glossary: venv" in makefile
    assert "scripts/docs/glossary.py check" in makefile
    assert "tests/contract/test_platform_glossary.py" in makefile

    profile_markers = runpy.run_path(str(bundle.root / "tests" / "conftest.py"))[
        "_ci_profile_markers_for_path"
    ]

    assert profile_markers(
        "tests/contract/test_platform_glossary.py"
    ) == {"docs"}
    assert profile_markers(
        "tests/contract/test_architecture_metadata_schema.py"
    ) == {"docs"}
