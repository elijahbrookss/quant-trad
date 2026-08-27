#!/usr/bin/env python3
"""Validate and render QT's reviewed terminology dispositions.

The disposition ledger is non-normative decision evidence.  Only the generated
platform glossary adopts vocabulary, and neither artifact activates a guarantee
or records proof results.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, NoReturn, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.docs import build_architecture_index as architecture_index


RECONCILIATION_DIR = ROOT / "docs" / "plans" / "documentation-reconciliation"
LEDGER_PATH = RECONCILIATION_DIR / "phase-3-terminology-dispositions.json"
PROPOSAL_PATH = RECONCILIATION_DIR / "proposed-glossary.md"
INVENTORY_PATH = RECONCILIATION_DIR / "terminology-inventory.md"
DISPOSITION_VIEW_PATH = RECONCILIATION_DIR / "phase-3-terminology-dispositions.md"
PLATFORM_GLOSSARY_PATH = ROOT / "docs" / "contracts" / "platform" / "04_glossary.md"

SCHEMA_VERSION = "qt.terminology_dispositions.v1"
BASELINE_COMMIT = "d46e40bf55caeea12f4ccbde640c71f271eaf9c4"
PROPOSAL_SHA256 = "709782a9866db2de178af273f3ea8476fd83fcbf3cb6f2ac70e21b35c8a0f2c9"

TERM_IDS = tuple(f"QT-TERM-{number:03d}" for number in range(1, 56))
ALIAS_IDS = tuple(f"QT-ALIAS-{number:03d}" for number in range(1, 21))
CONFLICT_IDS = tuple(f"QT-CONFLICT-{number:03d}" for number in range(1, 27))
DEFERRED_TERM_IDS = frozenset({"QT-TERM-035", "QT-TERM-055"})

TERM_ID_RE = re.compile(r"QT-TERM-\d{3}\Z")
ALIAS_ID_RE = re.compile(r"QT-ALIAS-\d{3}\Z")
CONFLICT_ID_RE = re.compile(r"QT-CONFLICT-\d{3}\Z")
SLUG_RE = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
HEX40_RE = re.compile(r"[0-9a-f]{40}\Z")
HEX64_RE = re.compile(r"[0-9a-f]{64}\Z")
RFC2119_RE = re.compile(r"\b(?:MUST(?: NOT)?|SHALL(?: NOT)?|REQUIRED)\b")

ROOT_KEYS = {
    "schema_version",
    "audit_baseline_commit",
    "proposal",
    "decision_authority",
    "generated_views",
    "terms",
    "aliases",
}
PROPOSAL_KEYS = {"path", "sha256", "term_count", "alias_inventory_path", "alias_count"}
DECISION_KEYS = {
    "phase",
    "policy_ids",
    "authorization_record",
    "normative_effect",
    "guarantee_activation_effect",
    "proof_or_attestation_effect",
}
TERM_KEYS = {
    "id",
    "source_label",
    "source_status",
    "entry_kind",
    "term_lifecycle",
    "disposition",
    "adopted_label",
    "owner",
    "required_reviewers",
    "consulted_boundaries",
    "authority_refs",
    "reviewed_evidence_refs",
    "definition",
    "usage_boundary",
    "conflict_dispositions",
    "alias_refs",
    "normative_anchor",
    "decision_rationale",
    "defer_reason",
    "revisit_condition",
}
ALIAS_KEYS = {
    "id",
    "source_finding",
    "source_classification",
    "disposition",
    "classification",
    "labels",
    "term_refs",
    "owner",
    "required_reviewers",
    "scope_and_handling",
    "automatic_replacement",
    "replacement_term_id",
    "authority_refs",
    "normative_anchor",
    "decision_rationale",
    "defer_reason",
    "revisit_condition",
}
REFERENCE_KEYS = {"path", "locator", "authority_kind", "role"}
LOCATOR_KEYS = {"kind", "value"}
CONFLICT_KEYS = {"id", "disposition"}

ENTRY_KINDS = {"domain_term", "contrast_set", "qualification_rule"}
TERM_LIFECYCLES = {"current", "historical", "unclear"}
TERM_DISPOSITIONS = {"adopted", "deferred"}
ALIAS_DISPOSITIONS = {"ratified", "deferred"}
ALIAS_CLASSIFICATIONS = {
    "code_spelling",
    "compatibility",
    "historical",
    "discouraged",
    "rejected",
}
CONFLICT_DISPOSITIONS = {
    "qualified_nonblocking",
    "resolved_by_authority",
    "open_blocking",
}
AUTHORITY_KINDS = {
    "accepted_adr",
    "normative_platform_contract",
    "source_module_contract",
    "supporting_architecture",
}
AUTHORITY_ROLES = {"primary", "supporting"}
PRIMARY_AUTHORITY_KINDS = {
    "accepted_adr",
    "normative_platform_contract",
    "source_module_contract",
}

REQUIRED_REVIEWERS_BY_ROW = {
    "QT-TERM-006": frozenset(
        {"platform-contract", "research-memory", "research-orchestration"}
    ),
    "QT-TERM-012": frozenset(
        {"platform-contract", "research-memory", "research-orchestration"}
    ),
    "QT-TERM-027": frozenset(
        {"data", "decision-layer", "execution-runtime", "identity", "platform-contract"}
    ),
    "QT-TERM-028": frozenset(
        {"data", "decision-layer", "execution-runtime", "identity", "platform-contract"}
    ),
    "QT-TERM-029": frozenset(
        {"data", "decision-layer", "execution-runtime", "identity", "platform-contract"}
    ),
    "QT-TERM-042": frozenset(
        {"execution-runtime", "identity", "platform-contract"}
    ),
    "QT-TERM-043": frozenset(
        {"execution-runtime", "identity", "platform-contract"}
    ),
    "QT-ALIAS-004": frozenset(
        {"platform-contract", "research-memory", "research-orchestration"}
    ),
    "QT-ALIAS-009": frozenset(
        {"data", "decision-layer", "execution-runtime", "identity", "platform-contract"}
    ),
    "QT-ALIAS-010": frozenset(
        {"data", "decision-layer", "execution-runtime", "identity", "platform-contract"}
    ),
    "QT-ALIAS-011": frozenset(
        {"data", "decision-layer", "execution-runtime", "identity", "platform-contract"}
    ),
}

REQUIRED_TERM_AUTHORITY_LOCATORS = {
    "QT-TERM-007": frozenset(
        {
            (
                "docs/architecture/decisions/0063-use-schema-registered-canonical-facts.md",
                "Dataset And Research Semantics",
                "accepted_adr",
                "primary",
            )
        }
    ),
    "QT-TERM-053": frozenset(
        {
            (
                "docs/architecture/decisions/0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md",
                "Decision",
                "accepted_adr",
                "primary",
            )
        }
    ),
}

REQUIRED_ADOPTED_LABELS = {
    "QT-TERM-031": "Collector definition admission / collector operation",
    "QT-TERM-042": "SeriesExecutionProfile (compatibility compiler)",
}

FORBIDDEN_ADOPTED_OUTPUT_PHRASES = frozenset(
    {
        "Provider ID / Venue ID / exchange slug",
        "A specifically named runtime/lifecycle fact",
        "Prefer over unqualified Dataset for source data",
        "Source, receipt, gap/quality, runtime-ledger, Check, or scientific evidence",
        "Acquisition coverage, stream coverage interval, archive coverage, Dataset coverage, and reporting coverage are separately owned evidence",
        "0059-use-first-class-scientific-research-objects.md",
        "0046-use-replay-bundles-for-execution-reproduction.md",
    }
)


class GlossaryValidationError(ValueError):
    """Raised when terminology metadata or its generated views are invalid."""


@dataclass(frozen=True)
class ProposedTerm:
    id: str
    label: str
    source_status: str
    entry_kind: str
    term_lifecycle: str


@dataclass(frozen=True)
class AliasInventoryRow:
    id: str
    source_finding: str
    source_classification: str


@dataclass(frozen=True)
class TerminologyInventory:
    term_ids: tuple[str, ...]
    aliases: Mapping[str, AliasInventoryRow]
    conflict_ids: frozenset[str]


@dataclass(frozen=True)
class GlossaryBundle:
    data: dict[str, Any]
    proposal_terms: Mapping[str, ProposedTerm]
    inventory: TerminologyInventory
    architecture_catalog: Any
    root: Path


def _fail(message: str) -> NoReturn:
    raise GlossaryValidationError(message)


def _reject_constant(value: str) -> None:
    _fail(f"strict_json_non_finite_number:{value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail(f"strict_json_duplicate_key:{key}")
        result[key] = value
    return result


def load_json_strict(path: Path) -> dict[str, Any]:
    """Load one JSON object while rejecting duplicate keys and non-finite values."""

    try:
        raw_bytes = path.read_bytes()
        if raw_bytes.startswith(b"\xef\xbb\xbf"):
            _fail(f"json_utf8_bom_forbidden:{path}")
        if b"\r" in raw_bytes:
            _fail(f"json_requires_lf_endings:{path}")
        value = json.loads(
            raw_bytes.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except GlossaryValidationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _fail(f"invalid_json:{path}:{exc}")
    if not isinstance(value, dict):
        _fail(f"json_root_must_be_object:{path}")
    return value


def _expect_object(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(f"{where}:expected_object")
    return value


def _expect_list(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(f"{where}:expected_array")
    return value


def _expect_string(value: Any, where: str, *, nonempty: bool = True) -> str:
    if not isinstance(value, str):
        _fail(f"{where}:expected_string")
    if nonempty and not value.strip():
        _fail(f"{where}:empty_string")
    return value


def _expect_int(value: Any, where: str) -> int:
    if type(value) is not int:
        _fail(f"{where}:expected_integer")
    return value


def _expect_bool(value: Any, where: str) -> bool:
    if type(value) is not bool:
        _fail(f"{where}:expected_boolean")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], where: str) -> None:
    missing = sorted(expected - set(value))
    extra = sorted(set(value) - expected)
    if missing:
        _fail(f"{where}:missing_keys:{','.join(missing)}")
    if extra:
        _fail(f"{where}:unknown_keys:{','.join(extra)}")


def _enum(value: Any, allowed: set[str], where: str) -> str:
    text = _expect_string(value, where)
    if text not in allowed:
        _fail(f"{where}:invalid_value:{text}")
    return text


def _string_list(
    value: Any,
    where: str,
    *,
    nonempty: bool = False,
    sorted_values: bool = True,
) -> list[str]:
    values = _expect_list(value, where)
    result = [_expect_string(item, f"{where}[{index}]") for index, item in enumerate(values)]
    if nonempty and not result:
        _fail(f"{where}:must_not_be_empty")
    if len(result) != len(set(result)):
        _fail(f"{where}:duplicate_values")
    if sorted_values and result != sorted(result):
        _fail(f"{where}:must_be_sorted")
    return result


def _nullable_string(value: Any, where: str) -> str | None:
    if value is None:
        return None
    return _expect_string(value, where)


def _single_line(value: Any, where: str, *, normative_scan: bool = False) -> str:
    text = _expect_string(value, where)
    if text != text.strip() or "\n" in text or "\r" in text:
        _fail(f"{where}:must_be_trimmed_single_line")
    if text.count("`") % 2:
        _fail(f"{where}:unbalanced_inline_code")
    if normative_scan and RFC2119_RE.search(text):
        _fail(f"{where}:uncited_normative_keyword")
    return text


def _repo_path(root: Path, raw: Any, where: str) -> tuple[str, Path]:
    value = _expect_string(raw, where)
    pure = PurePosixPath(value)
    if (
        "\\" in value
        or pure.is_absolute()
        or not pure.parts
        or any(part in {"", ".", ".."} for part in pure.parts)
        or pure.as_posix() != value
    ):
        _fail(f"{where}:path_must_be_normalized_repo_relative:{value}")
    candidate = root.joinpath(*pure.parts)
    try:
        candidate.resolve(strict=False).relative_to(root.resolve())
    except ValueError:
        _fail(f"{where}:path_escapes_repository:{value}")
    if not candidate.is_file():
        _fail(f"{where}:path_missing:{value}")
    current = root
    for part in pure.parts:
        try:
            names = {child.name for child in current.iterdir()}
        except OSError as exc:
            _fail(f"{where}:path_unreadable:{value}:{exc}")
        if part not in names:
            _fail(f"{where}:path_case_mismatch:{value}")
        current /= part
    return value, candidate


def _markdown_headings(path: Path) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        _fail(f"authority_unreadable:{path}:{exc}")
    headings: list[str] = []
    fence: str | None = None
    for line in lines:
        fence_match = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
        if fence_match:
            marker = fence_match.group(1)
            if fence is None:
                fence = marker[0]
            elif marker[0] == fence:
                fence = None
            continue
        if fence is not None:
            continue
        match = re.match(r"^#{1,6}[ \t]+(.+?)[ \t]*$", line)
        if match:
            headings.append(match.group(1).strip().rstrip("#").rstrip())
    return headings


def _frontmatter_scalar(path: Path, key: str, where: str) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "---":
        _fail(f"{where}:frontmatter_required")
    try:
        end = lines.index("---", 1)
    except ValueError:
        _fail(f"{where}:frontmatter_unclosed")
    values = []
    for line in lines[1:end]:
        match = re.fullmatch(rf"{re.escape(key)}:\s*([^\s#]+)\s*", line)
        if match:
            values.append(match.group(1))
    if len(values) != 1:
        _fail(f"{where}:frontmatter_requires_exactly_one_{key}")
    return values[0]


def _split_table_row(line: str, where: str) -> list[str]:
    if not line.startswith("|") or not line.endswith("|"):
        _fail(f"{where}:malformed_markdown_table_row")
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for character in line[1:-1]:
        if escaped:
            current.append(character)
            escaped = False
        elif character == "\\":
            current.append(character)
            escaped = True
        elif character == "|":
            cells.append("".join(current).strip())
            current = []
        else:
            current.append(character)
    cells.append("".join(current).strip())
    return cells


def _table_section(text: str, heading: str, columns: int) -> list[list[str]]:
    lines = text.splitlines()
    try:
        start = lines.index(heading) + 1
    except ValueError:
        _fail(f"inventory:missing_section:{heading[3:]}")
    end = next(
        (index for index in range(start, len(lines)) if lines[index].startswith("## ")),
        len(lines),
    )
    table_lines = [line for line in lines[start:end] if line.startswith("|")]
    if len(table_lines) < 3:
        _fail(f"inventory:empty_table:{heading[3:]}")
    rows: list[list[str]] = []
    for offset, line in enumerate(table_lines[2:], start=3):
        cells = _split_table_row(line, f"inventory:{heading[3:]}:row={offset}")
        if len(cells) != columns:
            _fail(
                f"inventory:{heading[3:]}:row={offset}:"
                f"expected_{columns}_cells:actual={len(cells)}"
            )
        rows.append(cells)
    return rows


def parse_frozen_proposal(path: Path = PROPOSAL_PATH) -> dict[str, ProposedTerm]:
    """Verify and parse the byte-frozen Phase 2 glossary proposal."""

    try:
        raw = path.read_bytes()
    except OSError as exc:
        _fail(f"proposal_unreadable:{path}:{exc}")
    if raw.startswith(b"\xef\xbb\xbf"):
        _fail("proposal:utf8_bom_forbidden")
    if b"\r" in raw or not raw.endswith(b"\n"):
        _fail("proposal:requires_lf_with_final_newline")
    observed_hash = hashlib.sha256(raw).hexdigest()
    if observed_hash != PROPOSAL_SHA256:
        _fail(f"proposal:sha256_mismatch:{observed_hash}")
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError as exc:
        _fail(f"proposal:invalid_utf8:{exc}")

    heading_re = re.compile(r"^### `(QT-TERM-\d{3})` — (.+)$")
    sections: dict[str, tuple[str, list[str]]] = {}
    current_id: str | None = None
    for line_number, line in enumerate(lines, start=1):
        if line.startswith("### ") and "QT-TERM-" in line:
            match = heading_re.fullmatch(line)
            if not match:
                _fail(f"proposal:noncanonical_term_heading:line={line_number}")
            current_id = match.group(1)
            if current_id in sections:
                _fail(f"proposal:duplicate_term:{current_id}")
            sections[current_id] = (match.group(2), [])
            continue
        if current_id is not None:
            sections[current_id][1].append(line)

    result: dict[str, ProposedTerm] = {}
    for term_id, (label, body) in sections.items():
        statuses = [
            match.group(1)
            for line in body
            if (match := re.fullmatch(r"- Proposal status: `?([a-z_]+)`?", line))
        ]
        kinds = [
            match.group(1)
            for line in body
            if (match := re.fullmatch(r"- Entry kind: `?([a-z_]+)`?", line))
        ]
        lifecycles = [
            match.group(1)
            for line in body
            if (match := re.fullmatch(r"- Term lifecycle: `?([a-z_]+)`?", line))
        ]
        if len(statuses) != 1:
            _fail(f"proposal:{term_id}:requires_one_status")
        if len(kinds) != 1:
            _fail(f"proposal:{term_id}:requires_one_entry_kind")
        if len(lifecycles) != 1:
            _fail(f"proposal:{term_id}:requires_one_term_lifecycle")
        if kinds[0] not in ENTRY_KINDS:
            _fail(f"proposal:{term_id}:invalid_entry_kind:{kinds[0]}")
        if lifecycles[0] not in TERM_LIFECYCLES:
            _fail(f"proposal:{term_id}:invalid_term_lifecycle:{lifecycles[0]}")
        result[term_id] = ProposedTerm(
            term_id, label, statuses[0], kinds[0], lifecycles[0]
        )

    if set(result) != set(TERM_IDS):
        missing = sorted(set(TERM_IDS) - set(result))
        extra = sorted(set(result) - set(TERM_IDS))
        _fail(f"proposal:term_set_mismatch:missing={','.join(missing)}:extra={','.join(extra)}")
    return result


def parse_terminology_inventory(path: Path = INVENTORY_PATH) -> TerminologyInventory:
    """Parse exact Phase 1 term, alias, and conflict denominators."""

    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        _fail(f"inventory_unreadable:{path}:{exc}")
    term_rows = _table_section(text, "## Term Candidates", 6)
    alias_rows = _table_section(text, "## Deprecated, Rejected, and Historical Aliases", 5)
    conflict_rows = _table_section(text, "## Semantic Conflicts and Collisions", 3)

    term_ids: list[str] = []
    for index, cells in enumerate(term_rows):
        match = re.fullmatch(r"`(QT-TERM-\d{3})`", cells[0])
        if not match:
            _fail(f"inventory.terms[{index}]:invalid_id_cell:{cells[0]}")
        term_ids.append(match.group(1))
    if tuple(term_ids) != TERM_IDS:
        _fail("inventory:term_ids_must_be_exact_001_through_055")

    aliases: dict[str, AliasInventoryRow] = {}
    alias_order: list[str] = []
    for index, cells in enumerate(alias_rows):
        match = re.fullmatch(r"`(QT-ALIAS-\d{3})`", cells[0])
        if not match:
            _fail(f"inventory.aliases[{index}]:invalid_id_cell:{cells[0]}")
        alias_id = match.group(1)
        if alias_id in aliases:
            _fail(f"inventory:duplicate_alias:{alias_id}")
        alias_order.append(alias_id)
        aliases[alias_id] = AliasInventoryRow(alias_id, cells[1], cells[2])
    if tuple(alias_order) != ALIAS_IDS:
        _fail("inventory:alias_ids_must_be_exact_001_through_020")

    conflict_ids: list[str] = []
    for index, cells in enumerate(conflict_rows):
        match = re.fullmatch(r"`(QT-CONFLICT-\d{3})`", cells[0])
        if not match:
            _fail(f"inventory.conflicts[{index}]:invalid_id_cell:{cells[0]}")
        conflict_ids.append(match.group(1))
    if tuple(conflict_ids) != CONFLICT_IDS:
        _fail("inventory:conflict_ids_must_be_exact_001_through_026")
    return TerminologyInventory(tuple(term_ids), aliases, frozenset(conflict_ids))


def _semantic_label_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(re.sub(r"[\W_]+", " ", normalized, flags=re.UNICODE).split())


def _exact_label_key(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _read_order_paths(root: Path) -> tuple[str, ...]:
    path = root / "docs" / "contracts" / "README.md"
    text = path.read_text(encoding="utf-8")
    match = re.search(r"(?ms)^## Read Order\s*$\n(?P<body>.*?)(?=^## |\Z)", text)
    if not match:
        _fail("navigation:contracts_read_order_missing")
    rows = re.findall(r"(?m)^(\d+)\. `([^`]+)`\s*$", match.group("body"))
    numbers = [int(number) for number, _ in rows]
    if numbers != list(range(1, len(numbers) + 1)):
        _fail("navigation:contracts_read_order_not_contiguous")
    paths = tuple(value for _, value in rows)
    if paths[:5] != (
        "platform/00_system_contract.md",
        "platform/01_runtime_contract.md",
        "platform/02_execution_playback_contract.md",
        "platform/03_engineering_contract.md",
        "platform/04_glossary.md",
    ):
        _fail("navigation:platform_glossary_must_be_read_order_item_5")
    return tuple(f"docs/contracts/{value}" for value in paths)


def _validate_authority_ref(
    raw: Any,
    *,
    root: Path,
    where: str,
    owner: str,
    reviewers: Sequence[str],
    catalog: Any,
    read_order_paths: Sequence[str],
    evidence_only: bool,
) -> tuple[str, str, str, str]:
    ref = _expect_object(raw, where)
    _exact_keys(ref, REFERENCE_KEYS, where)
    path_text, path = _repo_path(root, ref["path"], f"{where}.path")
    locator = _expect_object(ref["locator"], f"{where}.locator")
    _exact_keys(locator, LOCATOR_KEYS, f"{where}.locator")
    if locator["kind"] != "heading":
        _fail(f"{where}.locator.kind:expected_heading")
    heading = _single_line(locator["value"], f"{where}.locator.value")
    count = _markdown_headings(path).count(heading)
    if count != 1:
        _fail(f"{where}.locator:heading_must_resolve_once:{heading}:{count}")
    kind = _enum(ref["authority_kind"], AUTHORITY_KINDS, f"{where}.authority_kind")
    role = _enum(ref["role"], AUTHORITY_ROLES, f"{where}.role")
    if evidence_only and role != "supporting":
        _fail(f"{where}:reviewed_evidence_must_be_supporting")
    if kind == "supporting_architecture" and role == "primary":
        _fail(f"{where}:supporting_architecture_cannot_be_primary")

    components = {entry.repo_path: entry for entry in catalog.components}
    module_contracts = {entry.repo_path: entry for entry in catalog.module_contracts}
    if kind == "accepted_adr":
        component = components.get(path_text)
        if (
            component is None
            or component.doc_type != "adr"
            or component.status != "accepted"
            or not path_text.startswith("docs/architecture/decisions/")
        ):
            _fail(f"{where}:accepted_adr_requires_accepted_catalog_entry:{path_text}")
        if _frontmatter_scalar(path, "status", where) != "accepted":
            _fail(f"{where}:accepted_adr_status_mismatch:{path_text}")
    elif kind == "normative_platform_contract":
        if (
            path_text not in read_order_paths
            or not path_text.startswith("docs/contracts/platform/")
            or path_text == "docs/contracts/platform/04_glossary.md"
        ):
            _fail(f"{where}:invalid_platform_contract_authority:{path_text}")
    elif kind == "source_module_contract":
        module = module_contracts.get(path_text)
        if module is None or module.status != "active":
            _fail(f"{where}:source_module_contract_not_active_or_discovered:{path_text}")
        owning_components = [
            entry for entry in catalog.components if path_text in entry.module_contract_paths
        ]
        if len(owning_components) != 1:
            _fail(f"{where}:source_module_contract_requires_one_owner:{path_text}")
        owning_component = owning_components[0]
        if module.semantic_owner != owner or owning_component.semantic_owner != owner:
            _fail(f"{where}:source_module_owner_mismatch:{path_text}:{owner}")
        missing_reviewers = sorted(set(owning_component.required_reviewers) - set(reviewers))
        if missing_reviewers:
            _fail(
                f"{where}:source_module_missing_required_reviewers:"
                f"{','.join(missing_reviewers)}"
            )
    elif kind == "supporting_architecture":
        if not (
            path_text.startswith("docs/architecture/")
            or path_text.startswith("docs/engineering/")
        ):
            _fail(f"{where}:supporting_architecture_outside_docs:{path_text}")

    return path_text, heading, kind, role


def _validate_refs(
    raw: Any,
    *,
    root: Path,
    where: str,
    owner: str,
    reviewers: Sequence[str],
    catalog: Any,
    read_order_paths: Sequence[str],
    evidence_only: bool,
    require_primary: bool,
) -> list[dict[str, Any]]:
    refs = _expect_list(raw, where)
    if require_primary and not refs:
        _fail(f"{where}:must_not_be_empty")
    signatures: list[tuple[str, str, str, str]] = []
    for index, ref in enumerate(refs):
        signatures.append(
            _validate_authority_ref(
                ref,
                root=root,
                where=f"{where}[{index}]",
                owner=owner,
                reviewers=reviewers,
                catalog=catalog,
                read_order_paths=read_order_paths,
                evidence_only=evidence_only,
            )
        )
    physical_locators = [(path, heading) for path, heading, _, _ in signatures]
    if len(physical_locators) != len(set(physical_locators)):
        _fail(f"{where}:duplicate_references")
    if require_primary and not any(
        role == "primary" and kind in PRIMARY_AUTHORITY_KINDS
        for _, _, kind, role in signatures
    ):
        _fail(f"{where}:requires_eligible_primary_authority")
    return [dict(item) for item in refs]


def _validate_role_fields(row: Mapping[str, Any], where: str) -> tuple[str, list[str]]:
    owner = _expect_string(row["owner"], f"{where}.owner")
    if not SLUG_RE.fullmatch(owner):
        _fail(f"{where}.owner:invalid_slug:{owner}")
    reviewers = _string_list(row["required_reviewers"], f"{where}.required_reviewers", nonempty=True)
    for reviewer in reviewers:
        if not SLUG_RE.fullmatch(reviewer):
            _fail(f"{where}.required_reviewers:invalid_slug:{reviewer}")
    if owner not in reviewers:
        _fail(f"{where}.required_reviewers:must_include_owner:{owner}")
    if "platform-contract" not in reviewers:
        _fail(f"{where}.required_reviewers:must_include_platform-contract")
    return owner, reviewers


def _validate_fixed_reviewers(row_id: str, reviewers: Sequence[str], where: str) -> None:
    required = REQUIRED_REVIEWERS_BY_ROW.get(row_id, frozenset())
    missing = sorted(required - set(reviewers))
    if missing:
        _fail(f"{where}.required_reviewers:missing_approved_reviewers:{','.join(missing)}")


def _authority_signature(ref: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        ref["path"],
        ref["locator"]["value"],
        ref["authority_kind"],
        ref["role"],
    )


def validate_disposition_data(
    data: Mapping[str, Any],
    *,
    root: Path,
    proposal_terms: Mapping[str, ProposedTerm],
    inventory: TerminologyInventory,
    architecture_catalog: Any,
) -> dict[str, Any]:
    """Validate the exact Phase 3 terminology ledger and its authority graph."""

    _exact_keys(data, ROOT_KEYS, "ledger")
    if data["schema_version"] != SCHEMA_VERSION:
        _fail(f"ledger.schema_version:expected:{SCHEMA_VERSION}")
    baseline = _expect_string(data["audit_baseline_commit"], "ledger.audit_baseline_commit")
    if not HEX40_RE.fullmatch(baseline) or baseline != BASELINE_COMMIT:
        _fail(f"ledger.audit_baseline_commit:expected:{BASELINE_COMMIT}")

    proposal = _expect_object(data["proposal"], "ledger.proposal")
    _exact_keys(proposal, PROPOSAL_KEYS, "ledger.proposal")
    expected_proposal = {
        "path": "docs/plans/documentation-reconciliation/proposed-glossary.md",
        "sha256": PROPOSAL_SHA256,
        "term_count": 55,
        "alias_inventory_path": "docs/plans/documentation-reconciliation/terminology-inventory.md",
        "alias_count": 20,
    }
    if proposal != expected_proposal:
        _fail("ledger.proposal:frozen_binding_mismatch")

    decision = _expect_object(data["decision_authority"], "ledger.decision_authority")
    _exact_keys(decision, DECISION_KEYS, "ledger.decision_authority")
    expected_decision = {
        "phase": "phase_3",
        "policy_ids": ["DRR-15"],
        "authorization_record": "docs/plans/documentation-reconciliation/phase-3-authorization-and-plan.md",
        "normative_effect": "vocabulary_only_through_platform_glossary",
        "guarantee_activation_effect": "none",
        "proof_or_attestation_effect": "none",
    }
    if decision != expected_decision:
        _fail("ledger.decision_authority:approved_boundary_mismatch")
    _repo_path(root, decision["authorization_record"], "ledger.decision_authority.authorization_record")

    generated_views = _string_list(data["generated_views"], "ledger.generated_views", nonempty=True, sorted_values=False)
    if generated_views != [
        "docs/plans/documentation-reconciliation/phase-3-terminology-dispositions.md",
        "docs/contracts/platform/04_glossary.md",
    ]:
        _fail("ledger.generated_views:unexpected_paths_or_order")

    read_order_paths = _read_order_paths(root)
    term_rows = _expect_list(data["terms"], "ledger.terms")
    if len(term_rows) != 55:
        _fail(f"ledger.terms:expected_55:actual={len(term_rows)}")
    terms_by_id: dict[str, dict[str, Any]] = {}
    canonical_labels: dict[str, str] = {}
    term_order: list[str] = []
    for index, raw in enumerate(term_rows):
        where = f"ledger.terms[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(row, TERM_KEYS, where)
        term_id = _expect_string(row["id"], f"{where}.id")
        if not TERM_ID_RE.fullmatch(term_id) or term_id in terms_by_id:
            _fail(f"{where}.id:invalid_or_duplicate:{term_id}")
        term_order.append(term_id)
        source = proposal_terms.get(term_id)
        if source is None:
            _fail(f"{where}.id:not_in_frozen_proposal:{term_id}")
        source_label = _single_line(row["source_label"], f"{where}.source_label")
        source_status = _expect_string(row["source_status"], f"{where}.source_status")
        entry_kind = _enum(row["entry_kind"], ENTRY_KINDS, f"{where}.entry_kind")
        if (source_label, source_status, entry_kind) != (
            source.label,
            source.source_status,
            source.entry_kind,
        ):
            _fail(f"{where}:frozen_proposal_lineage_mismatch:{term_id}")
        term_lifecycle = _enum(
            row["term_lifecycle"], TERM_LIFECYCLES, f"{where}.term_lifecycle"
        )
        if term_lifecycle != source.term_lifecycle:
            _fail(f"{where}:frozen_proposal_lifecycle_mismatch:{term_id}")
        disposition = _enum(row["disposition"], TERM_DISPOSITIONS, f"{where}.disposition")
        owner, reviewers = _validate_role_fields(row, where)
        _validate_fixed_reviewers(term_id, reviewers, where)
        boundaries = _string_list(row["consulted_boundaries"], f"{where}.consulted_boundaries")
        for boundary in boundaries:
            if not SLUG_RE.fullmatch(boundary):
                _fail(f"{where}.consulted_boundaries:invalid_slug:{boundary}")
        overlap = sorted(set(boundaries) & set(reviewers))
        if overlap:
            _fail(f"{where}.consulted_boundaries:also_required_reviewer:{','.join(overlap)}")

        authority_refs = _validate_refs(
            row["authority_refs"],
            root=root,
            where=f"{where}.authority_refs",
            owner=owner,
            reviewers=reviewers,
            catalog=architecture_catalog,
            read_order_paths=read_order_paths,
            evidence_only=False,
            require_primary=disposition == "adopted",
        )
        reviewed_refs = _validate_refs(
            row["reviewed_evidence_refs"],
            root=root,
            where=f"{where}.reviewed_evidence_refs",
            owner=owner,
            reviewers=reviewers,
            catalog=architecture_catalog,
            read_order_paths=read_order_paths,
            evidence_only=True,
            require_primary=False,
        )
        required_locators = REQUIRED_TERM_AUTHORITY_LOCATORS.get(term_id, frozenset())
        missing_locators = sorted(
            required_locators - {_authority_signature(ref) for ref in authority_refs}
        )
        if missing_locators:
            _fail(f"{where}.authority_refs:missing_approved_locator:{missing_locators[0]}")

        conflicts_raw = _expect_list(row["conflict_dispositions"], f"{where}.conflict_dispositions")
        conflict_ids: list[str] = []
        conflict_states: list[str] = []
        for conflict_index, conflict_raw in enumerate(conflicts_raw):
            conflict_where = f"{where}.conflict_dispositions[{conflict_index}]"
            conflict = _expect_object(conflict_raw, conflict_where)
            _exact_keys(conflict, CONFLICT_KEYS, conflict_where)
            conflict_id = _expect_string(conflict["id"], f"{conflict_where}.id")
            if not CONFLICT_ID_RE.fullmatch(conflict_id) or conflict_id not in inventory.conflict_ids:
                _fail(f"{conflict_where}.id:not_in_inventory:{conflict_id}")
            conflict_ids.append(conflict_id)
            conflict_states.append(
                _enum(conflict["disposition"], CONFLICT_DISPOSITIONS, f"{conflict_where}.disposition")
            )
        if conflict_ids != sorted(conflict_ids) or len(conflict_ids) != len(set(conflict_ids)):
            _fail(f"{where}.conflict_dispositions:must_be_sorted_unique")

        alias_refs = _string_list(row["alias_refs"], f"{where}.alias_refs", sorted_values=True)
        for alias_id in alias_refs:
            if not ALIAS_ID_RE.fullmatch(alias_id):
                _fail(f"{where}.alias_refs:invalid_alias_id:{alias_id}")

        rationale = _single_line(row["decision_rationale"], f"{where}.decision_rationale")
        del rationale
        adopted_label = _nullable_string(row["adopted_label"], f"{where}.adopted_label")
        definition = _nullable_string(row["definition"], f"{where}.definition")
        usage = _nullable_string(row["usage_boundary"], f"{where}.usage_boundary")
        anchor = _nullable_string(row["normative_anchor"], f"{where}.normative_anchor")
        defer_reason = _nullable_string(row["defer_reason"], f"{where}.defer_reason")
        revisit = _nullable_string(row["revisit_condition"], f"{where}.revisit_condition")

        if disposition == "adopted":
            if adopted_label is None or definition is None or usage is None:
                _fail(f"{where}:adopted_requires_label_definition_and_usage")
            adopted_label = _single_line(adopted_label, f"{where}.adopted_label")
            required_label = REQUIRED_ADOPTED_LABELS.get(term_id)
            if required_label is not None and adopted_label != required_label:
                _fail(f"{where}.adopted_label:expected_approved_label:{required_label}")
            _single_line(definition, f"{where}.definition", normative_scan=True)
            _single_line(usage, f"{where}.usage_boundary", normative_scan=True)
            expected_anchor = f"qt-term-{term_id[-3:]}"
            if anchor != expected_anchor:
                _fail(f"{where}.normative_anchor:expected:{expected_anchor}")
            if defer_reason is not None or revisit is not None or reviewed_refs:
                _fail(f"{where}:adopted_forbids_defer_or_reviewed_evidence_fields")
            if "open_blocking" in conflict_states:
                _fail(f"{where}:adopted_forbidden_with_open_blocking_conflict")
            if source_status == "blocked" and "resolved_by_authority" not in conflict_states:
                _fail(f"{where}:blocked_source_requires_resolved_authority")
            key = _semantic_label_key(adopted_label)
            if not key or key in canonical_labels:
                _fail(f"{where}.adopted_label:normalized_collision:{adopted_label}")
            canonical_labels[key] = term_id
        else:
            if any(value is not None for value in (adopted_label, definition, usage, anchor)):
                _fail(f"{where}:deferred_forbids_normative_definition")
            if authority_refs:
                _fail(f"{where}:deferred_forbids_authority_refs")
            if not reviewed_refs or defer_reason is None or revisit is None:
                _fail(f"{where}:deferred_requires_evidence_reason_and_revisit")
            _single_line(defer_reason, f"{where}.defer_reason")
            _single_line(revisit, f"{where}.revisit_condition")
            if "open_blocking" not in conflict_states:
                _fail(f"{where}:deferred_requires_open_blocking_conflict")
        terms_by_id[term_id] = dict(row)

    if tuple(term_order) != TERM_IDS:
        _fail("ledger.terms:ids_must_be_sorted_exact_001_through_055")
    deferred_ids = {term_id for term_id, row in terms_by_id.items() if row["disposition"] == "deferred"}
    if deferred_ids != DEFERRED_TERM_IDS:
        _fail(f"ledger.terms:deferred_set_mismatch:{','.join(sorted(deferred_ids))}")
    if sum(row["disposition"] == "adopted" for row in terms_by_id.values()) != 53:
        _fail("ledger.terms:expected_53_adopted")

    alias_rows = _expect_list(data["aliases"], "ledger.aliases")
    if len(alias_rows) != 20:
        _fail(f"ledger.aliases:expected_20:actual={len(alias_rows)}")
    aliases_by_id: dict[str, dict[str, Any]] = {}
    alias_order: list[str] = []
    cross_row_alias_labels: dict[str, str] = {}
    for index, raw in enumerate(alias_rows):
        where = f"ledger.aliases[{index}]"
        row = _expect_object(raw, where)
        _exact_keys(row, ALIAS_KEYS, where)
        alias_id = _expect_string(row["id"], f"{where}.id")
        if not ALIAS_ID_RE.fullmatch(alias_id) or alias_id in aliases_by_id:
            _fail(f"{where}.id:invalid_or_duplicate:{alias_id}")
        alias_order.append(alias_id)
        source = inventory.aliases.get(alias_id)
        if source is None:
            _fail(f"{where}.id:not_in_alias_inventory:{alias_id}")
        source_finding = _single_line(row["source_finding"], f"{where}.source_finding")
        source_classification = _single_line(
            row["source_classification"], f"{where}.source_classification"
        )
        if (source_finding, source_classification) != (
            source.source_finding,
            source.source_classification,
        ):
            _fail(f"{where}:frozen_alias_lineage_mismatch:{alias_id}")
        disposition = _enum(row["disposition"], ALIAS_DISPOSITIONS, f"{where}.disposition")
        classification = _enum(row["classification"], ALIAS_CLASSIFICATIONS, f"{where}.classification")
        labels = _string_list(row["labels"], f"{where}.labels", nonempty=disposition == "ratified", sorted_values=False)
        for label_index, label in enumerate(labels):
            _single_line(label, f"{where}.labels[{label_index}]")
            exact_key = _exact_label_key(label)
            if sum(_exact_label_key(other) == exact_key for other in labels) > 1:
                _fail(f"{where}.labels:duplicate_normalized_label:{label}")
            semantic_key = _semantic_label_key(label)
            if not semantic_key:
                _fail(f"{where}.labels:empty_after_normalization:{label}")
            previous_alias = cross_row_alias_labels.get(semantic_key)
            if previous_alias is not None and previous_alias != alias_id:
                _fail(f"{where}.labels:cross_row_normalized_collision:{previous_alias}:{alias_id}")
            cross_row_alias_labels[semantic_key] = alias_id

        term_refs = _string_list(row["term_refs"], f"{where}.term_refs", nonempty=disposition == "ratified")
        for term_id in term_refs:
            if not TERM_ID_RE.fullmatch(term_id) or term_id not in terms_by_id:
                _fail(f"{where}.term_refs:unknown_term:{term_id}")
            if disposition == "ratified" and terms_by_id[term_id]["disposition"] != "adopted":
                _fail(f"{where}.term_refs:ratified_alias_requires_adopted_term:{term_id}")
        for label in labels:
            canonical_term_id = canonical_labels.get(_semantic_label_key(label))
            if canonical_term_id is not None and canonical_term_id not in term_refs:
                _fail(
                    f"{where}.labels:collides_with_unreferenced_canonical_term:"
                    f"{canonical_term_id}"
                )
        owner, reviewers = _validate_role_fields(row, where)
        _validate_fixed_reviewers(alias_id, reviewers, where)
        authority_refs = _validate_refs(
            row["authority_refs"],
            root=root,
            where=f"{where}.authority_refs",
            owner=owner,
            reviewers=reviewers,
            catalog=architecture_catalog,
            read_order_paths=read_order_paths,
            evidence_only=False,
            require_primary=disposition == "ratified",
        )
        handling = _nullable_string(row["scope_and_handling"], f"{where}.scope_and_handling")
        automatic = _expect_bool(row["automatic_replacement"], f"{where}.automatic_replacement")
        replacement = _nullable_string(row["replacement_term_id"], f"{where}.replacement_term_id")
        anchor = _nullable_string(row["normative_anchor"], f"{where}.normative_anchor")
        _single_line(row["decision_rationale"], f"{where}.decision_rationale")
        defer_reason = _nullable_string(row["defer_reason"], f"{where}.defer_reason")
        revisit = _nullable_string(row["revisit_condition"], f"{where}.revisit_condition")

        if classification in {"historical", "discouraged", "rejected"} and automatic:
            _fail(f"{where}:classification_forbids_automatic_replacement")
        if automatic:
            if classification not in {"code_spelling", "compatibility"}:
                _fail(f"{where}:automatic_replacement_requires_compatible_classification")
            if replacement is None or not TERM_ID_RE.fullmatch(replacement):
                _fail(f"{where}.replacement_term_id:must_target_term_directly")
            if replacement not in term_refs or terms_by_id[replacement]["disposition"] != "adopted":
                _fail(f"{where}.replacement_term_id:must_target_referenced_adopted_term")
            if term_refs != [replacement]:
                _fail(f"{where}.replacement_term_id:automatic_replacement_requires_one_target")
        elif replacement is not None:
            _fail(f"{where}:nonautomatic_alias_forbids_replacement_target")

        if disposition == "ratified":
            if handling is None:
                _fail(f"{where}:ratified_requires_scope_and_handling")
            _single_line(handling, f"{where}.scope_and_handling", normative_scan=True)
            expected_anchor = f"qt-alias-{alias_id[-3:]}"
            if anchor != expected_anchor:
                _fail(f"{where}.normative_anchor:expected:{expected_anchor}")
            if defer_reason is not None or revisit is not None:
                _fail(f"{where}:ratified_forbids_defer_fields")
        else:
            if handling is not None or authority_refs or anchor is not None or automatic or replacement is not None:
                _fail(f"{where}:deferred_forbids_normative_alias_rule")
            if defer_reason is None or revisit is None:
                _fail(f"{where}:deferred_requires_reason_and_revisit")
        aliases_by_id[alias_id] = dict(row)

    if tuple(alias_order) != ALIAS_IDS:
        _fail("ledger.aliases:ids_must_be_sorted_exact_001_through_020")
    if sum(row["disposition"] == "ratified" for row in aliases_by_id.values()) != 20:
        _fail("ledger.aliases:expected_all_20_ratified")

    for term_id, term in terms_by_id.items():
        expected_aliases = sorted(
            alias_id for alias_id, alias in aliases_by_id.items() if term_id in alias["term_refs"]
        )
        if term["alias_refs"] != expected_aliases:
            _fail(f"ledger.alias_crosswalk:not_bidirectional:{term_id}")

    return dict(data)


def validate_navigation(root: Path) -> None:
    """Validate the required adopted-glossary reading and navigation links."""

    _read_order_paths(root)
    checks = {
        "docs/contracts/README.md": ["[adopted platform glossary](platform/04_glossary.md)"],
        "docs/index.md": ["[Platform glossary](contracts/platform/04_glossary.md)"],
        "AGENTS.md": ["`docs/contracts/platform/04_glossary.md`"],
        "docs/assurance/guarantees/README.md": [
            "[`docs/contracts/platform/04_glossary.md`](../../contracts/platform/04_glossary.md)",
            "[historical terminology proposal](../../plans/documentation-reconciliation/proposed-glossary.md)",
        ],
        "docs/plans/documentation-reconciliation/README.md": [
            "`phase-3-terminology-dispositions.json`",
            "`phase-3-terminology-dispositions.md`",
            "`../../contracts/platform/04_glossary.md`",
        ],
    }
    for repo_path, needles in checks.items():
        _, path = _repo_path(root, repo_path, f"navigation.{repo_path}")
        text = path.read_text(encoding="utf-8")
        for needle in needles:
            count = text.count(needle)
            if count != 1:
                _fail(f"navigation:{repo_path}:requires_once:{needle}:actual={count}")


def validate_repository(root: Path = ROOT) -> GlossaryBundle:
    """Validate frozen inputs, the ledger, authority, and navigation."""

    root = root.resolve()
    proposal_terms = parse_frozen_proposal(root / PROPOSAL_PATH.relative_to(ROOT))
    inventory = parse_terminology_inventory(root / INVENTORY_PATH.relative_to(ROOT))
    try:
        catalog = architecture_index.build_catalog(root)
    except architecture_index.ArchitectureMetadataError as exc:
        _fail(f"architecture_catalog_invalid:{exc}")
    data = load_json_strict(root / LEDGER_PATH.relative_to(ROOT))
    validated = validate_disposition_data(
        data,
        root=root,
        proposal_terms=proposal_terms,
        inventory=inventory,
        architecture_catalog=catalog,
    )
    validate_navigation(root)
    return GlossaryBundle(validated, proposal_terms, inventory, catalog, root)


def _anchor_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold().replace("`", "")
    normalized = re.sub(r"[^\w\- ]", "", normalized, flags=re.UNICODE)
    return re.sub(r"-+", "-", re.sub(r"\s+", "-", normalized)).strip("-")


def _authority_link(ref: Mapping[str, Any], output_repo_path: str) -> str:
    path = ref["path"]
    heading = ref["locator"]["value"]
    relative = posixpath.relpath(path, PurePosixPath(output_repo_path).parent.as_posix())
    return f"[{PurePosixPath(path).name} — {heading}]({relative}#{_anchor_slug(heading)})"


def _code_list(values: Sequence[str]) -> str:
    return ", ".join(f"`{value}`" for value in values)


def _table_text(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _conflict_summary(term: Mapping[str, Any], *, sentence: bool = False) -> str:
    conflicts = term["conflict_dispositions"]
    if not conflicts:
        return "none recorded for this term." if sentence else "none"
    separator = "; " if sentence else "<br>"
    values = separator.join(f"`{item['id']}`: `{item['disposition']}`" for item in conflicts)
    if sentence:
        values = values.replace(":", " is")
        return values + ". Historical evidence and separate documentation repairs remain preserved."
    return values


def render_disposition_view(bundle: GlossaryBundle) -> str:
    """Render the complete non-normative Phase 3 terminology review view."""

    data = bundle.data
    terms = data["terms"]
    aliases = data["aliases"]
    adopted_count = sum(term["disposition"] == "adopted" for term in terms)
    deferred_count = sum(term["disposition"] == "deferred" for term in terms)
    ratified_count = sum(alias["disposition"] == "ratified" for alias in aliases)
    alias_deferred_count = sum(alias["disposition"] == "deferred" for alias in aliases)
    output_path = "docs/plans/documentation-reconciliation/phase-3-terminology-dispositions.md"

    lines = [
        "# Phase 3 Terminology Adoption And Disposition Record",
        "",
        "<!-- Generated from phase-3-terminology-dispositions.json. Do not edit by hand. -->",
        "",
        "> **Non-normative audit view.** This record explains the Phase 3 vocabulary",
        "> decisions. It is not product authority, does not activate a guarantee, does",
        "> not attest enforcement or proof, and does not resolve a remediation by itself.",
        "",
        "## Source Binding",
        "",
        f"- Frozen baseline: `{data['audit_baseline_commit']}`",
        "- Frozen proposal: [`docs/plans/documentation-reconciliation/proposed-glossary.md`](proposed-glossary.md)",
        f"- Frozen proposal SHA-256: `{data['proposal']['sha256']}`",
        "- Approved policy: `DRR-15`, applied term by term after substantive Phase 3 decisions",
        "- Normative vocabulary target: [`docs/contracts/platform/04_glossary.md`](../../contracts/platform/04_glossary.md)",
        "",
        "The proposal and Phase 1 inventory remain unchanged historical evidence. An",
        "adopted row becomes vocabulary authority only through the checked-in platform",
        "glossary. Deferred rows remain unadopted.",
        "",
        "## Derived Accounting",
        "",
        "| Population | Adopted/ratified | Deferred | Rejected | Total |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| Terms | {adopted_count} | {deferred_count} | 0 | {len(terms)} |",
        f"| Aliases | {ratified_count} | {alias_deferred_count} | — | {len(aliases)} |",
        "",
        "Counts are derived from the rows below. The validator also binds the approved",
        "deferred set to `QT-TERM-035` and `QT-TERM-055`; every other term is adopted.",
        "",
        "## Term Decisions",
        "",
        "| ID | Phase 2 label/status | Phase 3 disposition | Adopted label | Owner / required reviewers | Exact authority | Conflict result | Glossary anchor or blocker |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for term in terms:
        refs = term["authority_refs"] or term["reviewed_evidence_refs"]
        authorities = "<br>".join(_authority_link(ref, output_path) for ref in refs)
        label = term["adopted_label"] if term["adopted_label"] is not None else "—"
        if term["disposition"] == "adopted":
            destination = f"[definition](../../contracts/platform/04_glossary.md#{term['normative_anchor']})"
        else:
            destination = f"Deferred — {term['defer_reason']}"
        lines.append(
            f"| `{term['id']}` | {_table_text(term['source_label'])} (`{term['source_status']}`) | "
            f"`{term['disposition']}` | {_table_text(label)} | `{term['owner']}` / "
            f"{_code_list(term['required_reviewers'])} | {authorities} | "
            f"{_conflict_summary(term)} | {_table_text(destination)} |"
        )

    lines.extend(
        [
            "",
            "## Alias Decisions",
            "",
            "Every Phase 1 alias finding receives an explicit disposition. Ratification of a",
            "historical, discouraged, or rejected spelling does not make that spelling",
            "canonical and never rewrites historical evidence.",
            "",
            "| ID | Frozen finding | Disposition / classification | Canonical term refs | Owner / required reviewers | Scope and handling | Exact authority | Automatic replacement |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for alias in aliases:
        authorities = "<br>".join(_authority_link(ref, output_path) for ref in alias["authority_refs"])
        automatic = "true" if alias["automatic_replacement"] else "false"
        if alias["replacement_term_id"]:
            automatic += f" → `{alias['replacement_term_id']}`"
        lines.append(
            f"| `{alias['id']}` | {_table_text(alias['source_finding'])} | "
            f"`{alias['disposition']}` / `{alias['classification']}` | "
            f"{_code_list(alias['term_refs'])} | `{alias['owner']}` / "
            f"{_code_list(alias['required_reviewers'])} | "
            f"{_table_text(alias['scope_and_handling'] or '—')} | {authorities} | `{automatic}` |"
        )

    deferred = [term for term in terms if term["disposition"] == "deferred"]
    lines.extend(["", "## Deferred Decision Detail", ""])
    for index, term in enumerate(deferred):
        lines.extend(
            [
                f"### `{term['id']}` — {term['source_label']}",
                "",
                f"- Residual authority gap: {term['defer_reason']}",
                f"- Revisit condition: {term['revisit_condition']}",
                "- Guarantee effect: any guarantee that depends on this term remains unable",
                "  to satisfy an adopted-terminology activation prerequisite; no guarantee is",
                "  activated or reclassified by this record.",
            ]
        )
        if index != len(deferred) - 1:
            lines.append("")
    lines.extend(
        [
            "",
            "## Preservation And Activation Boundary",
            "",
            "This record preserves the frozen proposal, Phase 1 inventory, Gate reports,",
            "decision packet, guarantee registry, remediation records, proof catalog, and",
            "attestations. Vocabulary adoption is separate from documentation repair,",
            "enforcement, proof execution, attestation, remediation success, and guarantee",
            "activation.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_platform_glossary(bundle: GlossaryBundle) -> str:
    """Render the normative vocabulary index from reviewed adopted rows only."""

    terms = bundle.data["terms"]
    aliases = bundle.data["aliases"]
    adopted = [term for term in terms if term["disposition"] == "adopted"]
    deferred = [term for term in terms if term["disposition"] == "deferred"]
    ratified = [alias for alias in aliases if alias["disposition"] == "ratified"]
    output_path = "docs/contracts/platform/04_glossary.md"
    lines = [
        "# QT Platform Glossary",
        "",
        "<!-- Generated by scripts/docs/glossary.py from retained terminology disposition data. Do not edit by hand. -->",
        "",
        "> **Normative vocabulary index.** This file adopts reviewed vocabulary,",
        "> distinctions, and alias usage only. It does not create or broaden product",
        "> behavior. Each entry is bounded by its cited platform contract or accepted",
        "> ADR; the owning authority controls if this summary differs.",
        "",
        "## Authority Boundary",
        "",
        "A standardized term says only that its label, distinction, and usage boundary",
        "are accepted. Standardizing a word does not establish enforcement, report a",
        "test result, resolve a remediation, or activate a guarantee. The glossary is",
        "subordinate to the authorities it indexes and is not an alternate behavioral",
        "contract.",
        "",
        "The [historical terminology proposal](../../plans/documentation-reconciliation/proposed-glossary.md)",
        "and [review disposition record](../../plans/documentation-reconciliation/phase-3-terminology-dispositions.md)",
        "remain historical evidence, not product authority. Deferred IDs have no",
        "normative definition here.",
        "",
        "## Core Vocabulary Path",
        "",
        "Start here when a QT term is unfamiliar. The links lead to the standard",
        "definition or to the owning boundary when the concept is broader than one",
        "glossary entry.",
        "",
        "| Idea | Standard QT wording and important distinction |",
        "| --- | --- |",
        "| Stored market truth | A [Canonical Fact](#qt-term-001) is one immutable provider-neutral observation. A correction is a new [Fact revision](#qt-term-002) with the same [Observation key](#qt-term-003), not a rewrite. |",
        "| Reproducible source evidence | A [Frozen Dataset](#qt-term-007) identifies exact Fact revisions; a [Frozen Market Data Read Binding](#qt-term-008) identifies the exact slice a consumer used. |",
        "| Measurement | An [Indicator](../../architecture/indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md) advances causally and publishes typed outputs; output catalog and readiness vocabulary is standardized in [QT-TERM-036](#qt-term-036). |",
        "| Research question | A [Check](#qt-term-011) is bounded analysis. A [Check preview](#qt-term-012) is ephemeral and cannot silently become durable evidence or a [Research Observation](#qt-term-006). |",
        "| Decision logic | A [Strategy](#qt-term-040) consumes typed Indicator outputs and creates decisions; it does not own fills, wallet state, or execution truth. |",
        "| Execution event / record | The informal phrase **execution event** must resolve to a precise owner type—decision, canonical order transition, fill, wallet-ledger fact, or runtime event—rather than making those records interchangeable. See [runtime fact](#qt-term-005) and the [runtime contract](01_runtime_contract.md). |",
        "| Inspection and reporting | [RunResearchDataset](#qt-term-009) is the canonical run-level reporting read model. [Reports](#qt-term-024) and [BotLens projections](#qt-term-047) explain durable truth; they do not create it. |",
        "",
        "Deprecated, discouraged, compatibility, and historical spellings are recorded",
        "under [Alias and Historical-Usage Rules](#alias-and-historical-usage-rules). An",
        "old alias never silently changes the meaning of a current term.",
        "",
        "## Vocabulary Coverage",
        "",
        "| Disposition | Count |",
        "| --- | ---: |",
        f"| Adopted terms | {len(adopted)} |",
        f"| Deferred terms | {len(deferred)} |",
        f"| Ratified alias rules | {len(ratified)} |",
        "",
        "## Standard Term Index",
        "",
        "| ID | Adopted label | Kind | Owner |",
        "| --- | --- | --- | --- |",
    ]
    for term in adopted:
        lines.append(
            f"| [`{term['id']}`](#{term['normative_anchor']}) | "
            f"{_table_text(term['adopted_label'])} | `{term['entry_kind']}` | `{term['owner']}` |"
        )
    lines.extend(["", "## Standard Definitions", ""])
    for index, term in enumerate(adopted):
        lines.extend(
            [
                f"<a id=\"{term['normative_anchor']}\"></a>",
                f"### `{term['id']}` — {term['adopted_label']}",
                "",
                "- Adoption status: `adopted`",
                f"- Entry kind: `{term['entry_kind']}`",
                f"- Owner: `{term['owner']}`",
                f"- Required reviewers: {_code_list(term['required_reviewers'])}",
            ]
        )
        if term["consulted_boundaries"]:
            lines.append(f"- Consulted boundaries: {_code_list(term['consulted_boundaries'])}")
        lines.append("- Authority clauses:")
        for ref in term["authority_refs"]:
            lines.append(
                f"  - `{ref['authority_kind']}` — {_authority_link(ref, output_path)}"
            )
        lines.extend(
            [
                f"- Definition: {term['definition']}",
                f"- Usage boundary: {term['usage_boundary']}",
                f"- Conflict disposition: {_conflict_summary(term, sentence=True)}",
            ]
        )
        if index != len(adopted) - 1:
            lines.append("")

    lines.extend(
        [
            "",
            "## Alias and Historical-Usage Rules",
            "",
            "Ratified aliases are reviewed usage rules, not additional canonical terms.",
            "Historical, discouraged, and rejected spellings remain preserved in historical",
            "evidence and are never automatic replacements.",
            "",
        ]
    )
    for index, alias in enumerate(ratified):
        labels = ", ".join(f"`{label}`" for label in alias["labels"])
        term_links = ", ".join(
            f"[`{term_id}`](#qt-term-{term_id[-3:]})" for term_id in alias["term_refs"]
        )
        lines.extend(
            [
                f"<a id=\"{alias['normative_anchor']}\"></a>",
                f"### `{alias['id']}` — {labels}",
                "",
                f"- Review status: `{alias['disposition']}`",
                f"- Classification: `{alias['classification']}`",
                f"- Canonical term references: {term_links}",
                f"- Owner: `{alias['owner']}`",
                f"- Required reviewers: {_code_list(alias['required_reviewers'])}",
                "- Authority clauses:",
            ]
        )
        for ref in alias["authority_refs"]:
            lines.append(
                f"  - `{ref['authority_kind']}` — {_authority_link(ref, output_path)}"
            )
        automatic = "true" if alias["automatic_replacement"] else "false"
        if alias["replacement_term_id"]:
            automatic += f" → `{alias['replacement_term_id']}`"
        lines.extend(
            [
                f"- Scope and handling: {alias['scope_and_handling']}",
                f"- Automatic replacement: `{automatic}`",
            ]
        )
        if index != len(ratified) - 1:
            lines.append("")

    lines.extend(
        [
            "",
            "## Deferred Terms",
            "",
            "The following IDs are deliberately absent from the adopted definitions. Their",
            "proposal wording remains non-normative.",
            "",
            "| ID | Disposition | Residual authority gap | Revisit condition |",
            "| --- | --- | --- | --- |",
        ]
    )
    for term in deferred:
        lines.append(
            f"| `{term['id']}` | `{term['disposition']}` | "
            f"{_table_text(term['defer_reason'])} | {_table_text(term['revisit_condition'])} |"
        )
    return "\n".join(lines) + "\n"


def _rendered_views(bundle: GlossaryBundle) -> dict[str, bytes]:
    return {
        "docs/plans/documentation-reconciliation/phase-3-terminology-dispositions.md": render_disposition_view(bundle).encode("utf-8"),
        "docs/contracts/platform/04_glossary.md": render_platform_glossary(bundle).encode("utf-8"),
    }


def _validate_normative_output(bundle: GlossaryBundle, content: bytes) -> None:
    try:
        text = content.decode("utf-8")
    except UnicodeError as exc:
        _fail(f"platform_glossary:invalid_utf8:{exc}")
    adopted = [term for term in bundle.data["terms"] if term["disposition"] == "adopted"]
    ratified = [alias for alias in bundle.data["aliases"] if alias["disposition"] == "ratified"]
    if len(re.findall(r'^### `QT-TERM-\d{3}` — ', text, flags=re.MULTILINE)) != len(adopted):
        _fail("platform_glossary:adopted_heading_count_mismatch")
    if len(re.findall(r'^### `QT-ALIAS-\d{3}` — ', text, flags=re.MULTILINE)) != len(ratified):
        _fail("platform_glossary:alias_heading_count_mismatch")
    for term_id in DEFERRED_TERM_IDS:
        suffix = term_id[-3:]
        if f'<a id="qt-term-{suffix}"></a>' in text or f"### `{term_id}` — " in text:
            _fail(f"platform_glossary:deferred_term_normatively_defined:{term_id}")
        if text.count(f"`{term_id}`") != 1:
            _fail(f"platform_glossary:deferred_term_must_appear_once_in_accounting:{term_id}")
    for phrase in sorted(FORBIDDEN_ADOPTED_OUTPUT_PHRASES):
        if phrase in text:
            _fail(f"platform_glossary:forbidden_superseded_phrase:{phrase}")


def check_generated(bundle: GlossaryBundle) -> None:
    """Require both checked-in Markdown views to equal their rendered LF bytes."""

    rendered = _rendered_views(bundle)
    _validate_normative_output(
        bundle, rendered["docs/contracts/platform/04_glossary.md"]
    )
    for repo_path, expected in rendered.items():
        path = bundle.root.joinpath(*PurePosixPath(repo_path).parts)
        if not path.is_file() or path.read_bytes() != expected:
            _fail(f"generated_view_stale:{repo_path}")


def _main(argv: Sequence[str] | None = None, *, root: Path = ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "render", "check"))
    args = parser.parse_args(argv)
    try:
        bundle = validate_repository(root)
        if args.command == "render":
            rendered = _rendered_views(bundle)
            _validate_normative_output(
                bundle, rendered["docs/contracts/platform/04_glossary.md"]
            )
            for repo_path, content in rendered.items():
                bundle.root.joinpath(*PurePosixPath(repo_path).parts).write_bytes(content)
            print("rendered 55 terminology dispositions and 20 alias decisions")
        elif args.command == "check":
            check_generated(bundle)
            print("validated 55 terms, 20 aliases, and 2 generated views")
        else:
            print("validated 55 terms and 20 aliases")
        return 0
    except GlossaryValidationError as exc:
        print(f"glossary validation failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_main())
