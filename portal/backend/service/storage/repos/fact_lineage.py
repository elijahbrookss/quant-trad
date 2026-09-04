"""Exact raw-frame admission for canonical revisions, independent of latest state.

The caller owns the lifecycle shared fence while this read-only proof runs and
while its immutable dependency holds commit. A manifest range is not a mapping.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import replace
import json

from sqlalchemy import text

from market_data.archive import (
    RAW_ARCHIVE_COMPRESSION, RAW_ARCHIVE_FORMAT, RAW_ARCHIVE_SCHEMA_VERSION,
    RawArchiveReadLimits, iter_raw_archive_parquet, read_raw_archive_content_fingerprint,
)
from market_data.canonical_storage import LEGACY_MATERIAL_EVIDENCE_KEYS


EXACT_RAW_FACT_TYPES = frozenset({"market.trade", "market.l2_book", "market.bbo", "market.depth_observation"})
BOOK_SCOPE_FIELDS = ("definition_id", "session_id", "connection_epoch", "provider_product_id")


def _witness(row):
    fact_type = row["fact_type"]
    if fact_type not in EXACT_RAW_FACT_TYPES:
        raise ValueError(f"canonical_raw_lineage_unsupported: fact_type={fact_type}")
    evidence_key = {"market.trade": "_qt_trade_evidence", "market.l2_book": "_qt_l2_evidence"}.get(
        fact_type, LEGACY_MATERIAL_EVIDENCE_KEYS.get(fact_type),
    )
    evidence = row["provenance"].get(evidence_key)
    if fact_type in {"market.bbo", "market.depth_observation"} and isinstance(evidence, Mapping):
        evidence = evidence.get("source_position")
    context = f"fact_version_id={row['id']} fact_type={fact_type}"
    if not isinstance(evidence, Mapping):
        raise RuntimeError(f"canonical_raw_lineage_evidence_missing: {context}")
    for name, minimum in (("connection_epoch", 0), ("receive_ordinal", 1)):
        value = evidence.get(name)
        if type(value) is not int or not minimum <= value <= 9223372036854775807:
            raise RuntimeError(f"canonical_raw_lineage_position_invalid: field={name} {context}")
    product = evidence.get("provider_product_id")
    if not isinstance(product, str) or not product.strip():
        raise RuntimeError(f"canonical_raw_lineage_product_missing: {context}")
    if fact_type in {"market.trade", "market.l2_book"}:
        identity = evidence.get("raw_record_id")
        if not isinstance(identity, str) or not identity.strip():
            raise RuntimeError(f"canonical_raw_lineage_record_missing: {context}")
        key = ("record", identity)
    else:
        key = ("position", evidence.get("definition_id"), evidence.get("session_id"),
               evidence["connection_epoch"], evidence["receive_ordinal"])
    if fact_type != "market.trade":
        for name in ("definition_id", "session_id"):
            if not isinstance(evidence.get(name), str) or not evidence[name].strip():
                raise RuntimeError(f"canonical_raw_lineage_position_invalid: field={name} {context}")
    return key, dict(evidence)


def _verify_witness(row, evidence, record):
    expected = {
        "provider_product_id": evidence["provider_product_id"],
        "connection_epoch": evidence["connection_epoch"], "receive_ordinal": evidence["receive_ordinal"],
    }
    for name in ("raw_record_id", "definition_id", "session_id"):
        if name in evidence:
            expected[name] = evidence[name]
    if row is None:
        # A prefix position is a raw-scope obligation, not a synthesized Fact.
        expected["requested_channel"] = "level2"
    elif row["fact_type"] in {"market.trade", "market.l2_book"}:
        expected.update(provider=row["source_provider"], venue=row["source_venue"], received_at=row["received_at"])
    # A derived BBO/depth Fact is authored by QT, not by the exchange. Its
    # provider frame is bound by its declared source position; never relabel
    # the derived author or compare it with the raw transport's provider.
    for name, value in expected.items():
        if getattr(record, name) != value:
            identity = row["id"] if row is not None else evidence["root_fact_version_id"]
            raise RuntimeError(f"canonical_raw_lineage_witness_mismatch: fact_version_id={identity} field={name}")


def _raw_records(path, *, manifest_id, limits):
    try:
        yield from iter_raw_archive_parquet(path, limits=limits)
    except (OSError, RuntimeError, ValueError) as exc:
        raise RuntimeError(f"canonical_raw_lineage_decode_failed: manifest_id={manifest_id} {exc}") from exc


def canonical_book_prefixes(rows):
    """Collapse book roots without materializing the preceding raw positions."""
    prefixes = {}
    for row in rows:
        if row["fact_type"] not in EXACT_RAW_FACT_TYPES or row["fact_type"] == "market.trade":
            continue
        _, evidence = _witness(row)
        scope = tuple(evidence[name] for name in BOOK_SCOPE_FIELDS[:3])
        prior = prefixes.get(scope)
        if prior is not None and prior["provider_product_id"] != evidence["provider_product_id"]:
            raise RuntimeError(f"canonical_raw_lineage_prefix_scope_conflict: fact_version_id={row['id']}")
        if prior is None or evidence["receive_ordinal"] > prior["receive_ordinal"]:
            prefixes[scope] = {name: evidence[name] for name in (*BOOK_SCOPE_FIELDS, "receive_ordinal")} | {
                "first_receive_ordinal": 1, "root_fact_version_id": row["id"],
            }
    return list(prefixes.values())


def resolve_canonical_raw_archive_refs(session, *, rows, object_store, byte_verifier,
                                       limits: RawArchiveReadLimits = RawArchiveReadLimits(),
                                       max_mapping_rows: int = 50_000, bound_manifest_ids=None, check_budget=None,
                                       preserve_book_prefixes: bool = False, book_prefix_ranges=None,
                                       witness_manifest_ids=None):
    """Prove exact source rows in one current acknowledged placement per witness.

    Compacted mappings may be used after an original object's recorded expiry.
    Unexpected missing/corrupt selected objects fail; there is no silent fallback.
    Decode count and logical-frame budgets apply across the complete call.
    A recheck uses its already acknowledged dependency IDs so a later compacted
    placement cannot change an otherwise valid immutable page's proof.
    Book-prefix admission additionally proves every receive ordinal from one
    through each root position. Raw control frames are included even when they
    produced no canonical book mutation; this does not reconstruct book state.
    """
    if type(max_mapping_rows) is not int or max_mapping_rows <= 0 or len(rows) > max_mapping_rows:
        raise ValueError("canonical_raw_lineage_mapping_budget_invalid")
    wanted = defaultdict(list)
    for row in rows:
        key, evidence = _witness(row)
        wanted[key].append((row, evidence))
    record_ids = [key[1] for key in wanted if key[0] == "record"]
    positions = [dict(zip(("definition_id", "session_id", "connection_epoch", "receive_ordinal"), key[1:]))
                 for key in wanted if key[0] == "position"]
    if preserve_book_prefixes and book_prefix_ranges is not None:
        raise ValueError("canonical_raw_lineage_prefix_modes_conflict")
    prefixes = canonical_book_prefixes(rows) if preserve_book_prefixes else list(book_prefix_ranges or ())
    if prefixes:
        for scope in prefixes:
            if (any(not isinstance(scope.get(name), str) or not scope[name].strip()
                    for name in ("definition_id", "session_id", "provider_product_id", "root_fact_version_id"))
                    or type(scope.get("connection_epoch")) is not int or not 0 <= scope["connection_epoch"] <= 2**63 - 1
                    or type(scope.get("first_receive_ordinal")) is not int or type(scope.get("receive_ordinal")) is not int
                    or not 1 <= scope["first_receive_ordinal"] <= scope["receive_ordinal"] <= 2**63 - 1):
                raise ValueError("canonical_raw_lineage_prefix_range_invalid")
        if sum(scope["receive_ordinal"] - scope["first_receive_ordinal"] + 1 for scope in prefixes) > max_mapping_rows:
            raise RuntimeError("canonical_raw_lineage_prefix_budget_exceeded: complete book prefixes exceed the mapping budget")
        for scope in prefixes:
            key = tuple(scope[name] for name in BOOK_SCOPE_FIELDS[:3])
            for ordinal in range(scope["first_receive_ordinal"], scope["receive_ordinal"] + 1):
                wanted[("position", *key, ordinal)].append((None, {**scope, "receive_ordinal": ordinal}))
    if not wanted:
        return {}
    matches = []
    bound_predicate = "" if bound_manifest_ids is None else "AND manifests.id = ANY(:bound_ids)"
    bound_params = {} if bound_manifest_ids is None else {"bound_ids": sorted(set(bound_manifest_ids))}
    # Both predicates address the exact record mapping, never a manifest's
    # min/max ordinal range (which can contain holes or another reconnect).
    queries = [
        ("mappings.raw_record_id = ANY(:ids)", {"ids": record_ids}),
        ("EXISTS (SELECT 1 FROM jsonb_to_recordset(CAST(:positions AS jsonb)) "
         "AS positions(definition_id text, session_id text, connection_epoch bigint, receive_ordinal bigint) "
         "WHERE positions.definition_id=manifests.definition_id AND positions.session_id=mappings.session_id "
         "AND positions.session_id=manifests.session_id AND positions.connection_epoch=manifests.connection_epoch "
         "AND manifests.first_receive_ordinal<=positions.receive_ordinal AND manifests.last_receive_ordinal>=positions.receive_ordinal "
         "AND positions.connection_epoch=mappings.connection_epoch AND positions.receive_ordinal=mappings.receive_ordinal)",
         {"positions": json.dumps(positions)}),
        ("EXISTS (SELECT 1 FROM jsonb_to_recordset(CAST(:prefixes AS jsonb)) "
         "AS prefixes(definition_id text, session_id text, connection_epoch bigint, first_receive_ordinal bigint, receive_ordinal bigint) "
         "WHERE prefixes.definition_id=manifests.definition_id AND prefixes.session_id=manifests.session_id "
         "AND prefixes.session_id=mappings.session_id AND prefixes.connection_epoch=manifests.connection_epoch "
         "AND prefixes.connection_epoch=mappings.connection_epoch "
         "AND mappings.receive_ordinal BETWEEN prefixes.first_receive_ordinal AND prefixes.receive_ordinal)",
         {"prefixes": json.dumps(prefixes)}),
    ]
    for predicate, params in queries:
        if not (record_ids if "ids" in params else positions if "positions" in params else prefixes):
            continue
        found = session.execute(text(f"""
            SELECT manifests.*, mappings.raw_record_id, mappings.object_row_index, mappings.object_row_group,
                   mappings.spool_segment_id AS mapped_segment_id, mappings.session_id AS mapped_session_id,
                   mappings.connection_epoch AS mapped_epoch, mappings.receive_ordinal AS mapped_ordinal,
                   mappings.raw_frame_sha256 AS mapped_frame_sha256
            FROM market.raw_archive_record_mappings AS mappings
            JOIN market.raw_archive_manifests AS manifests ON manifests.id=mappings.manifest_id
            WHERE {predicate} {bound_predicate} AND NOT EXISTS (
                SELECT 1 FROM market.storage_lifecycle_events AS expired
                WHERE expired.action='archive_expire' AND expired.event_type='completed'
                  AND expired.target_kind='raw_manifest' AND expired.target_id=manifests.id)
            ORDER BY mappings.raw_record_id, manifests.byte_count, manifests.id
            LIMIT :limit
        """), {**params, **bound_params, "limit": max_mapping_rows - len(matches) + 1}).mappings().all()
        matches.extend(dict(item) for item in found)
        if len(matches) > max_mapping_rows:
            raise RuntimeError("canonical_raw_lineage_mapping_budget_exceeded: reduce canonical page size")
    candidates = defaultdict(dict)
    for mapping in matches:
        keys = (("record", mapping["raw_record_id"]),
                ("position", mapping["definition_id"], mapping["mapped_session_id"],
                 mapping["mapped_epoch"], mapping["mapped_ordinal"]))
        for key in keys:
            if key in wanted:
                prior = candidates[key].setdefault(mapping["id"], mapping)
                if prior != mapping:
                    raise RuntimeError(f"canonical_raw_lineage_mapping_conflict: manifest_id={mapping['id']}")
    selected = defaultdict(list)
    manifests = {}
    for key, witnesses in wanted.items():
        available = list(candidates[key].values())
        if witness_manifest_ids is not None:
            for row, _ in witnesses:
                if row is not None and row["id"] in witness_manifest_ids:
                    available = [item for item in available if item["id"] in witness_manifest_ids[row["id"]]]
        context = witnesses[0][0]["id"] if witnesses[0][0] is not None else witnesses[0][1]["root_fact_version_id"]
        if not available:
            raise RuntimeError(f"canonical_raw_lineage_mapping_missing: fact_version_id={context} raw_position={key}")
        if len({item["raw_record_id"] for item in available}) != 1:
            raise RuntimeError(f"canonical_raw_lineage_position_ambiguous: fact_version_id={context}")
        mapping = min(available, key=lambda item: (item["byte_count"], item["id"]))
        manifests[mapping["id"]] = mapping
        selected[mapping["id"]].append((mapping, witnesses))
    references = {}
    decoded_count = decoded_bytes = 0
    for identity in sorted(selected):
        manifest = manifests[identity]
        if (manifest["format"], manifest["schema_version"], manifest["compression"]) != (
                RAW_ARCHIVE_FORMAT, RAW_ARCHIVE_SCHEMA_VERSION, RAW_ARCHIVE_COMPRESSION):
            raise RuntimeError(f"canonical_raw_lineage_archive_format_invalid: manifest_id={identity}")
        byte_verifier.verify(manifest["object_key"], manifest["object_sha256"], expected_bytes=manifest["byte_count"])
        required = defaultdict(list)
        for mapping, witnesses in selected[identity]:
            index = mapping["object_row_index"]
            # The v1 placement writer records global row offsets and a zero
            # group placeholder, including for compacted objects.
            if type(index) is not int or not 0 <= index < manifest["record_count"] or mapping["object_row_group"] != 0:
                raise RuntimeError(f"canonical_raw_lineage_row_index_invalid: manifest_id={identity}")
            required[index].append((mapping, witnesses))
        if decoded_count >= limits.max_rows or decoded_bytes >= limits.max_logical_bytes:
            raise RuntimeError("canonical_raw_lineage_decode_budget_exceeded: reduce canonical page size")
        remaining = replace(limits, max_rows=limits.max_rows - decoded_count,
                            max_logical_bytes=limits.max_logical_bytes - decoded_bytes)
        count = first = last = 0
        for index, record in enumerate(_raw_records(object_store.local_path(manifest["object_key"]), manifest_id=identity, limits=remaining)):
            if check_budget is not None:
                check_budget()
            decoded_count += 1
            # int64 epoch/ordinal, timestamp, and 11 variable-column offsets;
            # matches the writer-owned non-null Arrow schema's logical size.
            decoded_bytes += len(record.raw_frame) + 68 + sum(
                len(value.encode("utf-8")) for value in record.__dict__.values() if isinstance(value, str))
            if decoded_count > limits.max_rows or decoded_bytes > limits.max_logical_bytes:
                raise RuntimeError("canonical_raw_lineage_decode_budget_exceeded: reduce canonical page size")
            if (record.definition_id, record.session_id, record.connection_epoch) != (
                    manifest["definition_id"], manifest["session_id"], manifest["connection_epoch"]):
                raise RuntimeError(f"canonical_raw_lineage_archive_scope_mismatch: manifest_id={identity}")
            count += 1
            first = first or record.receive_ordinal
            last = record.receive_ordinal
            for mapping, witnesses in required.pop(index, ()):
                actual = (record.raw_record_id, record.spool_segment_id, record.session_id,
                          record.connection_epoch, record.receive_ordinal, record.raw_frame_sha256)
                expected = (mapping["raw_record_id"], mapping["mapped_segment_id"], mapping["mapped_session_id"],
                            mapping["mapped_epoch"], mapping["mapped_ordinal"], mapping["mapped_frame_sha256"])
                if actual != expected:
                    raise RuntimeError(f"canonical_raw_lineage_mapping_mismatch: manifest_id={identity} row_index={index}")
                for row, evidence in witnesses:
                    _verify_witness(row, evidence, record)
        if required or (count, first, last) != (manifest["record_count"], manifest["first_receive_ordinal"], manifest["last_receive_ordinal"]):
            raise RuntimeError(f"canonical_raw_lineage_archive_coverage_mismatch: manifest_id={identity}")
        fingerprint = read_raw_archive_content_fingerprint(
            object_store.local_path(manifest["object_key"]), limits=remaining, check_budget=check_budget,
        )
        if fingerprint != manifest["content_fingerprint"]:
            raise RuntimeError(f"canonical_raw_lineage_content_fingerprint_mismatch: manifest_id={identity}")
        references[identity] = {name: manifest[name] for name in ("object_key", "object_uri", "object_sha256", "content_fingerprint")}
    byte_verifier.assert_unchanged()
    return references
