"""Immutable canonical Fact pages and placement-independent selection.

The repository chooses which manifests are visible in a database snapshot.
This module verifies their bytes and applies the same causal predicates to
hot and cold revisions before choosing the visible revision.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Any

from core.execution_control import consume_execution_json, consume_execution_resource, execution_checkpoint

from .archive import RawArchiveObjectStore
from .contracts import DatasetSeriesRequest
from .fact_archive import (
    FactArchiveLimits,
    FactArchiveManifest,
    _json_bytes,
    publish_canonical_fact_archive,
    read_admitted_canonical_fact_rows,
    read_canonical_fact_archive,
)


def _logical_row(source: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(source)
    row.pop("storage_day", None)
    return row


def publish_cold_page(
    rows: Iterable[Mapping[str, Any]],
    *,
    object_store: RawArchiveObjectStore,
    temporary_directory: Path,
    limits: FactArchiveLimits = FactArchiveLimits(),
) -> FactArchiveManifest:
    """Publish and verify one bounded page. Database admission belongs to the caller."""
    entries = []
    logical_bytes = 0
    for source in rows:
        execution_checkpoint()
        if len(entries) == limits.max_rows:
            raise ValueError("canonical_archive_limit_exceeded: cold page row count")
        row = _logical_row(source)
        logical_bytes += len(_json_bytes(row))
        if logical_bytes > limits.max_logical_bytes:
            raise ValueError("canonical_archive_limit_exceeded: cold page logical bytes")
        entries.append(row)
    entries.sort(key=lambda row: (row["market_commit_seq"], row["id"]))
    return publish_canonical_fact_archive(
        entries, object_store=object_store, temporary_directory=temporary_directory, limits=limits,
    )


@dataclass(frozen=True)
class ColdFactLocation:
    """An exact Fact locator in the page's immutable commit/ID order."""

    manifest: FactArchiveManifest
    ordinal: int

    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or not 0 <= self.ordinal < self.manifest.row_count:
            raise ValueError(
                f"canonical_cold_ordinal_invalid: manifest_id={self.manifest.manifest_id} ordinal={self.ordinal}"
            )


@dataclass(frozen=True)
class _Selection:
    span: DatasetSeriesRequest
    as_of_commit_seq: int | None
    known_at_lte: datetime | None
    source_identity_keys: frozenset[str]

    @classmethod
    def create(cls, series_id, start, end, as_of_commit_seq, known_at_lte, source_identity_keys):
        if known_at_lte is not None and (
            not isinstance(known_at_lte, datetime) or known_at_lte.utcoffset() is None
        ):
            raise ValueError("canonical_known_at_cutoff_invalid: timezone-aware timestamp required")
        return cls(
            DatasetSeriesRequest(series_id=series_id, start=start, end=end),
            int(as_of_commit_seq) if as_of_commit_seq is not None else None,
            known_at_lte,
            frozenset(key for value in source_identity_keys if (key := str(value).strip())),
        )

    def matches(self, row: Mapping[str, Any]) -> bool:
        return (
            row["series_id"] == self.span.series_id
            and self.span.start <= row["observation_time"] < self.span.end
            and (self.as_of_commit_seq is None or row["market_commit_seq"] <= self.as_of_commit_seq)
            and (self.known_at_lte is None or row["known_at"] <= self.known_at_lte)
            and (not self.source_identity_keys or row["source_identity_key"] in self.source_identity_keys)
        )

    def may_match(self, manifest: FactArchiveManifest) -> bool:
        if self.as_of_commit_seq is not None and manifest.first_cursor[0] > self.as_of_commit_seq:
            return False
        return any(
            bounds.series_id == self.span.series_id
            and bounds.first_observation_at < self.span.end
            and bounds.last_observation_at >= self.span.start
            and (self.known_at_lte is None or bounds.first_known_at <= self.known_at_lte)
            for bounds in manifest.series
        )


class ColdHistoryReader:
    """Verify bounded immutable Parquet pages selected by the serving repository."""

    def __init__(
        self,
        object_store: RawArchiveObjectStore,
        *,
        limits: FactArchiveLimits = FactArchiveLimits(),
        cache=None,
        ensure_contracts: Callable[[Sequence[tuple[str, str]]], None] | None = None,
    ):
        self.object_store = object_store
        self.limits = limits
        self.cache = cache
        self.ensure_contracts = ensure_contracts

    def _validate_contracts(self, manifest):
        execution_checkpoint()
        if self.ensure_contracts is not None:
            self.ensure_contracts(sorted({
                contract for bounds in manifest.series for contract in bounds.payload_contracts
            }))

    def read_page(self, manifest: FactArchiveManifest) -> tuple[dict[str, Any], ...]:
        """Return every revision in original page order after verifying the complete object."""
        self._validate_contracts(manifest)
        return read_canonical_fact_archive(
            self.object_store.local_path(manifest.object_key),
            expected=manifest,
            limits=self.limits,
            cache=self.cache,
        )

    def iter_range(
        self,
        manifests: Iterable[FactArchiveManifest],
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: int | None = None,
        known_at_lte: datetime | None = None,
        source_identity_keys: Sequence[str] = (),
    ) -> Iterator[dict[str, Any]]:
        """Stream candidate revisions for one series, without ranking or dropping invalidations."""
        selection = _Selection.create(series_id, start, end, as_of_commit_seq, known_at_lte, source_identity_keys)
        for manifest in manifests:
            execution_checkpoint()
            if selection.may_match(manifest):
                for row in self.read_page(manifest):
                    execution_checkpoint()
                    if selection.matches(row):
                        consume_execution_resource("input_rows", 1)
                        consume_execution_json("input_bytes", row)
                        yield row

    def read_by_ids(self, locators: Mapping[str, ColdFactLocation]) -> dict[str, dict[str, Any]]:
        """Verify whole pages before resolving IDs for audit or retirement evidence."""
        return self._read_locations(locators, admitted=False)

    def read_admitted_by_ids(self, locators: Mapping[str, ColdFactLocation]) -> dict[str, dict[str, Any]]:
        """Resolve visible SQL locations from pages that passed whole-page admission."""
        return self._read_locations(locators, admitted=True)

    def _read_locations(self, locators, *, admitted):
        pages = {}
        groups = defaultdict(list)
        for fact_id, location in locators.items():
            execution_checkpoint()
            manifest_id = location.manifest.manifest_id
            prior = pages.setdefault(manifest_id, location.manifest)
            if prior != location.manifest:
                raise RuntimeError(f"canonical_cold_manifest_conflict: manifest_id={manifest_id}")
            groups[manifest_id].append((fact_id, location.ordinal))

        result = {}
        for manifest_id, requests in groups.items():
            manifest = pages[manifest_id]
            if admitted:
                self._validate_contracts(manifest)
                rows = read_admitted_canonical_fact_rows(
                    self.object_store.local_path(manifest.object_key), expected=manifest, limits=self.limits,
                    cache=self.cache, ordinals=[ordinal for _, ordinal in requests],
                )
            else:
                rows = self.read_page(manifest)
            for fact_id, ordinal in requests:
                execution_checkpoint()
                row = rows[ordinal]
                if row["id"] != fact_id:
                    raise RuntimeError(
                        "canonical_cold_locator_mismatch: "
                        f"fact_version_id={fact_id} manifest_id={manifest_id} ordinal={ordinal}"
                    )
                result[fact_id] = row
        consume_execution_resource("input_rows", len(result))
        consume_execution_json("input_bytes", result)
        return result


def _visible_at_candle_close(row: Mapping[str, Any]) -> bool:
    try:
        close_time = datetime.fromisoformat(str(row["payload"]["close_time"]).replace("Z", "+00:00"))
        if close_time.utcoffset() is None:
            raise ValueError("close_time must be timezone-aware")
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"canonical_candle_close_invalid: fact_version_id={row['id']}") from exc
    return row["known_at"] <= close_time


def select_fact_rows(
    hot_rows: Iterable[Mapping[str, Any]],
    cold_rows: Iterable[Mapping[str, Any]],
    *,
    series_id: int,
    start: datetime,
    end: datetime,
    as_of_commit_seq: int | None = None,
    known_at_lte: datetime | None = None,
    source_identity_keys: Sequence[str] = (),
    latest_only: bool = True,
    include_invalidated: bool = False,
    causal_at_interval_close: bool = False,
) -> list[dict[str, Any]]:
    """Resolve visibility across tiers using the canonical selection order."""
    selection = _Selection.create(series_id, start, end, as_of_commit_seq, known_at_lte, source_identity_keys)
    versions = {}
    observations = {}
    latest = {}
    eligible = []
    entries = chain((("hot", row) for row in hot_rows), (("cold", row) for row in cold_rows))
    for tier, source in entries:
        execution_checkpoint()
        if not selection.matches(source):
            continue
        row = _logical_row(source)
        fact_id = row["id"]
        if fact_id in versions:
            prior, previous_tier = versions[fact_id]
            if _json_bytes(prior) != _json_bytes(row):
                raise RuntimeError(f"canonical_cold_overlap_mismatch: fact_version_id={fact_id}")
            if previous_tier is None or tier == previous_tier:
                raise RuntimeError(f"canonical_cold_duplicate_fact: fact_version_id={fact_id} tier={tier}")
            versions[fact_id] = (prior, None)
            continue
        key = row["observation_key"]
        revision = row["revision"]
        previous_id = observations.setdefault((key, revision), fact_id)
        if previous_id != fact_id:
            raise RuntimeError(
                f"canonical_cold_revision_conflict: series_id={series_id} observation_key={key} revision={revision}"
            )
        versions[fact_id] = (row, tier)
        if causal_at_interval_close and not _visible_at_candle_close(row):
            continue
        if not latest_only:
            eligible.append(row)
            continue
        previous = latest.get(key)
        if previous is None or revision > previous["revision"]:
            latest[key] = row

    visible = latest.values() if latest_only else eligible
    return sorted(
        (row for row in visible if include_invalidated or row["state"] == "active"),
        key=lambda row: (row["observation_time"], row["observation_key"], row["revision"]),
    )
