from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

import portal.backend.service.storage.repos.market_structure as repository_module


class _MappingResult:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def mappings(self) -> _MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        return dict(self._row)


class _Session:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row
        self.statement = ""
        self.params: dict[str, Any] = {}

    def execute(self, statement: Any, params: dict[str, Any]) -> _MappingResult:
        self.statement = str(statement)
        self.params = dict(params)
        return _MappingResult(self._row)


def _install_fake_db(monkeypatch: pytest.MonkeyPatch, row: dict[str, Any]) -> _Session:
    session = _Session(row)

    @contextmanager
    def session_scope():
        yield session

    monkeypatch.setattr(
        repository_module,
        "db",
        SimpleNamespace(session=session_scope),
    )
    return session


def test_derived_source_resolution_uses_index_endpoint_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _install_fake_db(
        monkeypatch,
        {"min_source_id": 17, "max_source_id": 17},
    )

    assert repository_module.market_structure_repository._resolve_derived_source_id(3) == 17
    normalized_sql = " ".join(session.statement.split()).lower()
    assert "join lateral" in normalized_sql
    assert "min(facts.source_id)" in normalized_sql
    assert "max(facts.source_id)" in normalized_sql
    assert "count(distinct" not in normalized_sql
    assert session.params == {"series_id": 3}


@pytest.mark.parametrize(
    ("row", "expected_count"),
    (
        ({"min_source_id": None, "max_source_id": None}, "0"),
        ({"min_source_id": 17, "max_source_id": 29}, ">1"),
    ),
)
def test_derived_source_resolution_fails_loud_for_invalid_source_scope(
    monkeypatch: pytest.MonkeyPatch,
    row: dict[str, Any],
    expected_count: str,
) -> None:
    _install_fake_db(monkeypatch, row)

    with pytest.raises(
        RuntimeError,
        match=(
            "market_trade_flow_source_invalid: expected exactly one canonical "
            f"upstream trade source series_id=3 source_count={expected_count}"
        ),
    ):
        repository_module.market_structure_repository._resolve_derived_source_id(3)
