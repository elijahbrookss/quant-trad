from __future__ import annotations

"""Repository wrapper for indicator persistence operations."""

from typing import Mapping, Optional, Sequence

from . import storage


class IndicatorRepository:
    """Persist and query indicator metadata and strategy links."""

    def get(self, inst_id: str) -> Optional[Mapping[str, object]]:
        return storage.get_indicator(inst_id)

    def load(self) -> Sequence[Mapping[str, object]]:
        return storage.load_indicators()

    def upsert(self, payload: Mapping[str, object]) -> None:
        storage.upsert_indicator(payload)

    def delete(self, inst_id: str) -> None:
        storage.delete_indicator(inst_id)

    def strategies_for_indicator(self, inst_id: str):
        return storage.strategies_for_indicator(inst_id)

    def upsert_strategy_indicator(
        self, *, strategy_id: str, indicator_id: str
    ) -> None:
        storage.upsert_strategy_indicator(
            strategy_id=strategy_id, indicator_id=indicator_id
        )


def default_repository() -> IndicatorRepository:
    return IndicatorRepository()
