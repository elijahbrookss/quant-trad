"""Lightweight persistence for strategy blueprints and derived assets."""

from __future__ import annotations

import json
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

try:  # pragma: no cover - import guard for optional dependency
    import yaml
except ModuleNotFoundError:  # pragma: no cover - fallback when PyYAML is missing
    yaml = None  # type: ignore[assignment]


@dataclass
class StrategyRecord:
    """Represents a user-defined strategy blueprint."""

    strategy_id: str
    name: str
    symbol: str
    timeframe: str
    description: Optional[str] = None
    indicators: List[Dict[str, Any]] = field(default_factory=list)
    selected_signals: Dict[str, List[str]] = field(default_factory=dict)
    yaml_config: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    last_backtest: Optional[Dict[str, Any]] = None
    launch_status: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the strategy record for JSON persistence."""

        return {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "description": self.description,
            "indicators": list(self.indicators),
            "selected_signals": {key: list(value) for key, value in self.selected_signals.items()},
            "yaml_config": self.yaml_config,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_backtest": self.last_backtest,
            "launch_status": self.launch_status,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "StrategyRecord":
        """Rehydrate a record from persisted JSON payload."""

        return cls(
            strategy_id=payload["strategy_id"],
            name=payload.get("name", ""),
            symbol=payload.get("symbol", ""),
            timeframe=payload.get("timeframe", ""),
            description=payload.get("description"),
            indicators=list(payload.get("indicators", [])),
            selected_signals={
                key: list(value)
                if isinstance(value, Iterable) and not isinstance(value, (str, bytes))
                else []
                for key, value in (payload.get("selected_signals") or {}).items()
            },
            yaml_config=payload.get("yaml_config"),
            created_at=_coerce_datetime(payload.get("created_at")),
            updated_at=_coerce_datetime(payload.get("updated_at")),
            last_backtest=payload.get("last_backtest"),
            launch_status=payload.get("launch_status"),
        )


class StrategyService:
    """Manages strategy blueprints with simple file-backed persistence."""

    def __init__(self, storage_path: Optional[Path] = None) -> None:
        self._storage_path = (storage_path or _default_storage_path()).expanduser()
        self._strategies: Dict[str, StrategyRecord] = {}
        self._lock = Lock()
        self._load()

    def list_strategies(self) -> List[StrategyRecord]:
        """Return every stored strategy in creation order."""

        with self._lock:
            return sorted(self._strategies.values(), key=lambda s: s.created_at)

    def get_strategy(self, strategy_id: str) -> Optional[StrategyRecord]:
        """Fetch a strategy record by identifier."""

        with self._lock:
            return self._strategies.get(strategy_id)

    def save_strategy(self, payload: Dict[str, Any]) -> StrategyRecord:
        """Create or update a strategy definition from the supplied payload."""

        strategy_id = payload.get("strategy_id") or self._generate_id()
        with self._lock:
            record = self._strategies.get(strategy_id)

            if record is None:
                record = StrategyRecord(
                    strategy_id=strategy_id,
                    name=payload["name"],
                    symbol=payload.get("symbol", ""),
                    timeframe=payload.get("timeframe", ""),
                    description=payload.get("description"),
                )
                self._strategies[strategy_id] = record

            record.name = payload.get("name", record.name)
            record.symbol = payload.get("symbol", record.symbol)
            record.timeframe = payload.get("timeframe", record.timeframe)
            record.description = payload.get("description", record.description)
            record.indicators = list(payload.get("indicators", record.indicators))
            record.selected_signals = {
                key: list(value)
                for key, value in (payload.get("selected_signals") or {}).items()
            }
            record.updated_at = datetime.utcnow()
            self._persist()
            return record

    def attach_yaml(self, strategy_id: str, yaml_text: str) -> StrategyRecord:
        """Attach additional configuration from a YAML document to the strategy."""

        with self._lock:
            record = self._ensure_strategy(strategy_id)
            if yaml is None:
                raise ValueError("PyYAML is required to upload strategy metadata")

            parsed = yaml.safe_load(yaml_text) if yaml_text.strip() else {}
            if parsed is None:
                parsed = {}
            if not isinstance(parsed, dict):
                raise ValueError("strategy YAML must define a mapping at the top level")

            record.yaml_config = parsed
            record.updated_at = datetime.utcnow()
            self._persist()
            return record

    def generate_order_signals(self, strategy_id: str) -> List[Dict[str, Any]]:
        """Build placeholder order signals derived from the stored selections."""

        with self._lock:
            record = self._ensure_strategy(strategy_id)
            results: List[Dict[str, Any]] = []
            tags: List[str] = []
            if isinstance(record.yaml_config, dict):
                raw_tags = record.yaml_config.get("tags", []) or []
                tags = [tag for tag in raw_tags if isinstance(tag, str)]
            stops: Dict[str, Any] = {}
            if isinstance(record.yaml_config, dict):
                raw_stops = record.yaml_config.get("stops", {}) or {}
                if isinstance(raw_stops, dict):
                    stops = raw_stops

            for idx, indicator in enumerate(record.indicators):
                indicator_id = indicator.get("id") or f"indicator-{idx}"
                indicator_name = indicator.get("name") or indicator_id
                signals = record.selected_signals.get(indicator_id, [])
                for sig_idx, signal_name in enumerate(signals):
                    results.append(
                        {
                            "id": f"{record.strategy_id}-{indicator_id}-{sig_idx}",
                            "indicator_id": indicator_id,
                            "indicator_name": indicator_name,
                            "signal": signal_name,
                            "action": self._infer_action(signal_name),
                            "tags": tags,
                            "stops": stops,
                        }
                    )

            return results

    def request_backtest(self, strategy_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Record a placeholder backtest request and return its status."""

        with self._lock:
            record = self._ensure_strategy(strategy_id)
            payload = {
                "status": "queued",
                "requested_at": datetime.utcnow().isoformat() + "Z",
                "params": params or {},
            }
            record.last_backtest = payload
            self._persist()
            return payload

    def launch_strategy(self, strategy_id: str, mode: str = "simulation") -> Dict[str, Any]:
        """Record a placeholder launch event for the strategy."""

        with self._lock:
            record = self._ensure_strategy(strategy_id)
            payload = {
                "status": "pending",
                "mode": mode,
                "requested_at": datetime.utcnow().isoformat() + "Z",
            }
            record.launch_status = payload
            self._persist()
            return payload

    def _ensure_strategy(self, strategy_id: str) -> StrategyRecord:
        """Return the strategy record or raise if it does not exist."""

        record = self._strategies.get(strategy_id)
        if record is None:
            raise KeyError(f"strategy '{strategy_id}' not found")
        return record

    def _infer_action(self, signal_name: str) -> str:
        """Infer a coarse action label from the signal name."""

        lowered = (signal_name or "").lower()
        if any(token in lowered for token in ("long", "buy", "bull")):
            return "enter_long"
        if any(token in lowered for token in ("short", "sell", "bear")):
            return "enter_short"
        if "exit" in lowered or "close" in lowered:
            return "exit"
        return "monitor"

    def _generate_id(self) -> str:
        """Generate a unique identifier for a new strategy."""

        return f"strategy-{uuid4().hex[:10]}"

    def _load(self) -> None:
        """Initialise in-memory store from the backing file if it exists."""

        if not self._storage_path:
            return

        with self._lock:
            if not self._storage_path.exists():
                return

            with suppress(OSError, json.JSONDecodeError):
                with self._storage_path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                    strategies = payload.get("strategies", []) if isinstance(payload, dict) else payload
                    self._strategies = {
                        item["strategy_id"]: StrategyRecord.from_dict(item)
                        for item in strategies
                        if isinstance(item, dict) and item.get("strategy_id")
                    }

    def _persist(self) -> None:
        """Write the in-memory strategies to the persistence file."""

        if not self._storage_path:
            return

        self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        with self._storage_path.open("w", encoding="utf-8") as handle:
            json.dump(
                {"strategies": [record.to_dict() for record in self._strategies.values()]},
                handle,
                indent=2,
                sort_keys=True,
            )


def _default_storage_path() -> Path:
    """Return the default filesystem location for strategy persistence."""

    return Path("~/.quant-trad/strategies.json")


def _coerce_datetime(value: Optional[str]) -> datetime:
    """Convert ISO8601 strings to ``datetime`` values for records."""

    if not value:
        return datetime.utcnow()
    with suppress(ValueError):
        return datetime.fromisoformat(value)
    return datetime.utcnow()


strategy_service = StrategyService()
"""Module-level singleton used by FastAPI routes."""

