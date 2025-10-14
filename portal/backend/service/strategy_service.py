"""In-memory management of strategy blueprints for the portal backend."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
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


class StrategyService:
    """Provides storage and helper routines for strategy definitions."""

    def __init__(self) -> None:
        self._strategies: Dict[str, StrategyRecord] = {}

    def list_strategies(self) -> List[StrategyRecord]:
        """Return every stored strategy in creation order."""

        return sorted(self._strategies.values(), key=lambda s: s.created_at)

    def get_strategy(self, strategy_id: str) -> Optional[StrategyRecord]:
        """Fetch a strategy record by identifier."""

        return self._strategies.get(strategy_id)

    def save_strategy(self, payload: Dict[str, Any]) -> StrategyRecord:
        """Create or update a strategy definition from the supplied payload."""

        strategy_id = payload.get("strategy_id") or self._generate_id()
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
        return record

    def attach_yaml(self, strategy_id: str, yaml_text: str) -> StrategyRecord:
        """Attach additional configuration from a YAML document to the strategy."""

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
        return record

    def generate_order_signals(self, strategy_id: str) -> List[Dict[str, Any]]:
        """Build placeholder order signals derived from the stored selections."""

        record = self._ensure_strategy(strategy_id)
        results: List[Dict[str, Any]] = []
        tags = []
        if isinstance(record.yaml_config, dict):
            tags = list(record.yaml_config.get("tags", []) or [])
        stops = {}
        if isinstance(record.yaml_config, dict):
            stops = record.yaml_config.get("stops", {}) or {}

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

        record = self._ensure_strategy(strategy_id)
        payload = {
            "status": "queued",
            "requested_at": datetime.utcnow().isoformat() + "Z",
            "params": params or {},
        }
        record.last_backtest = payload
        return payload

    def launch_strategy(self, strategy_id: str, mode: str = "simulation") -> Dict[str, Any]:
        """Record a placeholder launch event for the strategy."""

        record = self._ensure_strategy(strategy_id)
        payload = {
            "status": "pending",
            "mode": mode,
            "requested_at": datetime.utcnow().isoformat() + "Z",
        }
        record.launch_status = payload
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


strategy_service = StrategyService()
"""Module-level singleton used by FastAPI routes."""

