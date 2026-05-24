"""Run context for bot backtests (in-memory, per run)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from engines.bot_runtime.core.wallet import WalletLedger
from engines.bot_runtime.core.wallet_gateway import WalletGateway
from engines.bot_runtime.core.runtime_events import RuntimeEvent


@dataclass
class RunContext:
    """In-memory run context capturing canonical runtime events and wallet gateway state."""

    bot_id: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    ended_at: Optional[str] = None
    status: str = "running"
    wallet_ledger: Optional[WalletLedger] = None
    wallet_gateway: Optional[WalletGateway] = None
    runtime_event_seq: int = 0
    runtime_events: List[RuntimeEvent] = field(default_factory=list)
    runtime_event_stream: List[Dict[str, Any]] = field(default_factory=list)
    decision_artifacts: List[Dict[str, Any]] = field(default_factory=list)
    rejection_artifacts: List[Dict[str, Any]] = field(default_factory=list)


__all__ = ["RunContext"]
