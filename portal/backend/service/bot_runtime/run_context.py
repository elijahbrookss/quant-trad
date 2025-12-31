"""Run context for bot backtests (in-memory, per run)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .wallet import WalletLedger


@dataclass
class RunContext:
    """In-memory run context capturing ledger and trace state."""

    bot_id: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    ended_at: Optional[str] = None
    status: str = "running"
    wallet_ledger: WalletLedger = field(default_factory=WalletLedger)
    decision_trace: List[Dict[str, Any]] = field(default_factory=list)


__all__ = ["RunContext"]
