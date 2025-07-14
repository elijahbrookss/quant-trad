# signals/base.py

from dataclasses import dataclass, field
from typing import Dict, Any
from datetime import datetime

@dataclass
class BaseSignal:
    type: str                 # e.g. "breakout", "retest", etc.
    symbol: str               # e.g. "CL", "XAUUSD"
    time: datetime            # timestamp of the signal
    confidence: float         # 0.0 to 1.0
    metadata: Dict[str, Any]  # flexible structure for indicator-specific fields

    def to_dict(self):
        return {
            "type": self.type,
            "symbol": self.symbol,
            "time": self.time.isoformat(),
            "confidence": self.confidence,
            "metadata": self.metadata
        }
