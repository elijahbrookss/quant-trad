from dataclasses import dataclass
from typing import Tuple
from typing import Optional


@dataclass
class DataContext:
    symbol: Optional[str]
    start: Optional[str]
    end: Optional[str]
    interval: Optional[str]

    def __post_init__(self):
        self.validate()

    def validate(self):
        if not self.symbol:
            raise ValueError("DataContext validation failed: 'symbol' is required.")
        if not self.start:
            raise ValueError("DataContext validation failed: 'start' date is required.")
        if not self.end:
            raise ValueError("DataContext validation failed: 'end' date is required.")
        if not self.interval:
            raise ValueError("DataContext validation failed: 'interval' is required.")