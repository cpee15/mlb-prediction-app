from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Bet105RawBoard:
    filters: Dict[str, Any]
    market_rows: List[Dict[str, Any]] = field(default_factory=list)
    fixture_rows: List[Dict[str, Any]] = field(default_factory=list)
    participant_rows: List[Dict[str, Any]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    ids: Dict[str, List[str]] = field(default_factory=dict)
