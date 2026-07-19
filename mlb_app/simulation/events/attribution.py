"""Box-score attribution contracts for canonical play events."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class SacrificeType(str, Enum):
    FLY = "sacrifice_fly"
    BUNT = "sacrifice_bunt"


class ErrorType(str, Enum):
    FIELDING = "fielding"
    THROWING = "throwing"


@dataclass(frozen=True)
class PlayAttribution:
    """Box-score attribution attached to a canonical play."""

    rbi_credited_to: Optional[str] = None
    rbi_count: int = 0
    sacrifice_type: Optional[SacrificeType] = None
    error_fielder_id: Optional[str] = None
    error_type: Optional[ErrorType] = None

    def __post_init__(self) -> None:
        if self.rbi_count < 0:
            raise ValueError("rbi_count cannot be negative")

        if self.rbi_count and not self.rbi_credited_to:
            raise ValueError(
                "rbi_credited_to is required when "
                "rbi_count is positive"
            )

        if (
            self.error_type is not None
            and not self.error_fielder_id
        ):
            raise ValueError(
                "error_fielder_id is required when "
                "error_type is set"
            )

        if (
            self.error_fielder_id is not None
            and self.error_type is None
        ):
            raise ValueError(
                "error_type is required when "
                "error_fielder_id is set"
            )
