"""Report historical lineup and bullpen snapshot coverage."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Dict

from .historical_lineup_bullpen_source import (
    CanonicalHistoricalLineupBullpenWindow,
)


CANONICAL_HISTORICAL_LINEUP_BULLPEN_COVERAGE_REPORT_VERSION = (
    "canonical_historical_lineup_bullpen_coverage_report_v1"
)


def _sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class CanonicalHistoricalLineupBullpenCoverageReport:
    observed_window_digest: str
    source_window_digest: str
    game_count: int
    lineup_ready_game_count: int
    bullpen_ready_game_count: int
    ready_game_count: int
    partial_game_count: int
    unavailable_game_count: int
    missing_away_lineup_count: int
    missing_home_lineup_count: int
    missing_away_bullpen_count: int
    missing_home_bullpen_count: int
    report_digest: str
    report_version: str = (
        CANONICAL_HISTORICAL_LINEUP_BULLPEN_COVERAGE_REPORT_VERSION
    )

    def __post_init__(self) -> None:
        if self.game_count <= 0:
            raise ValueError(
                "game_count must be positive"
            )

        counts = (
            self.lineup_ready_game_count,
            self.bullpen_ready_game_count,
            self.ready_game_count,
            self.partial_game_count,
            self.unavailable_game_count,
            self.missing_away_lineup_count,
            self.missing_home_lineup_count,
            self.missing_away_bullpen_count,
            self.missing_home_bullpen_count,
        )
        if any(
            value < 0 or value > self.game_count
            for value in counts
        ):
            raise ValueError(
                "coverage counts must be within game_count"
            )

        if (
            self.ready_game_count
            + self.partial_game_count
            + self.unavailable_game_count
            != self.game_count
        ):
            raise ValueError(
                "status counts must equal game_count"
            )

        for field_name in (
            "observed_window_digest",
            "source_window_digest",
            "report_digest",
        ):
            value = getattr(self, field_name)
            if len(value) != 64:
                raise ValueError(
                    f"{field_name} must be a SHA-256 digest"
                )

        if self.report_version != (
            CANONICAL_HISTORICAL_LINEUP_BULLPEN_COVERAGE_REPORT_VERSION
        ):
            raise ValueError(
                "unsupported historical lineup-bullpen "
                "coverage report version"
            )

    @property
    def complete(self) -> bool:
        return self.ready_game_count == self.game_count

    @property
    def lineup_coverage_rate(self) -> float:
        return (
            self.lineup_ready_game_count
            / self.game_count
        )

    @property
    def bullpen_coverage_rate(self) -> float:
        return (
            self.bullpen_ready_game_count
            / self.game_count
        )

    @property
    def complete_game_coverage_rate(self) -> float:
        return (
            self.ready_game_count
            / self.game_count
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.report_version,
            "complete": self.complete,
            "game_count": self.game_count,
            "lineup_ready_game_count": (
                self.lineup_ready_game_count
            ),
            "bullpen_ready_game_count": (
                self.bullpen_ready_game_count
            ),
            "ready_game_count": self.ready_game_count,
            "partial_game_count": (
                self.partial_game_count
            ),
            "unavailable_game_count": (
                self.unavailable_game_count
            ),
            "missing_away_lineup_count": (
                self.missing_away_lineup_count
            ),
            "missing_home_lineup_count": (
                self.missing_home_lineup_count
            ),
            "missing_away_bullpen_count": (
                self.missing_away_bullpen_count
            ),
            "missing_home_bullpen_count": (
                self.missing_home_bullpen_count
            ),
            "lineup_coverage_rate": round(
                self.lineup_coverage_rate,
                6,
            ),
            "bullpen_coverage_rate": round(
                self.bullpen_coverage_rate,
                6,
            ),
            "complete_game_coverage_rate": round(
                self.complete_game_coverage_rate,
                6,
            ),
            "observed_window_digest": (
                self.observed_window_digest
            ),
            "source_window_digest": (
                self.source_window_digest
            ),
            "report_digest": self.report_digest,
            "player_identifiers_exposed": False,
            "current_active_roster_used": False,
            "used_pitchers_substituted_for_bullpen": False,
            "historical_replay_permitted": self.complete,
            "historical_replay_executed": False,
            "calibration_execution_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def report_historical_lineup_bullpen_coverage(
    window: CanonicalHistoricalLineupBullpenWindow,
) -> CanonicalHistoricalLineupBullpenCoverageReport:
    """Summarize exact historical input coverage without replay."""

    if not isinstance(
        window,
        CanonicalHistoricalLineupBullpenWindow,
    ):
        raise TypeError(
            "window must be "
            "CanonicalHistoricalLineupBullpenWindow"
        )

    games = window.games
    counts = {
        "game_count": len(games),
        "lineup_ready_game_count": sum(
            value.lineups_ready
            for value in games
        ),
        "bullpen_ready_game_count": sum(
            value.bullpens_ready
            for value in games
        ),
        "ready_game_count": sum(
            value.status == "ready"
            for value in games
        ),
        "partial_game_count": sum(
            value.status == "partial"
            for value in games
        ),
        "unavailable_game_count": sum(
            value.status == "unavailable"
            for value in games
        ),
        "missing_away_lineup_count": sum(
            len(value.away_lineup_ids) != 9
            for value in games
        ),
        "missing_home_lineup_count": sum(
            len(value.home_lineup_ids) != 9
            for value in games
        ),
        "missing_away_bullpen_count": sum(
            not value.away_bullpen_ids
            for value in games
        ),
        "missing_home_bullpen_count": sum(
            not value.home_bullpen_ids
            for value in games
        ),
    }

    report_digest = _sha256(
        {
            "observed_window_digest": (
                window.observed_window_digest
            ),
            "source_window_digest": window.digest,
            **counts,
        }
    )

    return CanonicalHistoricalLineupBullpenCoverageReport(
        observed_window_digest=(
            window.observed_window_digest
        ),
        source_window_digest=window.digest,
        report_digest=report_digest,
        **counts,
    )
