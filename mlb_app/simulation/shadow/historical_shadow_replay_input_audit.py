"""Audit immutable historical shadow replay input coverage."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Dict, Optional, Tuple

from .historical_shadow_replay_discovery import (
    CanonicalHistoricalShadowReplayDiscovery,
    CanonicalHistoricalShadowReplayInputGame,
    discover_historical_shadow_replay_inputs,
)
from .mlb_play_by_play_baserunning_source import (
    CanonicalMlbPlayByPlayBaserunningSnapshot,
)


CANONICAL_HISTORICAL_SHADOW_REPLAY_INPUT_AUDIT_VERSION = (
    "canonical_historical_shadow_replay_input_audit_v1"
)

HISTORICAL_LINEUP_SOURCE = (
    "mlb_stats_boxscore_historical"
)
HISTORICAL_BULLPEN_SOURCE = (
    "mlb_stats_roster_historical"
)
CURRENT_ACTIVE_ROSTER_SOURCE = (
    "mlb_stats_active_roster"
)


def _available_text(value: Optional[str]) -> bool:
    return bool(
        isinstance(value, str)
        and value.strip()
        and value != "unavailable"
    )


def _validate_digest(
    value: Optional[str],
    *,
    field_name: str,
) -> None:
    if value is None:
        return

    if not isinstance(value, str):
        raise TypeError(
            f"{field_name} must be a string or None"
        )

    if len(value) != 64:
        raise ValueError(
            f"{field_name} must be a SHA-256 hex digest"
        )

    try:
        int(value, 16)
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must be a SHA-256 hex digest"
        ) from exc


@dataclass(frozen=True)
class CanonicalHistoricalShadowReplayInputEvidence:
    game_pk: int
    game_date: str
    lineup_source: str = "unavailable"
    lineup_snapshot_digest: Optional[str] = None
    bullpen_source: str = "unavailable"
    bullpen_snapshot_digest: Optional[str] = None
    probability_provider_identity: Optional[str] = None
    exact_artifact_digest: Optional[str] = None
    fallback_catalog_digest: Optional[str] = None
    baserunning_catalog_digest: Optional[str] = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.game_pk, int)
            or isinstance(self.game_pk, bool)
            or self.game_pk <= 0
        ):
            raise ValueError(
                "game_pk must be a positive integer"
            )

        for field_name in (
            "lineup_source",
            "bullpen_source",
        ):
            if not isinstance(
                getattr(self, field_name),
                str,
            ):
                raise TypeError(
                    f"{field_name} must be a string"
                )

        for field_name in (
            "lineup_snapshot_digest",
            "bullpen_snapshot_digest",
            "exact_artifact_digest",
            "fallback_catalog_digest",
            "baserunning_catalog_digest",
        ):
            _validate_digest(
                getattr(self, field_name),
                field_name=field_name,
            )

    @property
    def lineups_ready(self) -> bool:
        return (
            self.lineup_source
            == HISTORICAL_LINEUP_SOURCE
            and _available_text(
                self.lineup_snapshot_digest
            )
        )

    @property
    def bullpens_ready(self) -> bool:
        return (
            self.bullpen_source
            == HISTORICAL_BULLPEN_SOURCE
            and _available_text(
                self.bullpen_snapshot_digest
            )
        )

    @property
    def probability_provider_ready(self) -> bool:
        return _available_text(
            self.probability_provider_identity
        )

    @property
    def exact_artifact_ready(self) -> bool:
        return _available_text(
            self.exact_artifact_digest
        )

    @property
    def fallback_catalog_ready(self) -> bool:
        return _available_text(
            self.fallback_catalog_digest
        )

    @property
    def baserunning_catalog_ready(self) -> bool:
        return _available_text(
            self.baserunning_catalog_digest
        )

    @property
    def current_roster_substitution_rejected(
        self,
    ) -> bool:
        return (
            self.bullpen_source
            == CURRENT_ACTIVE_ROSTER_SOURCE
        )

    def to_discovery_game(
        self,
    ) -> CanonicalHistoricalShadowReplayInputGame:
        return CanonicalHistoricalShadowReplayInputGame(
            game_pk=self.game_pk,
            game_date=self.game_date,
            lineups_ready=self.lineups_ready,
            bullpens_ready=self.bullpens_ready,
            probability_provider_ready=(
                self.probability_provider_ready
            ),
            exact_artifact_ready=(
                self.exact_artifact_ready
            ),
            fallback_catalog_ready=(
                self.fallback_catalog_ready
            ),
            baserunning_catalog_ready=(
                self.baserunning_catalog_ready
            ),
            probability_provider_identity=(
                self.probability_provider_identity
            ),
            exact_artifact_digest=(
                self.exact_artifact_digest
            ),
            fallback_catalog_digest=(
                self.fallback_catalog_digest
            ),
            baserunning_catalog_digest=(
                self.baserunning_catalog_digest
            ),
        )


@dataclass(frozen=True)
class CanonicalHistoricalShadowReplayInputAudit:
    discovery: CanonicalHistoricalShadowReplayDiscovery
    observed_window_digest: str
    evidence_digest: str
    historical_lineup_game_count: int
    historical_bullpen_game_count: int
    rejected_current_roster_game_count: int
    audit_version: str = (
        CANONICAL_HISTORICAL_SHADOW_REPLAY_INPUT_AUDIT_VERSION
    )

    def __post_init__(self) -> None:
        if not isinstance(
            self.discovery,
            CanonicalHistoricalShadowReplayDiscovery,
        ):
            raise TypeError(
                "discovery must be "
                "CanonicalHistoricalShadowReplayDiscovery"
            )

        for field_name in (
            "observed_window_digest",
            "evidence_digest",
        ):
            _validate_digest(
                getattr(self, field_name),
                field_name=field_name,
            )

        if self.audit_version != (
            CANONICAL_HISTORICAL_SHADOW_REPLAY_INPUT_AUDIT_VERSION
        ):
            raise ValueError(
                "unsupported historical shadow replay "
                "input audit version"
            )

    @property
    def ready(self) -> bool:
        return self.discovery.ready

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.audit_version,
            "ready": self.ready,
            "observed_window_digest": (
                self.observed_window_digest
            ),
            "evidence_digest": self.evidence_digest,
            "historical_lineup_game_count": (
                self.historical_lineup_game_count
            ),
            "historical_bullpen_game_count": (
                self.historical_bullpen_game_count
            ),
            "rejected_current_roster_game_count": (
                self.rejected_current_roster_game_count
            ),
            "discovery": self.discovery.to_diagnostics(),
            "current_active_roster_historical_eligible": (
                False
            ),
            "historical_replay_permitted": self.ready,
            "calibration_execution_permitted": False,
            "production_activation": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def audit_historical_shadow_replay_input_coverage(
    *,
    observed: CanonicalMlbPlayByPlayBaserunningSnapshot,
    evidence: Tuple[
        CanonicalHistoricalShadowReplayInputEvidence,
        ...,
    ],
) -> CanonicalHistoricalShadowReplayInputAudit:
    """
    Audit exact replay coverage without fetching or replacing inputs.

    Current active-roster data is never accepted as historical bullpen
    evidence. This function does not execute simulations or calibration.
    """

    if not isinstance(
        observed,
        CanonicalMlbPlayByPlayBaserunningSnapshot,
    ):
        raise TypeError(
            "observed must be "
            "CanonicalMlbPlayByPlayBaserunningSnapshot"
        )

    if not isinstance(evidence, tuple):
        raise TypeError("evidence must be a tuple")

    if not evidence:
        raise ValueError(
            "evidence must contain replay input evidence"
        )

    for value in evidence:
        if not isinstance(
            value,
            CanonicalHistoricalShadowReplayInputEvidence,
        ):
            raise TypeError(
                "evidence must contain "
                "CanonicalHistoricalShadowReplayInputEvidence"
            )

    ordered = tuple(
        sorted(
            evidence,
            key=lambda value: (
                value.game_date,
                value.game_pk,
            ),
        )
    )

    discovery = (
        discover_historical_shadow_replay_inputs(
            observed=observed,
            games=tuple(
                value.to_discovery_game()
                for value in ordered
            ),
        )
    )

    digest_payload = [
        asdict(value)
        for value in ordered
    ]
    evidence_digest = hashlib.sha256(
        json.dumps(
            digest_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    return CanonicalHistoricalShadowReplayInputAudit(
        discovery=discovery,
        observed_window_digest=observed.digest,
        evidence_digest=evidence_digest,
        historical_lineup_game_count=sum(
            value.lineups_ready
            for value in ordered
        ),
        historical_bullpen_game_count=sum(
            value.bullpens_ready
            for value in ordered
        ),
        rejected_current_roster_game_count=sum(
            value.current_roster_substitution_rejected
            for value in ordered
        ),
    )
