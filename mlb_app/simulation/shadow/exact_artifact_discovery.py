"""Exact starter-matchup probability artifact discovery."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalOutcomeProbability,
    CanonicalPlateAppearanceOutcome,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.pa_outcome_model import (
    build_pa_outcome_probabilities,
)


CANONICAL_SHADOW_EXACT_ARTIFACT_DISCOVERY_VERSION = (
    "canonical_shadow_exact_artifact_discovery_v1"
)

MIN_EXACT_BATTER_RECORDS_PER_SIDE = 7


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes),
    ):
        return value
    return ()


def _identifier(value: Any) -> Optional[str]:
    if value in (None, "") or isinstance(value, bool):
        return None

    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None

    return str(parsed) if parsed > 0 else None


def _rate(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None

    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(parsed) or parsed < 0.0:
        return None

    if parsed > 1.0:
        parsed /= 100.0

    return parsed


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None

    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None

    return parsed if math.isfinite(parsed) else None


def _real_batter_profile(
    row: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    if not (
        row.get("has_player_split")
        or row.get("has_batter_aggregate")
    ):
        return None

    inputs = _mapping(
        row.get("simulation_inputs")
    )

    profile = {
        "contact_skill": {
            "k_rate": _rate(inputs.get("k_pct")),
            "batting_avg": _rate(
                inputs.get("batting_avg")
            ),
            "contact_rate": None,
        },
        "plate_discipline": {
            "bb_rate": _rate(inputs.get("bb_pct")),
            "on_base_pct": _rate(
                inputs.get("on_base_pct")
            ),
        },
        "power": {
            "iso": _rate(inputs.get("iso")),
            "barrel_rate": _rate(
                inputs.get("barrel_pct")
            ),
            "hard_hit_rate": _rate(
                inputs.get("hard_hit_pct")
            ),
            "slugging_pct": _rate(
                inputs.get("slugging_pct")
            ),
        },
        "contact_quality": {
            "avg_exit_velocity": _number(
                inputs.get("avg_exit_velocity")
            ),
            "avg_launch_angle": _number(
                inputs.get("avg_launch_angle")
            ),
        },
    }

    usable = any(
        value is not None
        for section in profile.values()
        for value in section.values()
    )

    return profile if usable else None


def _canonical_probabilities(
    source: Any,
) -> Optional[Tuple[CanonicalOutcomeProbability, ...]]:
    probabilities = _mapping(source)

    expected = {
        "k",
        "bb",
        "hbp",
        "single",
        "double",
        "triple",
        "hr",
        "reached_on_error",
        "out",
    }

    if set(probabilities.keys()) != expected:
        return None

    parsed: Dict[str, float] = {}

    for key in expected:
        value = _number(probabilities.get(key))

        if (
            value is None
            or value < 0.0
            or value > 1.0
        ):
            return None

        parsed[key] = value

    if abs(sum(parsed.values()) - 1.0) > 0.001:
        return None

    canonical = {
        CanonicalPlateAppearanceOutcome.OUT: (
            parsed["out"]
            + parsed["reached_on_error"]
        ),
        CanonicalPlateAppearanceOutcome.SINGLE: (
            parsed["single"]
        ),
        CanonicalPlateAppearanceOutcome.DOUBLE: (
            parsed["double"]
        ),
        CanonicalPlateAppearanceOutcome.TRIPLE: (
            parsed["triple"]
        ),
        CanonicalPlateAppearanceOutcome.HOME_RUN: (
            parsed["hr"]
        ),
        CanonicalPlateAppearanceOutcome.WALK: (
            parsed["bb"]
        ),
        CanonicalPlateAppearanceOutcome.HIT_BY_PITCH: (
            parsed["hbp"]
        ),
        CanonicalPlateAppearanceOutcome.STRIKEOUT: (
            parsed["k"]
        ),
    }

    total = sum(canonical.values())

    if total <= 0.0:
        return None

    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=(
                canonical[outcome] / total
            ),
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def _side_records(
    *,
    offense_context: Mapping[str, Any],
    opposing_starter_id: Any,
    opposing_pitcher_profile: Mapping[str, Any],
    environment_profile: Mapping[str, Any],
) -> Tuple[
    Tuple[CanonicalProbabilityArtifactRecord, ...],
    int,
    int,
]:
    starter_id = _identifier(
        opposing_starter_id
    )

    if starter_id is None:
        return (), 0, 0

    offense_inputs = _mapping(
        offense_context.get("offense_inputs")
    )
    lineup = _sequence(
        offense_inputs.get("lineup")
    )

    records = []
    real_profile_count = 0
    invalid_record_count = 0

    for raw_row in lineup:
        row = _mapping(raw_row)
        batter_id = _identifier(
            row.get("batter_id")
        )
        batter_profile = _real_batter_profile(
            row
        )

        if (
            batter_id is None
            or batter_profile is None
        ):
            continue

        real_profile_count += 1

        model = build_pa_outcome_probabilities(
            batter_profile=batter_profile,
            pitcher_profile=dict(
                opposing_pitcher_profile
            ),
            environment_profile=dict(
                environment_profile
            ),
        )

        probabilities = _canonical_probabilities(
            model.get("probabilities")
        )

        if probabilities is None:
            invalid_record_count += 1
            continue

        records.append(
            CanonicalProbabilityArtifactRecord(
                batter_id=batter_id,
                pitcher_id=starter_id,
                probabilities=probabilities,
            )
        )

    return (
        tuple(records),
        real_profile_count,
        invalid_record_count,
    )


@dataclass(frozen=True)
class CanonicalShadowExactArtifactDiscovery:
    artifact: Optional[
        CanonicalProbabilityArtifact
    ] = None
    away_record_count: int = 0
    home_record_count: int = 0
    away_real_profile_count: int = 0
    home_real_profile_count: int = 0
    invalid_record_count: int = 0
    status: str = "unavailable"
    blocked_reasons: Tuple[str, ...] = ()
    discovery_version: str = (
        CANONICAL_SHADOW_EXACT_ARTIFACT_DISCOVERY_VERSION
    )

    def __post_init__(self) -> None:
        if self.discovery_version != (
            CANONICAL_SHADOW_EXACT_ARTIFACT_DISCOVERY_VERSION
        ):
            raise ValueError(
                "unsupported canonical exact-artifact "
                "discovery version"
            )

        if (
            self.artifact is not None
            and not isinstance(
                self.artifact,
                CanonicalProbabilityArtifact,
            )
        ):
            raise TypeError(
                "artifact must be a "
                "CanonicalProbabilityArtifact or None"
            )

    @property
    def ready(self) -> bool:
        return self.artifact is not None

    def readiness_workspace_fields(
        self,
    ) -> Dict[str, Any]:
        if self.artifact is None:
            return {}

        return {
            "canonicalExactProbabilityArtifact": {
                "artifact_version": (
                    self.artifact.artifact_version
                ),
                "provider_identity": (
                    self.artifact.provider.identity
                ),
                "digest": self.artifact.digest,
                "record_count": len(
                    self.artifact.records
                ),
                "away_record_count": (
                    self.away_record_count
                ),
                "home_record_count": (
                    self.home_record_count
                ),
                "coverage": (
                    "confirmed_batters_vs_probable_starters"
                ),
            }
        }

    def to_diagnostics(self) -> Dict[str, Any]:
        artifact = self.artifact

        return {
            "schema_version": self.discovery_version,
            "status": self.status,
            "ready": self.ready,
            "source": (
                "confirmed_player_profiles_vs_probable_starters"
            ),
            "minimum_records_per_side": (
                MIN_EXACT_BATTER_RECORDS_PER_SIDE
            ),
            "away_record_count": (
                self.away_record_count
            ),
            "home_record_count": (
                self.home_record_count
            ),
            "away_real_profile_count": (
                self.away_real_profile_count
            ),
            "home_real_profile_count": (
                self.home_real_profile_count
            ),
            "invalid_record_count": (
                self.invalid_record_count
            ),
            "blocked_reasons": list(
                self.blocked_reasons
            ),
            "artifact": (
                {
                    "artifact_version": (
                        artifact.artifact_version
                    ),
                    "provider_identity": (
                        artifact.provider.identity
                    ),
                    "digest": artifact.digest,
                    "record_count": len(
                        artifact.records
                    ),
                    "coverage": (
                        "confirmed_batters_vs_probable_starters"
                    ),
                }
                if artifact is not None
                else None
            ),
            "bullpen_exact_rows_included": False,
            "team_fallback_profiles_included": False,
            "reached_on_error_mapping": (
                "folded_into_canonical_out"
            ),
            "probability_records_exposed": False,
            "activation_permitted": False,
            "authoritative_source": "legacy",
        }


def discover_canonical_shadow_exact_artifact(
    *,
    away_context: Optional[Mapping[str, Any]],
    home_context: Optional[Mapping[str, Any]],
    workspace: Optional[Mapping[str, Any]],
    provider: Optional[
        CanonicalProbabilityProviderIdentity
    ],
) -> CanonicalShadowExactArtifactDiscovery:
    """
    Build exact confirmed-batter versus probable-starter probability rows.

    Only real batter split/aggregate profiles are eligible. Team fallback
    profiles and team-average PA distributions cannot become exact records.
    """

    if provider is None:
        return CanonicalShadowExactArtifactDiscovery(
            status="blocked",
            blocked_reasons=("missing_provider",),
        )

    if not isinstance(
        provider,
        CanonicalProbabilityProviderIdentity,
    ):
        raise TypeError(
            "provider must be a "
            "CanonicalProbabilityProviderIdentity or None"
        )

    away_data = _mapping(away_context)
    home_data = _mapping(home_context)
    workspace_data = _mapping(workspace)

    away_records, away_real, away_invalid = (
        _side_records(
            offense_context=away_data,
            opposing_starter_id=(
                home_data.get("pitcher_id")
            ),
            opposing_pitcher_profile=_mapping(
                workspace_data.get(
                    "homePitcherProfile"
                )
            ),
            environment_profile=_mapping(
                workspace_data.get(
                    "environmentProfile"
                )
            ),
        )
    )

    home_records, home_real, home_invalid = (
        _side_records(
            offense_context=home_data,
            opposing_starter_id=(
                away_data.get("pitcher_id")
            ),
            opposing_pitcher_profile=_mapping(
                workspace_data.get(
                    "awayPitcherProfile"
                )
            ),
            environment_profile=_mapping(
                workspace_data.get(
                    "environmentProfile"
                )
            ),
        )
    )

    reasons = []

    if (
        len(away_records)
        < MIN_EXACT_BATTER_RECORDS_PER_SIDE
    ):
        reasons.append(
            "insufficient_away_exact_records"
        )

    if (
        len(home_records)
        < MIN_EXACT_BATTER_RECORDS_PER_SIDE
    ):
        reasons.append(
            "insufficient_home_exact_records"
        )

    if reasons:
        return CanonicalShadowExactArtifactDiscovery(
            away_record_count=len(away_records),
            home_record_count=len(home_records),
            away_real_profile_count=away_real,
            home_real_profile_count=home_real,
            invalid_record_count=(
                away_invalid + home_invalid
            ),
            status=(
                "partial"
                if away_records or home_records
                else "unavailable"
            ),
            blocked_reasons=tuple(reasons),
        )

    artifact = CanonicalProbabilityArtifact(
        provider=provider,
        records=(
            *away_records,
            *home_records,
        ),
    )

    return CanonicalShadowExactArtifactDiscovery(
        artifact=artifact,
        away_record_count=len(away_records),
        home_record_count=len(home_records),
        away_real_profile_count=away_real,
        home_real_profile_count=home_real,
        invalid_record_count=(
            away_invalid + home_invalid
        ),
        status="ready",
    )
