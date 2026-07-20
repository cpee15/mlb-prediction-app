"""JSON-compatible canonical probability-diagnostics serialization."""

from __future__ import annotations

from typing import Any, Dict

from mlb_app.simulation.game.probability_diagnostics import (
    CANONICAL_PROBABILITY_DIAGNOSTICS_VERSION,
    CanonicalProbabilityResolutionDiagnostics,
)


CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION = (
    "canonical_probability_diagnostics_shadow_v1"
)


def probability_resolution_diagnostics_to_dict(
    diagnostics: CanonicalProbabilityResolutionDiagnostics,
) -> Dict[str, Any]:
    """Serialize an immutable diagnostics snapshot for shadow output."""

    if not isinstance(
        diagnostics,
        CanonicalProbabilityResolutionDiagnostics,
    ):
        raise TypeError(
            "diagnostics must be a "
            "CanonicalProbabilityResolutionDiagnostics"
        )

    if diagnostics.diagnostics_version != (
        CANONICAL_PROBABILITY_DIAGNOSTICS_VERSION
    ):
        raise ValueError(
            "unsupported probability diagnostics version"
        )

    return {
        "schema_version": (
            CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION
        ),
        "diagnostics_version": (
            diagnostics.diagnostics_version
        ),
        "summary": {
            "total_resolutions": (
                diagnostics.total_resolutions
            ),
            "exact_resolutions": (
                diagnostics.exact_resolutions
            ),
            "fallback_resolutions": (
                diagnostics.fallback_resolutions
            ),
            "fallback_rate": (
                diagnostics.fallback_rate
            ),
        },
        "tier_usage": [
            {
                "tier": usage.tier.value,
                "count": usage.count,
            }
            for usage in diagnostics.tier_usage
        ],
        "observations": [
            {
                "trial_index": observation.trial_index,
                "sequence": observation.sequence,
                "inning": observation.inning,
                "half": observation.half,
                "batter_id": observation.batter_id,
                "pitcher_id": observation.pitcher_id,
                "tier": observation.tier.value,
                "source_identity": (
                    observation.source_identity
                ),
                "is_fallback": (
                    observation.is_fallback
                ),
                "exact_artifact_digest": (
                    observation.exact_artifact_digest
                ),
                "fallback_catalog_digest": (
                    observation.fallback_catalog_digest
                ),
                "policy_version": (
                    observation.policy_version
                ),
                "diagnostics_version": (
                    observation.diagnostics_version
                ),
            }
            for observation in diagnostics.observations
        ],
    }
