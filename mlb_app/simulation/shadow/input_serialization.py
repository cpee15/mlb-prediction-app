"""JSON-compatible canonical shadow input provenance serialization."""

from __future__ import annotations

from typing import Any, Dict

from .input_assembly import (
    CanonicalShadowExecutionInputs,
)


CANONICAL_SHADOW_INPUT_PROVENANCE_VERSION = (
    "canonical_shadow_input_provenance_v1"
)


def canonical_shadow_input_provenance_to_dict(
    inputs: CanonicalShadowExecutionInputs,
) -> Dict[str, Any]:
    """
    Serialize canonical shadow input provenance without probability rows.

    This payload is diagnostic only. It does not authorize canonical output,
    change execution inputs, or expose individual probability distributions.
    """

    if not isinstance(
        inputs,
        CanonicalShadowExecutionInputs,
    ):
        raise TypeError(
            "inputs must be CanonicalShadowExecutionInputs"
        )

    matchup = inputs.matchup_input
    provider = matchup.probability_provider
    away_plan = matchup.away_pitching_plan
    home_plan = matchup.home_pitching_plan

    return {
        "schema_version": (
            CANONICAL_SHADOW_INPUT_PROVENANCE_VERSION
        ),
        "assembly_version": inputs.assembly_version,
        "assembly_digest": inputs.assembly_digest,
        "matchup": {
            "schema_version": matchup.schema_version,
            "game_pk": matchup.game_pk,
            "away_lineup_player_ids": list(
                matchup.away_lineup.player_ids
            ),
            "home_lineup_player_ids": list(
                matchup.home_lineup.player_ids
            ),
            "away_pitching_plan": {
                "starter_id": away_plan.starter_id,
                "bullpen_pitcher_ids": list(
                    away_plan.bullpen_pitcher_ids
                ),
            },
            "home_pitching_plan": {
                "starter_id": home_plan.starter_id,
                "bullpen_pitcher_ids": list(
                    home_plan.bullpen_pitcher_ids
                ),
            },
        },
        "probability_provider": {
            "identity": provider.identity,
            "provider_name": provider.provider_name,
            "provider_version": provider.provider_version,
            "artifact_id": provider.artifact_id,
        },
        "artifacts": {
            "exact": {
                "artifact_version": (
                    inputs.exact_artifact.artifact_version
                ),
                "digest": inputs.exact_artifact_digest,
                "record_count": len(
                    inputs.exact_artifact.records
                ),
            },
            "fallback_catalog": {
                "schema_version": (
                    inputs.fallback_catalog.schema_version
                ),
                "digest": (
                    inputs.fallback_catalog_digest
                ),
                "record_count": len(
                    inputs.fallback_catalog.records
                ),
            },
        },
        "fallback_policy": {
            "policy_version": (
                inputs.fallback_policy.policy_version
            ),
            "tiers": [
                tier.value
                for tier in inputs.fallback_policy.tiers
            ],
        },
        "game_config": {
            "regulation_innings": (
                inputs.game_config.regulation_innings
            ),
            "max_extra_innings": (
                inputs.game_config.max_extra_innings
            ),
            "automatic_runner_enabled": (
                inputs.game_config
                .automatic_runner_enabled
            ),
            "max_plate_appearances_per_half": (
                inputs.game_config
                .max_plate_appearances_per_half
            ),
        },
        "dfs_rules": {
            "batter_rules_supplied": (
                inputs.batter_dfs_rules is not None
            ),
            "pitcher_rules_supplied": (
                inputs.pitcher_dfs_rules is not None
            ),
        },
        "probability_records_exposed": False,
        "authoritative_source": "legacy",
    }
