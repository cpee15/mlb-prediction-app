"""Immutable hitter-profile overlay for canonical simulation shadow."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import math
from typing import Any, Mapping

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalMatchupInput,
    CanonicalOutcomeProbability,
    CanonicalPlateAppearanceOutcome,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityProviderIdentity,
)


SCHEMA_VERSION = (
    "hitter_profile_simulation_shadow_overlay_v1"
)
PROVIDER_NAME = (
    "hitter_profile_simulation_shadow"
)
PROVIDER_VERSION = "v1"

DELTA_KEYS = {
    CanonicalPlateAppearanceOutcome.OUT: (
        "out",
        "reached_on_error",
    ),
    CanonicalPlateAppearanceOutcome.SINGLE: (
        "single",
    ),
    CanonicalPlateAppearanceOutcome.DOUBLE: (
        "double",
    ),
    CanonicalPlateAppearanceOutcome.TRIPLE: (
        "triple",
    ),
    CanonicalPlateAppearanceOutcome.HOME_RUN: (
        "hr",
    ),
    CanonicalPlateAppearanceOutcome.WALK: (
        "bb",
    ),
    CanonicalPlateAppearanceOutcome.HIT_BY_PITCH: (
        "hbp",
    ),
    CanonicalPlateAppearanceOutcome.STRIKEOUT: (
        "k",
    ),
}


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return (
        result
        if math.isfinite(result)
        else None
    )


def _accepted_gate(
    gate: Mapping[str, Any] | None,
) -> bool:
    payload = dict(gate or {})
    decision = dict(
        payload.get("decision") or {}
    )
    return (
        payload.get("gate_passed") is True
        and payload.get("status")
        == "accepted_for_feature_flag_integration"
        and decision.get(
            "feature_flag_integration_allowed"
        )
        is True
        and decision.get(
            "production_activation_allowed"
        )
        is False
        and payload.get(
            "production_authority_changed"
        )
        is False
    )


def _candidate_deltas(
    value: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    payload = dict(value or {})
    telemetry = dict(
        payload.get("fallback_telemetry") or {}
    )

    if (
        payload.get("status") != "ready"
        or payload.get("executed") is not True
        or payload.get(
            "production_authority_changed"
        )
        is not False
        or payload.get(
            "production_inputs_unchanged"
        )
        is not True
        or telemetry.get("fallback_count") != 0
    ):
        return None

    deltas = payload.get(
        "probability_deltas"
    )
    if not isinstance(deltas, Mapping):
        return None

    required = {
        key
        for keys in DELTA_KEYS.values()
        for key in keys
    }
    if not required.issubset(deltas):
        return None

    if any(
        _number(deltas.get(key)) is None
        for key in required
    ):
        return None

    return deltas


def _overlay_probabilities(
    record: CanonicalProbabilityArtifactRecord,
    deltas: Mapping[str, Any],
) -> tuple[CanonicalOutcomeProbability, ...]:
    adjusted = {}

    for point in record.probabilities:
        delta = sum(
            float(deltas[key])
            for key in DELTA_KEYS[
                point.outcome
            ]
        )
        adjusted[point.outcome] = max(
            0.0,
            point.probability + delta,
        )

    total = sum(adjusted.values())
    if total <= 0.0:
        raise ValueError(
            "candidate overlay has no probability mass"
        )

    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=(
                adjusted[outcome] / total
            ),
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def _artifact_id(
    *,
    matchup_input: CanonicalMatchupInput,
    exact_artifact: CanonicalProbabilityArtifact,
    fallback_catalog: CanonicalProbabilityFallbackCatalog,
    eligible_batters: tuple[str, ...],
) -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "game_pk": matchup_input.game_pk,
        "base_provider": (
            matchup_input
            .probability_provider
            .identity
        ),
        "exact_artifact_digest":
            exact_artifact.digest,
        "fallback_catalog_digest":
            fallback_catalog.digest,
        "eligible_batters":
            list(eligible_batters),
    }
    digest = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return digest


def _unchanged_result(
    *,
    status: str,
    enabled: bool,
    blocker: str | None,
    matchup_input: CanonicalMatchupInput,
    exact_artifact: CanonicalProbabilityArtifact,
    fallback_catalog: CanonicalProbabilityFallbackCatalog,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "enabled": enabled,
        "overlay_applied": False,
        "eligible_batter_count": 0,
        "overlaid_matchup_count": 0,
        "preserved_matchup_count":
            len(exact_artifact.records),
        "blockers": (
            [blocker]
            if blocker is not None
            else []
        ),
        "matchup_input": matchup_input,
        "exact_artifact": exact_artifact,
        "fallback_catalog": fallback_catalog,
        "base_provider_identity": (
            matchup_input
            .probability_provider
            .identity
        ),
        "shadow_provider_identity": None,
        "production_inputs_unchanged": True,
        "production_authority_changed": False,
    }


def build_hitter_profile_simulation_shadow_overlay(
    *,
    enabled: bool = False,
    acceptance_gate: Mapping[
        str,
        Any,
    ] | None = None,
    matchup_input: CanonicalMatchupInput,
    exact_artifact: CanonicalProbabilityArtifact,
    fallback_catalog: CanonicalProbabilityFallbackCatalog,
    candidate_results: Mapping[
        str,
        Mapping[str, Any],
    ] | None = None,
) -> dict[str, Any]:
    """Copy canonical inputs and overlay eligible exact matchup rows."""

    if enabled is not True:
        return _unchanged_result(
            status="disabled",
            enabled=False,
            blocker=None,
            matchup_input=matchup_input,
            exact_artifact=exact_artifact,
            fallback_catalog=fallback_catalog,
        )

    if not _accepted_gate(
        acceptance_gate
    ):
        return _unchanged_result(
            status="blocked",
            enabled=True,
            blocker="canary_acceptance_gate_not_passed",
            matchup_input=matchup_input,
            exact_artifact=exact_artifact,
            fallback_catalog=fallback_catalog,
        )

    base_provider = (
        matchup_input.probability_provider
    )
    if (
        exact_artifact.provider != base_provider
        or fallback_catalog.provider
        != base_provider
    ):
        return _unchanged_result(
            status="fallback",
            enabled=True,
            blocker="canonical_provider_identity_mismatch",
            matchup_input=matchup_input,
            exact_artifact=exact_artifact,
            fallback_catalog=fallback_catalog,
        )

    candidates = {
        str(key): dict(value)
        for key, value in (
            candidate_results or {}
        ).items()
    }
    eligible = {
        batter_id: deltas
        for batter_id, candidate in (
            candidates.items()
        )
        if (
            deltas := _candidate_deltas(
                candidate
            )
        )
        is not None
    }

    if not eligible:
        return _unchanged_result(
            status="fallback",
            enabled=True,
            blocker="no_eligible_hitter_profile_candidates",
            matchup_input=matchup_input,
            exact_artifact=exact_artifact,
            fallback_catalog=fallback_catalog,
        )

    try:
        eligible_batters = tuple(
            sorted(eligible)
        )
        shadow_provider = (
            CanonicalProbabilityProviderIdentity(
                provider_name=PROVIDER_NAME,
                provider_version=PROVIDER_VERSION,
                artifact_id=_artifact_id(
                    matchup_input=matchup_input,
                    exact_artifact=exact_artifact,
                    fallback_catalog=fallback_catalog,
                    eligible_batters=eligible_batters,
                ),
            )
        )

        overlaid_records = []
        overlaid_count = 0

        for record in exact_artifact.records:
            deltas = eligible.get(
                record.batter_id
            )
            if deltas is None:
                probabilities = (
                    record.probabilities
                )
            else:
                probabilities = (
                    _overlay_probabilities(
                        record,
                        deltas,
                    )
                )
                overlaid_count += 1

            overlaid_records.append(
                CanonicalProbabilityArtifactRecord(
                    batter_id=record.batter_id,
                    pitcher_id=record.pitcher_id,
                    probabilities=probabilities,
                )
            )

        if overlaid_count == 0:
            return _unchanged_result(
                status="fallback",
                enabled=True,
                blocker=(
                    "eligible_candidates_absent_"
                    "from_exact_artifact"
                ),
                matchup_input=matchup_input,
                exact_artifact=exact_artifact,
                fallback_catalog=fallback_catalog,
            )

        shadow_matchup = replace(
            matchup_input,
            probability_provider=shadow_provider,
        )
        shadow_artifact = (
            CanonicalProbabilityArtifact(
                provider=shadow_provider,
                records=tuple(
                    overlaid_records
                ),
            )
        )
        shadow_fallback = (
            CanonicalProbabilityFallbackCatalog(
                provider=shadow_provider,
                records=tuple(
                    CanonicalProbabilityFallbackRecord(
                        tier=record.tier,
                        identity=record.identity,
                        probabilities=(
                            record.probabilities
                        ),
                    )
                    for record in (
                        fallback_catalog.records
                    )
                ),
            )
        )

        return {
            "schema_version": SCHEMA_VERSION,
            "status": "ready",
            "enabled": True,
            "overlay_applied": True,
            "eligible_batter_count":
                len(eligible_batters),
            "overlaid_matchup_count":
                overlaid_count,
            "preserved_matchup_count": (
                len(exact_artifact.records)
                - overlaid_count
            ),
            "blockers": [],
            "matchup_input": shadow_matchup,
            "exact_artifact": shadow_artifact,
            "fallback_catalog": shadow_fallback,
            "base_provider_identity":
                base_provider.identity,
            "shadow_provider_identity":
                shadow_provider.identity,
            "base_exact_artifact_digest":
                exact_artifact.digest,
            "shadow_exact_artifact_digest":
                shadow_artifact.digest,
            "base_fallback_catalog_digest":
                fallback_catalog.digest,
            "shadow_fallback_catalog_digest":
                shadow_fallback.digest,
            "production_inputs_unchanged": True,
            "production_authority_changed": False,
        }
    except Exception:
        return _unchanged_result(
            status="fallback",
            enabled=True,
            blocker="hitter_profile_overlay_error",
            matchup_input=matchup_input,
            exact_artifact=exact_artifact,
            fallback_catalog=fallback_catalog,
        )
