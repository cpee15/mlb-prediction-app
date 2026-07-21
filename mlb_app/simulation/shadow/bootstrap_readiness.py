"""Fail-open canonical shadow bootstrap-readiness diagnostics."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Sequence


CANONICAL_SHADOW_BOOTSTRAP_READINESS_VERSION = (
    "canonical_shadow_bootstrap_readiness_v1"
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_present(
    source: Mapping[str, Any],
    keys: Iterable[str],
) -> tuple[Any, Optional[str]]:
    for key in keys:
        value = source.get(key)
        if value not in (None, "", [], {}):
            return value, key

    return None, None


def _normalize_identifier(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None

    if isinstance(value, bool):
        return None

    return str(value)


def _extract_identifier_from_record(
    record: Any,
) -> Optional[str]:
    if isinstance(record, Mapping):
        for key in (
            "player_id",
            "playerId",
            "id",
            "mlb_id",
            "mlbId",
            "batter_id",
            "pitcher_id",
        ):
            identifier = _normalize_identifier(
                record.get(key)
            )

            if identifier is not None:
                return identifier

        return None

    return _normalize_identifier(record)


def _extract_identifiers(
    value: Any,
) -> tuple[str, ...]:
    if isinstance(value, Mapping):
        nested, _ = _first_present(
            value,
            (
                "players",
                "lineup",
                "batting_order",
                "battingOrder",
                "pitchers",
                "bullpen",
                "relievers",
                "ids",
            ),
        )

        if nested is None:
            identifier = _extract_identifier_from_record(
                value
            )
            return (
                (identifier,)
                if identifier is not None
                else ()
            )

        return _extract_identifiers(nested)

    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes),
    ):
        identifier = _extract_identifier_from_record(
            value
        )
        return (
            (identifier,)
            if identifier is not None
            else ()
        )

    identifiers = []

    for record in value:
        identifier = _extract_identifier_from_record(
            record
        )

        if (
            identifier is not None
            and identifier not in identifiers
        ):
            identifiers.append(identifier)

    return tuple(identifiers)


def _lineup_requirement(
    matchup: Mapping[str, Any],
    *,
    side: str,
) -> Dict[str, Any]:
    raw, source = _first_present(
        matchup,
        (
            f"{side}_lineup",
            f"{side}Lineup",
            f"{side}_confirmed_lineup",
            f"{side}_projected_lineup",
            f"{side}_batting_order",
        ),
    )

    identifiers = _extract_identifiers(raw)

    return {
        "ready": len(identifiers) == 9,
        "source": source,
        "player_count": len(identifiers),
        "required_player_count": 9,
    }


def _starter_requirement(
    matchup: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    side: str,
) -> Dict[str, Any]:
    raw, source = _first_present(
        matchup,
        (
            f"{side}_pitcher_id",
            f"{side}_starter_id",
            f"{side}PitcherId",
            f"{side}StarterId",
        ),
    )

    if raw in (None, ""):
        raw, context_source = _first_present(
            context,
            (
                "pitcher_id",
                "starter_id",
            ),
        )
        source = (
            f"{side}_context.{context_source}"
            if context_source
            else None
        )

    identifier = _normalize_identifier(raw)

    return {
        "ready": identifier is not None,
        "source": source,
        "pitcher_id_present": identifier is not None,
    }


def _bullpen_requirement(
    matchup: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    side: str,
) -> Dict[str, Any]:
    raw, source = _first_present(
        matchup,
        (
            f"{side}_bullpen_pitcher_ids",
            f"{side}_bullpen",
            f"{side}_relief_pitchers",
            f"{side}_relievers",
        ),
    )

    if raw in (None, "", [], {}):
        raw, context_source = _first_present(
            context,
            (
                "bullpen_pitcher_ids",
                "bullpen_pitchers",
                "relievers",
            ),
        )
        source = (
            f"{side}_context.{context_source}"
            if context_source
            else None
        )

    identifiers = _extract_identifiers(raw)

    return {
        "ready": len(identifiers) > 0,
        "source": source,
        "pitcher_count": len(identifiers),
        "minimum_pitcher_count": 1,
    }


def _workspace_requirement(
    workspace: Mapping[str, Any],
    *,
    keys: Sequence[str],
) -> Dict[str, Any]:
    raw, source = _first_present(
        workspace,
        keys,
    )

    return {
        "ready": raw not in (None, "", [], {}),
        "source": source,
    }


def build_canonical_shadow_bootstrap_readiness(
    *,
    game_pk: Any,
    matchup: Optional[Mapping[str, Any]],
    away_context: Optional[Mapping[str, Any]],
    home_context: Optional[Mapping[str, Any]],
    workspace: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """
    Report whether production data can assemble canonical execution inputs.

    This function performs discovery diagnostics only. It does not construct
    canonical contracts, run simulations, expose probability rows, or change
    production authority.
    """

    matchup_data = _mapping(matchup)
    away_data = _mapping(away_context)
    home_data = _mapping(home_context)
    workspace_data = _mapping(workspace)

    requirements = {
        "game_identity": {
            "ready": game_pk not in (None, ""),
            "source": "game_pk",
        },
        "away_lineup": _lineup_requirement(
            matchup_data,
            side="away",
        ),
        "home_lineup": _lineup_requirement(
            matchup_data,
            side="home",
        ),
        "away_starter": _starter_requirement(
            matchup_data,
            away_data,
            side="away",
        ),
        "home_starter": _starter_requirement(
            matchup_data,
            home_data,
            side="home",
        ),
        "away_bullpen": _bullpen_requirement(
            matchup_data,
            away_data,
            side="away",
        ),
        "home_bullpen": _bullpen_requirement(
            matchup_data,
            home_data,
            side="home",
        ),
        "probability_provider": _workspace_requirement(
            workspace_data,
            keys=(
                "canonicalProbabilityProvider",
                "canonical_probability_provider",
            ),
        ),
        "exact_probability_artifact": _workspace_requirement(
            workspace_data,
            keys=(
                "canonicalExactProbabilityArtifact",
                "canonical_exact_probability_artifact",
            ),
        ),
        "fallback_probability_catalog": _workspace_requirement(
            workspace_data,
            keys=(
                "canonicalProbabilityFallbackCatalog",
                "canonical_probability_fallback_catalog",
            ),
        ),
    }

    missing_requirements = [
        name
        for name, requirement in requirements.items()
        if requirement["ready"] is not True
    ]

    ready = not missing_requirements

    return {
        "schema_version": (
            CANONICAL_SHADOW_BOOTSTRAP_READINESS_VERSION
        ),
        "status": "ready" if ready else "blocked",
        "ready": ready,
        "game_pk": game_pk,
        "requirements": requirements,
        "missing_requirements": missing_requirements,
        "activation_permitted": False,
        "activation_status": "diagnostic_only",
        "probability_records_exposed": False,
        "authoritative_source": "legacy",
    }
