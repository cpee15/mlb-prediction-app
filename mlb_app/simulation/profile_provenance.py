"""Diagnostic-only provenance for profiles consumed by PA models."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional


CANONICAL_PROFILE_PROVENANCE_VERSION = (
    "canonical_profile_provenance_v1"
)


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def build_canonical_profile_provenance(
    profile: Optional[Mapping[str, Any]],
    *,
    role: str,
    input_values: Optional[Mapping[str, Any]] = None,
    shared_profile_reused: bool = False,
) -> Dict[str, Any]:
    """Describe a profile without changing any modeled value."""

    profile = dict(profile or {})
    metadata = dict(profile.get("metadata") or {})
    explicit = dict(profile.get("profile_provenance") or {})
    inputs = dict(input_values or {})

    source_type = _first(
        explicit.get("source_type"),
        profile.get("profile_source"),
        profile.get("source"),
        metadata.get("source_type"),
        "unavailable",
    )
    granularity = _first(
        explicit.get("profile_granularity"),
        profile.get("profile_granularity"),
        metadata.get("profile_granularity"),
        "unknown",
    )
    fallback_used = explicit.get("fallback_used")
    if fallback_used is None:
        fallback_context = " ".join(
            str(value).lower()
            for value in (
                source_type,
                explicit.get("lineup_source"),
                profile.get("lineup_source"),
                metadata.get("lineup_source"),
            )
            if value is not None
        )
        fallback_used = (
            "fallback" in fallback_context
            or "prior" in fallback_context
            or source_type == "unavailable"
        )

    return {
        "schema_version": CANONICAL_PROFILE_PROVENANCE_VERSION,
        "role": role,
        "source_type": source_type,
        "profile_granularity": granularity,
        "player_id": _first(
            explicit.get("player_id"),
            profile.get("batter_id"),
            profile.get("pitcher_id"),
            profile.get("player_id"),
            metadata.get("player_id"),
            metadata.get("pitcher_id"),
            metadata.get("batter_id"),
        ),
        "team_id": _first(
            explicit.get("team_id"),
            profile.get("team_id"),
            metadata.get("team_id"),
        ),
        "requested_split": _first(
            explicit.get("requested_split"),
            profile.get("requested_split"),
            profile.get("split"),
        ),
        "selected_split": _first(
            explicit.get("selected_split"),
            profile.get("selected_split"),
        ),
        "sample_window": _first(
            explicit.get("sample_window"),
            profile.get("sample_window"),
            metadata.get("sample_window"),
        ),
        "sample_size": _first(
            explicit.get("sample_size"),
            profile.get("sample_size"),
            metadata.get("sample_size"),
        ),
        "as_of_date": _first(
            explicit.get("as_of_date"),
            profile.get("as_of_date"),
            metadata.get("as_of_date"),
        ),
        "sample_blend_policy": _first(
            explicit.get("sample_blend_policy"),
            profile.get("sample_blend_policy"),
            metadata.get("sample_blend_policy"),
        ),
        "fallback_used": bool(fallback_used),
        "shared_profile_reused": bool(shared_profile_reused),
        "available_input_fields": sorted(
            key for key, value in inputs.items() if value is not None
        ),
        "missing_input_fields": sorted(
            key for key, value in inputs.items() if value is None
        ),
    }
