"""
Canonical venue and park-factor diagnostic contract.

This module is intentionally isolated from production simulation wiring.
It provides deterministic venue resolution, park-factor validation,
season-aware source selection, provenance metadata, and explicit fallbacks.

It does not modify simulation state, parameters, or probabilities.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
import math
import re
from typing import Any, Iterable, Mapping, Sequence


SUPPORTED_ROOF_TYPES = {
    "open_air",
    "fixed_dome",
    "retractable",
    "unknown",
}

SUPPORTED_FACTOR_SCOPES = {
    "overall_runs",
    "home_runs",
    "hits",
    "doubles",
    "triples",
}

PRIMARY_SOURCE_CLASS = "explicit_versioned_primary_source"
SECONDARY_SOURCE_CLASS = "approved_secondary_source"
PRIOR_SEASON_SOURCE_CLASS = "nearest_prior_final_season"
NEUTRAL_SOURCE_CLASS = "neutral_factor"

SOURCE_CLASS_PRIORITY = {
    PRIMARY_SOURCE_CLASS: 1,
    SECONDARY_SOURCE_CLASS: 2,
    PRIOR_SEASON_SOURCE_CLASS: 3,
    NEUTRAL_SOURCE_CLASS: 4,
}


def _normalize_alias(value: str) -> str:
    normalized = re.sub(
        r"[^a-z0-9]+",
        " ",
        value.lower().strip(),
    )

    return " ".join(
        normalized.split()
    )


def _iso_datetime(value: datetime) -> str:
    return value.isoformat()


@dataclass(frozen=True)
class CanonicalVenue:
    canonical_venue_id: str
    provider_venue_id: str | None
    canonical_venue_name: str
    venue_aliases: tuple[str, ...]
    home_team_id: str | None
    timezone: str
    latitude: float | None
    longitude: float | None
    elevation_meters: float | None
    roof_type: str
    active_from: date | None
    active_through: date | None

    def validate(self) -> tuple[str, ...]:
        errors: list[str] = []

        if not self.canonical_venue_id.strip():
            errors.append(
                "canonical_venue_id_nonempty"
            )

        if not self.canonical_venue_name.strip():
            errors.append(
                "canonical_venue_name_nonempty"
            )

        if not self.timezone.strip():
            errors.append(
                "timezone_nonempty"
            )

        if self.roof_type not in SUPPORTED_ROOF_TYPES:
            errors.append(
                "roof_type_supported"
            )

        if (
            self.active_from is not None
            and self.active_through is not None
            and self.active_from > self.active_through
        ):
            errors.append(
                "venue_date_range_valid"
            )

        return tuple(errors)

    def is_active_on(
        self,
        game_date: date,
    ) -> bool:
        if (
            self.active_from is not None
            and game_date < self.active_from
        ):
            return False

        if (
            self.active_through is not None
            and game_date > self.active_through
        ):
            return False

        return True

    def alias_keys(self) -> tuple[str, ...]:
        values = [
            self.canonical_venue_id,
            self.canonical_venue_name,
            *self.venue_aliases,
        ]

        if self.provider_venue_id:
            values.append(
                self.provider_venue_id
            )

        return tuple(
            sorted(
                {
                    _normalize_alias(value)
                    for value in values
                    if value.strip()
                }
            )
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["venue_aliases"] = list(
            self.venue_aliases
        )
        payload["active_from"] = (
            self.active_from.isoformat()
            if self.active_from
            else None
        )
        payload["active_through"] = (
            self.active_through.isoformat()
            if self.active_through
            else None
        )
        return payload


@dataclass(frozen=True)
class ParkFactorRecord:
    canonical_venue_id: str
    season: int
    factor_version: str
    factor_scope: str
    factor_value: float
    neutral_value: float
    sample_games: int | None
    source_name: str
    source_record_id: str | None
    source_published_at: datetime | None
    retrieved_at: datetime
    is_final: bool
    source_class: str

    def validate(
        self,
        game_season: int,
    ) -> tuple[str, ...]:
        errors: list[str] = []

        if not self.canonical_venue_id.strip():
            errors.append(
                "canonical_venue_id_nonempty"
            )

        if (
            not math.isfinite(
                self.factor_value
            )
            or self.factor_value <= 0
        ):
            errors.append(
                "factor_value_finite_and_positive"
            )

        if self.neutral_value != 1.0:
            errors.append(
                "neutral_value_exactly_one"
            )

        if (
            self.factor_scope
            not in SUPPORTED_FACTOR_SCOPES
        ):
            errors.append(
                "factor_scope_supported"
            )

        if self.season > game_season:
            errors.append(
                "season_not_after_game_season"
            )

        if not self.source_name.strip():
            errors.append(
                "source_name_present"
            )

        if not self.factor_version.strip():
            errors.append(
                "factor_version_present"
            )

        if (
            self.source_class
            not in SOURCE_CLASS_PRIORITY
        ):
            errors.append(
                "source_class_supported"
            )

        if (
            self.sample_games is not None
            and self.sample_games < 0
        ):
            errors.append(
                "sample_games_nonnegative"
            )

        return tuple(errors)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_published_at"] = (
            _iso_datetime(
                self.source_published_at
            )
            if self.source_published_at
            else None
        )
        payload["retrieved_at"] = (
            _iso_datetime(
                self.retrieved_at
            )
        )
        return payload


@dataclass(frozen=True)
class VenueResolution:
    query: str
    normalized_query: str
    resolved: bool
    canonical_venue_id: str | None
    canonical_venue_name: str | None
    diagnostic_code: str
    candidate_count: int
    production_authority: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ParkFactorResolution:
    canonical_venue_id: str | None
    game_season: int
    factor_scope: str
    factor_value: float
    neutral_value: float
    source_class: str
    source_name: str
    factor_version: str
    factor_season: int | None
    is_final: bool
    stale: bool
    stale_seasons: int | None
    fallback_used: bool
    diagnostic_code: str
    validation_errors: tuple[str, ...]
    provenance: Mapping[str, Any]
    production_authority: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["validation_errors"] = list(
            self.validation_errors
        )
        payload["provenance"] = dict(
            self.provenance
        )
        return payload


def build_alias_index(
    venues: Iterable[CanonicalVenue],
) -> dict[str, tuple[CanonicalVenue, ...]]:
    index: dict[str, list[CanonicalVenue]] = {}

    for venue in venues:
        if venue.validate():
            continue

        for alias in venue.alias_keys():
            index.setdefault(
                alias,
                [],
            ).append(
                venue
            )

    return {
        alias: tuple(
            sorted(
                matches,
                key=lambda item: (
                    item.canonical_venue_id
                ),
            )
        )
        for alias, matches in index.items()
    }


def resolve_venue(
    query: str,
    game_date: date,
    venues: Sequence[CanonicalVenue],
) -> VenueResolution:
    normalized_query = _normalize_alias(
        query
    )

    index = build_alias_index(
        venues
    )

    candidates = [
        venue
        for venue in index.get(
            normalized_query,
            (),
        )
        if venue.is_active_on(
            game_date
        )
    ]

    if len(candidates) == 1:
        venue = candidates[0]

        return VenueResolution(
            query=query,
            normalized_query=normalized_query,
            resolved=True,
            canonical_venue_id=(
                venue.canonical_venue_id
            ),
            canonical_venue_name=(
                venue.canonical_venue_name
            ),
            diagnostic_code=(
                "venue_resolved"
            ),
            candidate_count=1,
        )

    if len(candidates) > 1:
        return VenueResolution(
            query=query,
            normalized_query=normalized_query,
            resolved=False,
            canonical_venue_id=None,
            canonical_venue_name=None,
            diagnostic_code=(
                "venue_alias_ambiguous"
            ),
            candidate_count=len(
                candidates
            ),
        )

    return VenueResolution(
        query=query,
        normalized_query=normalized_query,
        resolved=False,
        canonical_venue_id=None,
        canonical_venue_name=None,
        diagnostic_code=(
            "venue_unresolved_neutral_fallback"
        ),
        candidate_count=0,
    )


def _neutral_resolution(
    *,
    canonical_venue_id: str | None,
    game_season: int,
    factor_scope: str,
    diagnostic_code: str,
    validation_errors: tuple[str, ...] = (),
) -> ParkFactorResolution:
    return ParkFactorResolution(
        canonical_venue_id=(
            canonical_venue_id
        ),
        game_season=game_season,
        factor_scope=factor_scope,
        factor_value=1.0,
        neutral_value=1.0,
        source_class=NEUTRAL_SOURCE_CLASS,
        source_name="neutral_fallback",
        factor_version="neutral-v1",
        factor_season=None,
        is_final=True,
        stale=False,
        stale_seasons=None,
        fallback_used=True,
        diagnostic_code=diagnostic_code,
        validation_errors=validation_errors,
        provenance={
            "source_name": (
                "neutral_fallback"
            ),
            "source_record_id": None,
            "source_published_at": None,
            "retrieved_at": None,
        },
    )


def resolve_park_factor(
    *,
    venue_resolution: VenueResolution,
    game_season: int,
    factor_scope: str,
    records: Sequence[ParkFactorRecord],
    allow_prior_season_fallback: bool = True,
) -> ParkFactorResolution:
    if not venue_resolution.resolved:
        return _neutral_resolution(
            canonical_venue_id=None,
            game_season=game_season,
            factor_scope=factor_scope,
            diagnostic_code=(
                venue_resolution.diagnostic_code
            ),
        )

    canonical_venue_id = (
        venue_resolution.canonical_venue_id
    )

    candidates = [
        record
        for record in records
        if (
            record.canonical_venue_id
            == canonical_venue_id
            and record.factor_scope
            == factor_scope
            and record.season
            <= game_season
        )
    ]

    valid_candidates = []
    invalid_errors: list[str] = []

    for record in candidates:
        errors = record.validate(
            game_season
        )

        if errors:
            invalid_errors.extend(
                errors
            )
            continue

        valid_candidates.append(
            record
        )

    exact_season = [
        record
        for record in valid_candidates
        if record.season == game_season
    ]

    exact_season.sort(
        key=lambda record: (
            SOURCE_CLASS_PRIORITY[
                record.source_class
            ],
            not record.is_final,
            record.factor_version,
            record.source_name,
        )
    )

    selected: ParkFactorRecord | None = None
    stale = False
    stale_seasons: int | None = None
    fallback_used = False
    diagnostic_code = (
        "park_factor_resolved_exact_season"
    )

    if exact_season:
        selected = exact_season[0]
    elif allow_prior_season_fallback:
        prior_final = [
            record
            for record in valid_candidates
            if (
                record.season < game_season
                and record.is_final
            )
        ]

        prior_final.sort(
            key=lambda record: (
                -record.season,
                SOURCE_CLASS_PRIORITY[
                    record.source_class
                ],
                record.factor_version,
                record.source_name,
            )
        )

        if prior_final:
            selected = prior_final[0]
            stale = True
            stale_seasons = (
                game_season
                - selected.season
            )
            fallback_used = True
            diagnostic_code = (
                "prior_season_factor_fallback"
            )

    if selected is None:
        return _neutral_resolution(
            canonical_venue_id=(
                canonical_venue_id
            ),
            game_season=game_season,
            factor_scope=factor_scope,
            diagnostic_code=(
                "park_factor_invalid_neutral_fallback"
                if invalid_errors
                else "park_factor_missing_neutral_fallback"
            ),
            validation_errors=tuple(
                sorted(
                    set(
                        invalid_errors
                    )
                )
            ),
        )

    return ParkFactorResolution(
        canonical_venue_id=(
            canonical_venue_id
        ),
        game_season=game_season,
        factor_scope=factor_scope,
        factor_value=(
            selected.factor_value
        ),
        neutral_value=(
            selected.neutral_value
        ),
        source_class=(
            selected.source_class
        ),
        source_name=(
            selected.source_name
        ),
        factor_version=(
            selected.factor_version
        ),
        factor_season=(
            selected.season
        ),
        is_final=selected.is_final,
        stale=stale,
        stale_seasons=stale_seasons,
        fallback_used=fallback_used,
        diagnostic_code=diagnostic_code,
        validation_errors=(),
        provenance={
            "source_name": (
                selected.source_name
            ),
            "source_record_id": (
                selected.source_record_id
            ),
            "source_published_at": (
                _iso_datetime(
                    selected.source_published_at
                )
                if selected.source_published_at
                else None
            ),
            "retrieved_at": (
                _iso_datetime(
                    selected.retrieved_at
                )
            ),
        },
    )


def evaluate_venue_park_factor_diagnostic(
    *,
    enabled: bool,
    venue_query: str,
    game_date: date,
    factor_scope: str,
    venues: Sequence[CanonicalVenue],
    records: Sequence[ParkFactorRecord],
    allow_prior_season_fallback: bool = True,
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "diagnostic_code": (
                "venue_park_factor_diagnostic_disabled"
            ),
            "production_authority": False,
            "simulation_inputs_changed": False,
            "canonical_probability_authority_changed": False,
        }

    venue_resolution = resolve_venue(
        venue_query,
        game_date,
        venues,
    )

    factor_resolution = resolve_park_factor(
        venue_resolution=(
            venue_resolution
        ),
        game_season=game_date.year,
        factor_scope=factor_scope,
        records=records,
        allow_prior_season_fallback=(
            allow_prior_season_fallback
        ),
    )

    return {
        "enabled": True,
        "venue_resolution": (
            venue_resolution.to_dict()
        ),
        "park_factor_resolution": (
            factor_resolution.to_dict()
        ),
        "production_authority": False,
        "simulation_inputs_changed": False,
        "canonical_probability_authority_changed": False,
    }
