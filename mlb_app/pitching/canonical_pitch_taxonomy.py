"""
Canonical pitch taxonomy and source-normalization contract.

This module provides deterministic, diagnostic-only normalization of source
pitch classifications into a bounded canonical taxonomy.

It does not:
- select production pitches;
- alter pitch sequencing;
- modify matchup or plate-appearance probabilities;
- modify contact quality or batted-ball outcomes;
- replace canonical simulation probabilities;
- grant production authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import re
from types import MappingProxyType
from typing import Any, Mapping


TAXONOMY_VERSION = "8C-v1"


@dataclass(frozen=True)
class CanonicalPitch:
    canonical_pitch_id: str
    canonical_name: str
    family: str
    velocity_band: str
    movement_profile: str
    active: bool = True


@dataclass(frozen=True)
class PitchTaxonomyInput:
    source_name: str | None
    source_pitch_value: str | None
    source_record_id: str | None = None
    source_timestamp_utc: datetime | None = None
    enabled: bool = False


@dataclass(frozen=True)
class PitchTaxonomyResult:
    emitted: bool
    reason: str
    source_name: str | None
    source_pitch_value: str | None
    canonical_pitch_id: str | None
    canonical_pitch_name: str | None
    canonical_family: str | None
    normalization_status: str | None
    normalization_rule: str | None
    source_priority: int | None
    source_record_id: str | None
    source_timestamp_utc: str | None
    taxonomy_version: str
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False
    pitch_selection_changed: bool = False
    pitch_sequence_changed: bool = False
    matchup_adjustment_activated: bool = False
    contact_quality_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(self.diagnostic_codes)
        payload["validation_errors"] = list(self.validation_errors)
        return payload


_CANONICAL_PITCHES = (
    CanonicalPitch(
        "FF",
        "four_seam_fastball",
        "fastball",
        "high",
        "ride",
    ),
    CanonicalPitch(
        "SI",
        "sinker",
        "fastball",
        "high",
        "arm_side_run_sink",
    ),
    CanonicalPitch(
        "FC",
        "cutter",
        "fastball",
        "high",
        "glove_side_cut",
    ),
    CanonicalPitch(
        "SL",
        "slider",
        "breaking",
        "medium",
        "glove_side_break",
    ),
    CanonicalPitch(
        "ST",
        "sweeper",
        "breaking",
        "medium",
        "large_horizontal_break",
    ),
    CanonicalPitch(
        "CU",
        "curveball",
        "breaking",
        "low",
        "vertical_break",
    ),
    CanonicalPitch(
        "KC",
        "knuckle_curve",
        "breaking",
        "low",
        "vertical_break",
    ),
    CanonicalPitch(
        "CH",
        "changeup",
        "offspeed",
        "medium",
        "arm_side_fade",
    ),
    CanonicalPitch(
        "FS",
        "splitter",
        "offspeed",
        "medium",
        "vertical_drop",
    ),
    CanonicalPitch(
        "FO",
        "forkball",
        "offspeed",
        "low",
        "vertical_drop",
    ),
    CanonicalPitch(
        "KN",
        "knuckleball",
        "specialty",
        "low",
        "unstable",
    ),
    CanonicalPitch(
        "EP",
        "eephus",
        "specialty",
        "very_low",
        "high_arc",
    ),
    CanonicalPitch(
        "SC",
        "screwball",
        "specialty",
        "low",
        "reverse_break",
    ),
    CanonicalPitch(
        "PO",
        "pitchout",
        "non_competitive",
        "unknown",
        "not_applicable",
    ),
    CanonicalPitch(
        "IN",
        "intentional_ball",
        "non_competitive",
        "unknown",
        "not_applicable",
    ),
    CanonicalPitch(
        "UN",
        "unknown",
        "unknown",
        "unknown",
        "unknown",
    ),
)

CANONICAL_PITCHES: Mapping[str, CanonicalPitch] = MappingProxyType(
    {
        pitch.canonical_pitch_id: pitch
        for pitch in _CANONICAL_PITCHES
    }
)


def _normalize_text(value: str) -> str:
    normalized = value.strip().lower()
    normalized = normalized.replace("_", " ")
    normalized = re.sub(r"[\-–—]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


_EXACT_STATCAST_ALIASES = MappingProxyType(
    {
        "FF": "FF",
        "SI": "SI",
        "FC": "FC",
        "SL": "SL",
        "ST": "ST",
        "CU": "CU",
        "KC": "KC",
        "CH": "CH",
        "FS": "FS",
        "FO": "FO",
        "KN": "KN",
        "EP": "EP",
        "SC": "SC",
        "PO": "PO",
        "IN": "IN",
    }
)

_LEGACY_STATCAST_ALIASES = MappingProxyType(
    {
        "FT": "SI",
    }
)

_GENERIC_ALIASES = MappingProxyType(
    {
        "four seam fastball": "FF",
        "four seamer": "FF",
        "4 seam fastball": "FF",
        "4 seamer": "FF",
        "sinker": "SI",
        "two seam fastball": "SI",
        "two seamer": "SI",
        "2 seam fastball": "SI",
        "2 seamer": "SI",
        "cutter": "FC",
        "cut fastball": "FC",
        "slider": "SL",
        "sweeper": "ST",
        "curveball": "CU",
        "curve": "CU",
        "knuckle curve": "KC",
        "knuckle curveball": "KC",
        "changeup": "CH",
        "change up": "CH",
        "splitter": "FS",
        "split finger": "FS",
        "split finger fastball": "FS",
        "forkball": "FO",
        "knuckleball": "KN",
        "eephus": "EP",
        "screwball": "SC",
        "pitchout": "PO",
        "intentional ball": "IN",
        "intentional walk pitch": "IN",
    }
)

_AMBIGUOUS_ALIASES = frozenset(
    {
        "fastball",
        "breaking ball",
        "offspeed",
        "off speed",
        "curve",
    }
)

_TRUSTED_PROVIDER_NAMES = frozenset(
    {
        "statcast",
        "baseball_savant",
        "baseball savant",
        "trusted_provider",
        "provider",
        "generic",
        "repository",
    }
)


def canonical_pitch_records() -> tuple[CanonicalPitch, ...]:
    return tuple(_CANONICAL_PITCHES)


def canonical_pitch_ids() -> tuple[str, ...]:
    return tuple(
        pitch.canonical_pitch_id
        for pitch in _CANONICAL_PITCHES
    )


def canonical_pitch_names() -> tuple[str, ...]:
    return tuple(
        pitch.canonical_name
        for pitch in _CANONICAL_PITCHES
    )


def _sorted_unique_strings(
    values: list[str] | tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                value
                for value in values
                if isinstance(value, str)
                and value
            }
        )
    )


def _fallback_result(
    *,
    taxonomy_input: PitchTaxonomyInput,
    reason: str,
    normalization_status: str,
    normalization_rule: str,
    source_priority: int,
    diagnostic_codes: list[str],
    validation_errors: list[str] | None = None,
) -> PitchTaxonomyResult:
    unknown = CANONICAL_PITCHES["UN"]

    return PitchTaxonomyResult(
        emitted=True,
        reason=reason,
        source_name=taxonomy_input.source_name,
        source_pitch_value=taxonomy_input.source_pitch_value,
        canonical_pitch_id=unknown.canonical_pitch_id,
        canonical_pitch_name=unknown.canonical_name,
        canonical_family=unknown.family,
        normalization_status=normalization_status,
        normalization_rule=normalization_rule,
        source_priority=source_priority,
        source_record_id=taxonomy_input.source_record_id,
        source_timestamp_utc=(
            taxonomy_input.source_timestamp_utc.isoformat()
            if taxonomy_input.source_timestamp_utc is not None
            else None
        ),
        taxonomy_version=TAXONOMY_VERSION,
        diagnostic_codes=_sorted_unique_strings(
            diagnostic_codes
        ),
        validation_errors=_sorted_unique_strings(
            validation_errors or []
        ),
    )


def normalize_pitch_type(
    taxonomy_input: PitchTaxonomyInput,
) -> PitchTaxonomyResult:
    if not taxonomy_input.enabled:
        return PitchTaxonomyResult(
            emitted=False,
            reason="taxonomy_disabled",
            source_name=taxonomy_input.source_name,
            source_pitch_value=taxonomy_input.source_pitch_value,
            canonical_pitch_id=None,
            canonical_pitch_name=None,
            canonical_family=None,
            normalization_status=None,
            normalization_rule=None,
            source_priority=None,
            source_record_id=taxonomy_input.source_record_id,
            source_timestamp_utc=(
                taxonomy_input.source_timestamp_utc.isoformat()
                if taxonomy_input.source_timestamp_utc is not None
                else None
            ),
            taxonomy_version=TAXONOMY_VERSION,
            diagnostic_codes=(
                "pitch_taxonomy_disabled",
            ),
            validation_errors=(),
        )

    source_name = (
        taxonomy_input.source_name.strip().lower()
        if isinstance(taxonomy_input.source_name, str)
        else ""
    )

    if not source_name:
        return _fallback_result(
            taxonomy_input=taxonomy_input,
            reason="missing_source_name",
            normalization_status="missing",
            normalization_rule="unknown_fallback",
            source_priority=5,
            diagnostic_codes=[
                "pitch_taxonomy_source_name_missing",
            ],
        )

    raw_value = taxonomy_input.source_pitch_value

    if raw_value is None or not str(raw_value).strip():
        return _fallback_result(
            taxonomy_input=taxonomy_input,
            reason="missing_source_value",
            normalization_status="missing",
            normalization_rule="unknown_fallback",
            source_priority=5,
            diagnostic_codes=[
                "pitch_taxonomy_source_value_missing",
            ],
        )

    raw_text = str(raw_value).strip()
    raw_upper = raw_text.upper()
    normalized = _normalize_text(raw_text)

    if normalized in _AMBIGUOUS_ALIASES:
        return _fallback_result(
            taxonomy_input=taxonomy_input,
            reason="ambiguous_source_value",
            normalization_status="ambiguous",
            normalization_rule="unknown_fallback",
            source_priority=5,
            diagnostic_codes=[
                "pitch_taxonomy_source_value_ambiguous",
            ],
        )

    if (
        source_name in {"statcast", "baseball_savant", "baseball savant"}
        and raw_upper in _EXACT_STATCAST_ALIASES
    ):
        canonical_id = _EXACT_STATCAST_ALIASES[
            raw_upper
        ]
        canonical = CANONICAL_PITCHES[
            canonical_id
        ]

        return PitchTaxonomyResult(
            emitted=True,
            reason="normalized",
            source_name=taxonomy_input.source_name,
            source_pitch_value=taxonomy_input.source_pitch_value,
            canonical_pitch_id=canonical.canonical_pitch_id,
            canonical_pitch_name=canonical.canonical_name,
            canonical_family=canonical.family,
            normalization_status="exact",
            normalization_rule="statcast_explicit_pitch_type",
            source_priority=1,
            source_record_id=taxonomy_input.source_record_id,
            source_timestamp_utc=(
                taxonomy_input.source_timestamp_utc.isoformat()
                if taxonomy_input.source_timestamp_utc is not None
                else None
            ),
            taxonomy_version=TAXONOMY_VERSION,
            diagnostic_codes=(
                "pitch_taxonomy_exact_match",
            ),
            validation_errors=(),
        )

    if (
        source_name in {"statcast", "baseball_savant", "baseball savant"}
        and raw_upper in _LEGACY_STATCAST_ALIASES
    ):
        canonical_id = _LEGACY_STATCAST_ALIASES[
            raw_upper
        ]
        canonical = CANONICAL_PITCHES[
            canonical_id
        ]

        return PitchTaxonomyResult(
            emitted=True,
            reason="normalized",
            source_name=taxonomy_input.source_name,
            source_pitch_value=taxonomy_input.source_pitch_value,
            canonical_pitch_id=canonical.canonical_pitch_id,
            canonical_pitch_name=canonical.canonical_name,
            canonical_family=canonical.family,
            normalization_status="legacy_alias",
            normalization_rule="source_specific_legacy_alias",
            source_priority=4,
            source_record_id=taxonomy_input.source_record_id,
            source_timestamp_utc=(
                taxonomy_input.source_timestamp_utc.isoformat()
                if taxonomy_input.source_timestamp_utc is not None
                else None
            ),
            taxonomy_version=TAXONOMY_VERSION,
            diagnostic_codes=(
                "pitch_taxonomy_legacy_alias_used",
            ),
            validation_errors=(),
        )

    if raw_upper in CANONICAL_PITCHES:
        canonical = CANONICAL_PITCHES[
            raw_upper
        ]

        return PitchTaxonomyResult(
            emitted=True,
            reason="normalized",
            source_name=taxonomy_input.source_name,
            source_pitch_value=taxonomy_input.source_pitch_value,
            canonical_pitch_id=canonical.canonical_pitch_id,
            canonical_pitch_name=canonical.canonical_name,
            canonical_family=canonical.family,
            normalization_status="exact",
            normalization_rule="trusted_provider_explicit_pitch_type",
            source_priority=2,
            source_record_id=taxonomy_input.source_record_id,
            source_timestamp_utc=(
                taxonomy_input.source_timestamp_utc.isoformat()
                if taxonomy_input.source_timestamp_utc is not None
                else None
            ),
            taxonomy_version=TAXONOMY_VERSION,
            diagnostic_codes=(
                "pitch_taxonomy_trusted_provider_exact_match",
            ),
            validation_errors=(),
        )

    if normalized in _GENERIC_ALIASES:
        canonical_id = _GENERIC_ALIASES[
            normalized
        ]
        canonical = CANONICAL_PITCHES[
            canonical_id
        ]

        return PitchTaxonomyResult(
            emitted=True,
            reason="normalized",
            source_name=taxonomy_input.source_name,
            source_pitch_value=taxonomy_input.source_pitch_value,
            canonical_pitch_id=canonical.canonical_pitch_id,
            canonical_pitch_name=canonical.canonical_name,
            canonical_family=canonical.family,
            normalization_status="normalized_alias",
            normalization_rule="repository_canonical_alias",
            source_priority=3,
            source_record_id=taxonomy_input.source_record_id,
            source_timestamp_utc=(
                taxonomy_input.source_timestamp_utc.isoformat()
                if taxonomy_input.source_timestamp_utc is not None
                else None
            ),
            taxonomy_version=TAXONOMY_VERSION,
            diagnostic_codes=(
                "pitch_taxonomy_alias_normalized",
            ),
            validation_errors=(),
        )

    diagnostic_codes = [
        "pitch_taxonomy_source_value_unsupported",
    ]

    if source_name not in _TRUSTED_PROVIDER_NAMES:
        diagnostic_codes.append(
            "pitch_taxonomy_source_name_unrecognized"
        )

    return _fallback_result(
        taxonomy_input=taxonomy_input,
        reason="unsupported_source_value",
        normalization_status="unsupported",
        normalization_rule="unknown_fallback",
        source_priority=5,
        diagnostic_codes=diagnostic_codes,
    )


def normalize_pitch_payload(
    payload: Mapping[str, Any],
) -> PitchTaxonomyResult:
    copied_payload = dict(payload)

    timestamp = copied_payload.get(
        "source_timestamp_utc"
    )

    if isinstance(timestamp, str):
        try:
            timestamp = datetime.fromisoformat(
                timestamp
            )
        except ValueError:
            timestamp = None

    taxonomy_input = PitchTaxonomyInput(
        source_name=copied_payload.get(
            "source_name"
        ),
        source_pitch_value=copied_payload.get(
            "source_pitch_value"
        ),
        source_record_id=copied_payload.get(
            "source_record_id"
        ),
        source_timestamp_utc=(
            timestamp
            if isinstance(timestamp, datetime)
            else None
        ),
        enabled=bool(
            copied_payload.get(
                "enabled",
                False,
            )
        ),
    )

    return normalize_pitch_type(
        taxonomy_input
    )
