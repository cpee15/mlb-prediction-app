"""
Append-only diagnostic shadow dataset for Layer 8K matchup observations.

The dataset is deterministic and non-authoritative. It does not join outcomes,
evaluate predictions, tune parameters, or modify production/simulation behavior.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Iterable, Sequence

from mlb_app.pitching.pitch_type_matchup_overlay_observability import (
    MatchupOverlayObservationBundle,
)


SHADOW_DATASET_VERSION = "8M-v1"

SHADOW_ROW_FIELD_ORDER = (
    "dataset_row_id",
    "observation_id",
    "observation_date_utc",
    "pitcher_id",
    "batter_id",
    "pitcher_hand",
    "batter_hand",
    "count_context",
    "overlay_status",
    "observability_status",
    "coverage_share",
    "matched_pitch_count",
    "unmatched_pitch_count",
    "overlay_entry_count",
    "fallback_entry_count",
    "unknown_pitch_entry_count",
    "pitcher_only_entry_count",
    "matched_usage_share",
    "unmatched_usage_share",
    "pitcher_profile_version",
    "batter_profile_version",
    "overlay_version",
    "observability_version",
    "shadow_dataset_version",
)

SUPPORTED_OBSERVABILITY_STATUSES = frozenset(
    {
        "complete",
        "partial",
        "empty",
        "invalid",
        "disabled",
    }
)

SUPPORTED_OVERLAY_STATUSES = frozenset(
    {
        "resolved",
        "partial",
        "sparse",
        "stale",
        "unavailable",
        "invalid",
        "disabled",
    }
)


@dataclass(frozen=True)
class MatchupOverlayShadowRow:
    dataset_row_id: str
    observation_id: str
    observation_date_utc: str
    pitcher_id: str | None
    batter_id: str | None
    pitcher_hand: str | None
    batter_hand: str | None
    count_context: str | None
    overlay_status: str
    observability_status: str
    coverage_share: float
    matched_pitch_count: int
    unmatched_pitch_count: int
    overlay_entry_count: int
    fallback_entry_count: int
    unknown_pitch_entry_count: int
    pitcher_only_entry_count: int
    matched_usage_share: float | None
    unmatched_usage_share: float | None
    pitcher_profile_version: str | None
    batter_profile_version: str | None
    overlay_version: str
    observability_version: str
    shadow_dataset_version: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchupOverlayShadowPartition:
    partition_key: str
    partition_path: str
    row_count: int
    minimum_observation_id: str
    maximum_observation_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchupOverlayShadowDuplicate:
    dataset_row_id: str
    observation_id: str
    observation_date_utc: str
    duplicate_type: str
    duplicate_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchupOverlayShadowManifest:
    shadow_dataset_version: str
    generated_at_utc: str
    row_count: int
    unique_observation_count: int
    duplicate_row_count: int
    partition_count: int
    minimum_observation_date_utc: str | None
    maximum_observation_date_utc: str | None
    schema_fingerprint: str
    production_authority: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchupOverlayShadowDataset:
    emitted: bool
    reason: str
    dataset_status: str
    rows: tuple[MatchupOverlayShadowRow, ...]
    partitions: tuple[MatchupOverlayShadowPartition, ...]
    duplicates: tuple[MatchupOverlayShadowDuplicate, ...]
    manifest: MatchupOverlayShadowManifest | None
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    shadow_dataset_version: str
    append_only: bool = True
    historical_outcomes_joined: bool = False
    predictive_evaluation_executed: bool = False
    production_authority: bool = False
    production_behavior_changed: bool = False
    simulation_behavior_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "emitted": self.emitted,
            "reason": self.reason,
            "dataset_status": self.dataset_status,
            "rows": [row.to_dict() for row in self.rows],
            "partitions": [
                partition.to_dict()
                for partition in self.partitions
            ],
            "duplicates": [
                duplicate.to_dict()
                for duplicate in self.duplicates
            ],
            "manifest": (
                self.manifest.to_dict()
                if self.manifest is not None
                else None
            ),
            "diagnostic_codes": list(self.diagnostic_codes),
            "validation_errors": list(self.validation_errors),
            "shadow_dataset_version": (
                self.shadow_dataset_version
            ),
            "append_only": self.append_only,
            "historical_outcomes_joined": (
                self.historical_outcomes_joined
            ),
            "predictive_evaluation_executed": (
                self.predictive_evaluation_executed
            ),
            "production_authority": self.production_authority,
            "production_behavior_changed": (
                self.production_behavior_changed
            ),
            "simulation_behavior_changed": (
                self.simulation_behavior_changed
            ),
        }


def _sorted_unique_strings(
    values: Iterable[str],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                value
                for value in values
                if isinstance(value, str) and value
            }
        )
    )


def _schema_fingerprint() -> str:
    serialized = json.dumps(
        SHADOW_ROW_FIELD_ORDER,
        separators=(",", ":"),
    )
    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


def _dataset_row_id(
    observation_id: str,
    observation_date_utc: str,
) -> str:
    serialized = json.dumps(
        {
            "observation_id": observation_id,
            "observation_date_utc": observation_date_utc,
            "shadow_dataset_version": SHADOW_DATASET_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()[:20]
    return f"matchup-shadow-{digest}"


def _partition_key(
    observation_date_utc: str,
) -> str:
    return observation_date_utc.replace("-", "_")


def _row_signature(
    row: MatchupOverlayShadowRow,
) -> str:
    return json.dumps(
        row.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    )


def build_pitch_type_matchup_overlay_shadow_dataset(
    bundles: Sequence[
        MatchupOverlayObservationBundle | None
    ],
    *,
    enabled: bool = False,
    generated_at_utc: str | None = None,
) -> MatchupOverlayShadowDataset:
    if not enabled:
        return MatchupOverlayShadowDataset(
            emitted=False,
            reason="shadow_dataset_disabled",
            dataset_status="disabled",
            rows=(),
            partitions=(),
            duplicates=(),
            manifest=None,
            diagnostic_codes=(
                "matchup_shadow_dataset_disabled",
            ),
            validation_errors=(),
            shadow_dataset_version=SHADOW_DATASET_VERSION,
        )

    diagnostics: list[str] = []
    validation_errors: list[str] = []
    candidate_rows: list[MatchupOverlayShadowRow] = []

    for bundle in bundles:
        if bundle is None:
            validation_errors.append(
                "matchup_shadow_observation_missing"
            )
            continue

        if not bundle.emitted:
            diagnostics.append(
                "matchup_shadow_observation_not_emitted"
            )
            continue

        if bundle.summary is None:
            diagnostics.append(
                "matchup_shadow_summary_missing"
            )
            continue

        summary = bundle.summary
        observation_date = summary.as_of_date_utc

        if not observation_date:
            validation_errors.append(
                "matchup_shadow_date_missing"
            )
            continue

        if summary.overlay_status not in SUPPORTED_OVERLAY_STATUSES:
            validation_errors.append(
                "matchup_shadow_overlay_status_invalid"
            )

        if (
            bundle.observability_status
            not in SUPPORTED_OBSERVABILITY_STATUSES
        ):
            validation_errors.append(
                "matchup_shadow_observability_status_invalid"
            )

        if not 0.0 <= summary.coverage_share <= 1.0:
            validation_errors.append(
                "matchup_shadow_coverage_invalid"
            )

        counts = (
            summary.matched_pitch_count,
            summary.unmatched_pitch_count,
            summary.overlay_entry_count,
            summary.fallback_entry_count,
            summary.unknown_pitch_entry_count,
            summary.pitcher_only_entry_count,
        )

        if any(value < 0 for value in counts):
            validation_errors.append(
                "matchup_shadow_count_invalid"
            )

        usage_values = (
            summary.matched_usage_share,
            summary.unmatched_usage_share,
        )

        numeric_usage = [
            value
            for value in usage_values
            if value is not None
        ]

        if any(
            not 0.0 <= value <= 1.0
            for value in numeric_usage
        ):
            validation_errors.append(
                "matchup_shadow_usage_invalid"
            )

        if (
            len(numeric_usage) == 2
            and sum(numeric_usage) > 1.000001
        ):
            validation_errors.append(
                "matchup_shadow_usage_total_invalid"
            )

        candidate_rows.append(
            MatchupOverlayShadowRow(
                dataset_row_id=_dataset_row_id(
                    summary.observation_id,
                    observation_date,
                ),
                observation_id=summary.observation_id,
                observation_date_utc=observation_date,
                pitcher_id=summary.pitcher_id,
                batter_id=summary.batter_id,
                pitcher_hand=summary.pitcher_hand,
                batter_hand=summary.batter_hand,
                count_context=summary.count_context,
                overlay_status=summary.overlay_status,
                observability_status=(
                    bundle.observability_status
                ),
                coverage_share=round(
                    summary.coverage_share,
                    6,
                ),
                matched_pitch_count=(
                    summary.matched_pitch_count
                ),
                unmatched_pitch_count=(
                    summary.unmatched_pitch_count
                ),
                overlay_entry_count=(
                    summary.overlay_entry_count
                ),
                fallback_entry_count=(
                    summary.fallback_entry_count
                ),
                unknown_pitch_entry_count=(
                    summary.unknown_pitch_entry_count
                ),
                pitcher_only_entry_count=(
                    summary.pitcher_only_entry_count
                ),
                matched_usage_share=(
                    summary.matched_usage_share
                ),
                unmatched_usage_share=(
                    summary.unmatched_usage_share
                ),
                pitcher_profile_version=(
                    summary.pitcher_profile_version
                ),
                batter_profile_version=(
                    summary.batter_profile_version
                ),
                overlay_version=summary.overlay_version,
                observability_version=(
                    summary.observability_version
                ),
                shadow_dataset_version=(
                    SHADOW_DATASET_VERSION
                ),
            )
        )

    grouped: dict[
        str,
        list[MatchupOverlayShadowRow],
    ] = {}

    for row in candidate_rows:
        grouped.setdefault(
            row.dataset_row_id,
            [],
        ).append(row)

    accepted_rows: list[MatchupOverlayShadowRow] = []
    duplicates: list[MatchupOverlayShadowDuplicate] = []

    for dataset_row_id in sorted(grouped):
        group = grouped[dataset_row_id]
        signatures = {
            _row_signature(row)
            for row in group
        }

        if len(signatures) == 1:
            accepted_rows.append(group[0])

            if len(group) > 1:
                diagnostics.append(
                    "matchup_shadow_exact_duplicate_collapsed"
                )
                duplicates.append(
                    MatchupOverlayShadowDuplicate(
                        dataset_row_id=dataset_row_id,
                        observation_id=(
                            group[0].observation_id
                        ),
                        observation_date_utc=(
                            group[
                                0
                            ].observation_date_utc
                        ),
                        duplicate_type="exact",
                        duplicate_count=len(group) - 1,
                    )
                )
        else:
            validation_errors.append(
                "matchup_shadow_conflicting_duplicate"
            )
            duplicates.append(
                MatchupOverlayShadowDuplicate(
                    dataset_row_id=dataset_row_id,
                    observation_id=(
                        group[0].observation_id
                    ),
                    observation_date_utc=(
                        group[0].observation_date_utc
                    ),
                    duplicate_type="conflicting",
                    duplicate_count=len(group) - 1,
                )
            )

    accepted_rows.sort(
        key=lambda row: (
            row.observation_date_utc,
            row.observation_id,
            row.dataset_row_id,
        )
    )

    partition_groups: dict[
        str,
        list[MatchupOverlayShadowRow],
    ] = {}

    for row in accepted_rows:
        key = _partition_key(
            row.observation_date_utc
        )
        partition_groups.setdefault(
            key,
            [],
        ).append(row)

    partitions = tuple(
        MatchupOverlayShadowPartition(
            partition_key=key,
            partition_path=(
                f"observation_date_utc={key}/"
                "shadow_dataset_rows.csv"
            ),
            row_count=len(rows),
            minimum_observation_id=min(
                row.observation_id
                for row in rows
            ),
            maximum_observation_id=max(
                row.observation_id
                for row in rows
            ),
        )
        for key, rows in sorted(
            partition_groups.items()
        )
    )

    dates = [
        row.observation_date_utc
        for row in accepted_rows
    ]

    generated_at = (
        generated_at_utc
        or datetime.now(
            timezone.utc
        ).isoformat()
    )

    manifest = MatchupOverlayShadowManifest(
        shadow_dataset_version=(
            SHADOW_DATASET_VERSION
        ),
        generated_at_utc=generated_at,
        row_count=len(accepted_rows),
        unique_observation_count=len(
            {
                row.observation_id
                for row in accepted_rows
            }
        ),
        duplicate_row_count=sum(
            duplicate.duplicate_count
            for duplicate in duplicates
        ),
        partition_count=len(partitions),
        minimum_observation_date_utc=(
            min(dates) if dates else None
        ),
        maximum_observation_date_utc=(
            max(dates) if dates else None
        ),
        schema_fingerprint=_schema_fingerprint(),
    )

    if validation_errors:
        dataset_status = "invalid"
        reason = "shadow_dataset_invalid"
    elif not accepted_rows:
        dataset_status = "empty"
        reason = "shadow_dataset_empty"
    elif any(
        row.observability_status
        in {"partial", "empty", "invalid"}
        for row in accepted_rows
    ):
        dataset_status = "partial"
        reason = "shadow_dataset_partial"
    else:
        dataset_status = "ready"
        reason = "shadow_dataset_ready"

    return MatchupOverlayShadowDataset(
        emitted=True,
        reason=reason,
        dataset_status=dataset_status,
        rows=tuple(accepted_rows),
        partitions=partitions,
        duplicates=tuple(duplicates),
        manifest=manifest,
        diagnostic_codes=_sorted_unique_strings(
            diagnostics
        ),
        validation_errors=_sorted_unique_strings(
            validation_errors
        ),
        shadow_dataset_version=(
            SHADOW_DATASET_VERSION
        ),
    )
