"""Validation for canonical projection payloads."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Tuple

from .contracts import CanonicalProjectionPayload
from .serialization import projection_payload_to_dict


@dataclass(frozen=True)
class ProjectionPayloadValidation:
    simulation_counts_match: bool
    summaries_are_finite: bool
    deterministic_ordering: bool
    json_serializable: bool
    warnings: Tuple[str, ...]

    @property
    def passed(self) -> bool:
        return (
            self.simulation_counts_match
            and self.summaries_are_finite
            and self.deterministic_ordering
            and self.json_serializable
        )


def validate_projection_payload(
    payload: CanonicalProjectionPayload,
) -> ProjectionPayloadValidation:
    summaries = [
        metric.summary
        for team in payload.teams
        for metric in team.metrics
    ] + [
        metric.summary
        for player in (
            payload.batters + payload.pitchers
        )
        for metric in player.metrics
    ]

    simulation_counts_match = all(
        summary.count == payload.simulation_count
        for summary in summaries
    )

    summaries_are_finite = all(
        _summary_is_finite(summary)
        for summary in summaries
    )

    deterministic_ordering = (
        tuple(
            team.team_side
            for team in payload.teams
        ) == ("away", "home")
        and tuple(
            (
                player.team_side,
                player.player_id,
            )
            for player in payload.batters
        )
        == tuple(
            sorted(
                (
                    player.team_side,
                    player.player_id,
                )
                for player in payload.batters
            )
        )
        and tuple(
            (
                player.team_side,
                player.player_id,
            )
            for player in payload.pitchers
        )
        == tuple(
            sorted(
                (
                    player.team_side,
                    player.player_id,
                )
                for player in payload.pitchers
            )
        )
    )

    try:
        json.dumps(
            projection_payload_to_dict(payload),
            sort_keys=True,
        )
        json_serializable = True
    except (TypeError, ValueError):
        json_serializable = False

    warnings = []

    if not simulation_counts_match:
        warnings.append(
            "summary_simulation_count_mismatch"
        )
    if not summaries_are_finite:
        warnings.append(
            "nonfinite_summary_value"
        )
    if not deterministic_ordering:
        warnings.append(
            "nondeterministic_payload_order"
        )
    if not json_serializable:
        warnings.append(
            "payload_not_json_serializable"
        )

    return ProjectionPayloadValidation(
        simulation_counts_match=(
            simulation_counts_match
        ),
        summaries_are_finite=summaries_are_finite,
        deterministic_ordering=deterministic_ordering,
        json_serializable=json_serializable,
        warnings=tuple(warnings),
    )


def _summary_is_finite(summary) -> bool:
    import math

    return all(
        math.isfinite(value)
        for value in (
            summary.mean,
            summary.median,
            summary.p10,
            summary.p25,
            summary.p75,
            summary.p90,
            summary.minimum,
            summary.maximum,
        )
    )
