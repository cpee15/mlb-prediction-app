"""Run the cutoff-safe shadow hitter hit-type allocation audit."""

from __future__ import annotations

import collections
import datetime as dt
import json
import math
import statistics

from mlb_app.database import StatcastEvent
from mlb_app.model_projection_routes import _session_factory
from mlb_app.simulation.shadow.hitter_hit_type_allocation_validation import (
    bootstrap_hitter_hit_type_allocation_differences,
    evaluate_hitter_hit_type_allocation_models,
)


AB_EVENTS = {
    "field_out",
    "force_out",
    "grounded_into_double_play",
    "field_error",
    "fielders_choice",
    "double_play",
    "fielders_choice_out",
    "triple_play",
    "single",
    "double",
    "triple",
    "home_run",
    "strikeout",
    "strikeout_double_play",
}
BBE_EVENTS = AB_EVENTS - {
    "strikeout",
    "strikeout_double_play",
}
HIT_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
}
WINDOWS = (
    (
        2024,
        dt.date(2024, 6, 1),
        dt.date(2024, 6, 30),
    ),
    (
        2024,
        dt.date(2024, 7, 1),
        dt.date(2024, 7, 31),
    ),
    (
        2024,
        dt.date(2024, 8, 1),
        dt.date(2024, 8, 31),
    ),
    (
        2025,
        dt.date(2025, 6, 1),
        dt.date(2025, 6, 30),
    ),
    (
        2025,
        dt.date(2025, 7, 1),
        dt.date(2025, 7, 31),
    ),
    (
        2025,
        dt.date(2025, 8, 1),
        dt.date(2025, 8, 31),
    ),
)
SINGLE_WEIGHT = 0.90


def _number(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _terminal_rows(rows):
    deduped = {}
    for row in rows:
        (
            row_id,
            _game_date,
            game_pk,
            at_bat,
            _pitch_number,
            batter_id,
            hand,
            _event,
            _exit_velocity,
            _launch_angle,
            _xba,
            _xwoba,
        ) = row

        if game_pk is not None and at_bat is not None:
            key = (
                int(batter_id),
                hand,
                int(game_pk),
                int(at_bat),
            )
        else:
            key = (
                int(batter_id),
                hand,
                "row",
                int(row_id),
            )
        deduped[key] = row
    return list(deduped.values())


def _hit_counts(rows):
    counts = collections.Counter(
        row[7]
        for row in rows
        if row[7] in HIT_EVENTS
    )
    return {
        hit_type: counts.get(hit_type, 0)
        for hit_type in HIT_EVENTS
    }


def _sample(
    player_id,
    hand,
    season,
    cutoff,
    holdout_end,
    pre,
    holdout,
):
    pre_ab = len(pre)
    holdout_ab = len(holdout)

    if pre_ab < 50:
        return None, "insufficient_pre_ab"
    if holdout_ab < 20:
        return None, "insufficient_holdout_ab"

    pre_counts = _hit_counts(pre)
    holdout_counts = _hit_counts(holdout)
    pre_hits = sum(pre_counts.values())
    holdout_hits = sum(holdout_counts.values())

    if pre_hits < 15:
        return None, "insufficient_pre_hits"
    if holdout_hits < 8:
        return None, "insufficient_holdout_hits"

    pre_bbe = [
        row
        for row in pre
        if row[7] in BBE_EVENTS
    ]
    expected_rows = [
        row
        for row in pre_bbe
        if (
            _number(row[10]) is not None
            and _number(row[11]) is not None
        )
    ]
    if len(expected_rows) < 20:
        return None, "insufficient_expected_bbe"

    expected_coverage = (
        len(expected_rows) / len(pre_bbe)
        if pre_bbe
        else 0.0
    )
    if expected_coverage < 0.80:
        return None, "insufficient_expected_coverage"

    geometry_rows = [
        row
        for row in pre_bbe
        if (
            _number(row[8]) is not None
            and _number(row[9]) is not None
        )
    ]
    if len(geometry_rows) < 20:
        return None, "insufficient_contact_geometry"

    expected_damage = [
        max(
            _number(row[11])
            - (
                SINGLE_WEIGHT
                * _number(row[10])
            ),
            0.0,
        )
        for row in expected_rows
    ]

    return {
        "season": season,
        "cutoff": cutoff.isoformat(),
        "holdout_end": holdout_end.isoformat(),
        "player_id": player_id,
        "split": (
            "vsR"
            if hand == "R"
            else "vsL"
        ),
        "pre_ab": pre_ab,
        "pre_hits": pre_hits,
        "pre_bbe": len(pre_bbe),
        "expected_bbe": len(expected_rows),
        "expected_coverage": expected_coverage,
        "geometry_bbe": len(geometry_rows),
        "pre_single_share":
            pre_counts["single"] / pre_hits,
        "pre_double_share":
            pre_counts["double"] / pre_hits,
        "pre_triple_share":
            pre_counts["triple"] / pre_hits,
        "pre_home_run_share":
            pre_counts["home_run"] / pre_hits,
        "pre_expected_damage_per_bbe":
            statistics.fmean(expected_damage),
        "pre_avg_exit_velocity":
            statistics.fmean(
                _number(row[8])
                for row in geometry_rows
            ),
        "pre_avg_launch_angle":
            statistics.fmean(
                _number(row[9])
                for row in geometry_rows
            ),
        "holdout_ab": holdout_ab,
        "holdout_hits": holdout_hits,
        "holdout_single_count":
            holdout_counts["single"],
        "holdout_double_count":
            holdout_counts["double"],
        "holdout_triple_count":
            holdout_counts["triple"],
        "holdout_home_run_count":
            holdout_counts["home_run"],
    }, None


def build_samples(session):
    samples = []
    window_coverage = []

    for season, cutoff, holdout_end in WINDOWS:
        raw = (
            session.query(
                StatcastEvent.id,
                StatcastEvent.game_date,
                StatcastEvent.game_pk,
                StatcastEvent.at_bat_number,
                StatcastEvent.pitch_number,
                StatcastEvent.batter_id,
                StatcastEvent.p_throws,
                StatcastEvent.events,
                StatcastEvent.launch_speed,
                StatcastEvent.launch_angle,
                StatcastEvent.estimated_ba_using_speedangle,
                StatcastEvent.estimated_woba_using_speedangle,
            )
            .filter(
                StatcastEvent.game_date
                >= dt.date(season, 1, 1),
                StatcastEvent.game_date
                <= holdout_end,
                StatcastEvent.p_throws.in_(
                    ("R", "L")
                ),
                StatcastEvent.events.in_(
                    tuple(AB_EVENTS)
                ),
            )
            .order_by(
                StatcastEvent.batter_id,
                StatcastEvent.p_throws,
                StatcastEvent.game_date,
                StatcastEvent.game_pk,
                StatcastEvent.at_bat_number,
                StatcastEvent.pitch_number,
                StatcastEvent.id,
            )
            .all()
        )

        grouped = collections.defaultdict(
            lambda: {
                "pre": [],
                "holdout": [],
            }
        )
        for row in _terminal_rows(raw):
            key = (
                int(row[5]),
                row[6],
            )
            period = (
                "pre"
                if row[1] <= cutoff
                else "holdout"
            )
            grouped[key][period].append(row)

        blockers = collections.Counter()
        window_samples = []

        for (
            player_id,
            hand,
        ), periods in grouped.items():
            result, blocker = _sample(
                player_id,
                hand,
                season,
                cutoff,
                holdout_end,
                periods["pre"],
                periods["holdout"],
            )
            if blocker:
                blockers[blocker] += 1
                continue
            samples.append(result)
            window_samples.append(result)

        window_coverage.append({
            "season": season,
            "cutoff": cutoff.isoformat(),
            "holdout_end":
                holdout_end.isoformat(),
            "sample_count":
                len(window_samples),
            "holdout_ab": sum(
                row["holdout_ab"]
                for row in window_samples
            ),
            "holdout_hits": sum(
                row["holdout_hits"]
                for row in window_samples
            ),
            "split_counts": dict(
                collections.Counter(
                    row["split"]
                    for row in window_samples
                )
            ),
            "mean_expected_coverage": (
                statistics.fmean(
                    row["expected_coverage"]
                    for row in window_samples
                )
                if window_samples
                else None
            ),
            "blocker_counts": dict(blockers),
        })

    return samples, window_coverage


def main():
    factory = _session_factory()
    session = factory()
    try:
        samples, window_coverage = (
            build_samples(session)
        )
    finally:
        session.close()

    result = (
        evaluate_hitter_hit_type_allocation_models(
            samples
        )
    )
    bootstrap = (
        bootstrap_hitter_hit_type_allocation_differences(
            samples
        )
    )

    payload = {
        **result,
        "schema_version":
            "historical_shadow_hitter_hit_type_allocation_audit_v1",
        "window_coverage": window_coverage,
        "holdout_ab": sum(
            row["holdout_ab"]
            for row in samples
        ),
        "holdout_hits": sum(
            row["holdout_hits"]
            for row in samples
        ),
        "sample_counts_by_season": dict(
            collections.Counter(
                row["season"]
                for row in samples
            )
        ),
        "sample_counts_by_split": dict(
            collections.Counter(
                row["split"]
                for row in samples
            )
        ),
        "allocation_policy": {
            "condition": "conditional_on_hit",
            "joint_outcomes": [
                "single",
                "double",
                "triple",
                "home_run",
            ],
            "primary_metric":
                "weighted_multinomial_log_loss",
        },
        "speed_policy": {
            "sprint_speed_included": False,
            "reason":
                "sprint speed is not stored in "
                "StatcastEvent",
            "triple_conclusion_restricted": True,
        },
        "production_impact": {
            "parameter_selected": False,
            "production_authority_changed": False,
            "pa_outcome_model_modified": False,
        },
        "clustered_bootstrap": bootstrap,
    }
    print(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
