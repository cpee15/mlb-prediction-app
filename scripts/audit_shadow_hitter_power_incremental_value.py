"""Run the cutoff-safe shadow hitter expected-power incremental audit."""

from __future__ import annotations

import collections
import datetime as dt
import json
import math
import statistics

from mlb_app.database import StatcastEvent
from mlb_app.model_projection_routes import _session_factory
from mlb_app.simulation.shadow.hitter_power_incremental_validation import (
    bootstrap_hitter_power_model_differences,
    evaluate_hitter_power_incremental_models,
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
BBE_EVENTS = AB_EVENTS - {"strikeout", "strikeout_double_play"}
HIT_EVENTS = {"single", "double", "triple", "home_run"}
TOTAL_BASES = {"single": 1, "double": 2, "triple": 3, "home_run": 4}
WINDOWS = (
    (2024, dt.date(2024, 6, 1), dt.date(2024, 6, 30)),
    (2024, dt.date(2024, 7, 1), dt.date(2024, 7, 31)),
    (2024, dt.date(2024, 8, 1), dt.date(2024, 8, 31)),
    (2025, dt.date(2025, 6, 1), dt.date(2025, 6, 30)),
    (2025, dt.date(2025, 7, 1), dt.date(2025, 7, 31)),
    (2025, dt.date(2025, 8, 1), dt.date(2025, 8, 31)),
)
SINGLE_WEIGHT = 0.90


def _number(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _barrel_proxy(exit_velocity, launch_angle):
    if (
        exit_velocity is None
        or launch_angle is None
        or exit_velocity < 98
    ):
        return 0
    speed = min(exit_velocity, 116.0)
    lower = max(8.0, 26.0 - (speed - 98.0))
    upper = min(50.0, 30.0 + ((speed - 98.0) * 2.0))
    return int(lower <= launch_angle <= upper)


def _terminal_rows(rows):
    deduped = {}
    for row in rows:
        (
            row_id,
            game_date,
            game_pk,
            at_bat,
            pitch_number,
            batter_id,
            hand,
            event,
            exit_velocity,
            launch_angle,
            xba,
            xwoba,
        ) = row
        if game_pk is not None and at_bat is not None:
            key = (int(batter_id), hand, int(game_pk), int(at_bat))
        else:
            key = (int(batter_id), hand, "row", int(row_id))
        deduped[key] = row
    return list(deduped.values())


def _sample(player_id, hand, season, cutoff, holdout_end, pre, holdout):
    pre_ab = len(pre)
    holdout_ab = len(holdout)
    if pre_ab < 50:
        return None, "insufficient_pre_ab"
    if holdout_ab < 20:
        return None, "insufficient_holdout_ab"
    pre_bbe = [row for row in pre if row[7] in BBE_EVENTS]
    covered = [
        row
        for row in pre_bbe
        if _number(row[10]) is not None and _number(row[11]) is not None
    ]
    if len(covered) < 20:
        return None, "insufficient_expected_bbe"
    coverage = len(covered) / len(pre_bbe) if pre_bbe else 0.0
    if coverage < 0.80:
        return None, "insufficient_expected_coverage"
    geometry = [
        row
        for row in pre_bbe
        if _number(row[8]) is not None and _number(row[9]) is not None
    ]
    if len(geometry) < 20:
        return None, "insufficient_contact_geometry"

    pre_hits = sum(row[7] in HIT_EVENTS for row in pre)
    pre_tb = sum(TOTAL_BASES.get(row[7], 0) for row in pre)
    holdout_hits = sum(row[7] in HIT_EVENTS for row in holdout)
    holdout_tb = sum(TOTAL_BASES.get(row[7], 0) for row in holdout)
    damage = [
        max(
            _number(row[11]) - (SINGLE_WEIGHT * _number(row[10])),
            0.0,
        )
        for row in covered
    ]
    expected_damage_per_ab = (
        statistics.fmean(damage) * (len(pre_bbe) / pre_ab)
    )
    return {
        "season": season,
        "cutoff": cutoff.isoformat(),
        "holdout_end": holdout_end.isoformat(),
        "player_id": player_id,
        "split": "vsR" if hand == "R" else "vsL",
        "pre_ab": pre_ab,
        "pre_bbe": len(pre_bbe),
        "expected_bbe": len(covered),
        "expected_coverage": coverage,
        "pre_actual_iso": (pre_tb - pre_hits) / pre_ab,
        "pre_expected_damage_per_ab": expected_damage_per_ab,
        "pre_hard_hit_rate": statistics.fmean(
            int(_number(row[8]) >= 95) for row in geometry
        ),
        "pre_barrel_proxy_rate": statistics.fmean(
            _barrel_proxy(_number(row[8]), _number(row[9]))
            for row in geometry
        ),
        "holdout_ab": holdout_ab,
        "holdout_iso": (holdout_tb - holdout_hits) / holdout_ab,
    }, None


def main():
    factory = _session_factory()
    session = factory()
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
                StatcastEvent.game_date >= dt.date(season, 1, 1),
                StatcastEvent.game_date <= holdout_end,
                StatcastEvent.p_throws.in_(("R", "L")),
                StatcastEvent.events.in_(tuple(AB_EVENTS)),
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
        grouped = collections.defaultdict(lambda: {"pre": [], "holdout": []})
        for row in _terminal_rows(raw):
            key = (int(row[5]), row[6])
            target = "pre" if row[1] <= cutoff else "holdout"
            grouped[key][target].append(row)
        blocked = collections.Counter()
        window_samples = []
        for (player_id, hand), periods in grouped.items():
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
                blocked[blocker] += 1
            else:
                samples.append(result)
                window_samples.append(result)
        window_coverage.append({
            "season": season,
            "cutoff": cutoff.isoformat(),
            "holdout_end": holdout_end.isoformat(),
            "sample_count": len(window_samples),
            "holdout_ab": sum(row["holdout_ab"] for row in window_samples),
            "split_counts": dict(
                collections.Counter(row["split"] for row in window_samples)
            ),
            "mean_expected_coverage": (
                statistics.fmean(
                    row["expected_coverage"] for row in window_samples
                )
                if window_samples
                else None
            ),
            "blocked_counts": dict(blocked),
        })
    result = evaluate_hitter_power_incremental_models(samples)
    result["clustered_bootstrap"] = (
        bootstrap_hitter_power_model_differences(
            samples,
            iterations=400,
        )
    )
    result["audit_method"] = {
        "schema_version":
            "cutoff_safe_hitter_power_incremental_audit_v1",
        "single_weight": SINGLE_WEIGHT,
        "expected_damage_definition":
            "mean(max(xwoba - 0.90*xba, 0)) * BBE/AB",
        "barrel_is_proxy": True,
        "direct_xslg_used": False,
    }
    result["window_coverage"] = window_coverage
    print(json.dumps(result, indent=2, sort_keys=True))
    session.close()


if __name__ == "__main__":
    main()
