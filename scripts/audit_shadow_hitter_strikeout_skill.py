"""Audit cutoff-safe hitter strikeout-skill evidence on historical holdouts."""

from __future__ import annotations

import collections
import datetime as dt
import json

from sqlalchemy import text

from mlb_app.model_projection_routes import _session_factory
from mlb_app.simulation.shadow.hitter_strikeout_skill_validation import (
    bootstrap_hitter_strikeout_skill_differences,
    evaluate_hitter_strikeout_skill_models,
)


VALID_EVENT = """
events IS NOT NULL
AND LOWER(TRIM(events)) NOT IN ('', 'nan', 'none', 'null')
"""
WHIFF = """
description IN (
    'swinging_strike',
    'swinging_strike_blocked',
    'missed_bunt'
)
"""
SWING = """
description IN (
    'swinging_strike',
    'swinging_strike_blocked',
    'missed_bunt',
    'foul',
    'foul_tip',
    'foul_bunt',
    'bunt_foul_tip',
    'hit_into_play'
)
"""
STRIKEOUT = """
events IN ('strikeout', 'strikeout_double_play')
"""
WINDOWS = (
    (2024, dt.date(2024, 6, 1), dt.date(2024, 6, 30)),
    (2024, dt.date(2024, 7, 1), dt.date(2024, 7, 31)),
    (2024, dt.date(2024, 8, 1), dt.date(2024, 8, 31)),
    (2025, dt.date(2025, 6, 1), dt.date(2025, 6, 30)),
    (2025, dt.date(2025, 7, 1), dt.date(2025, 7, 31)),
    (2025, dt.date(2025, 8, 1), dt.date(2025, 8, 31)),
)


def _aggregate(session, start: dt.date, end: dt.date) -> dict:
    query = text(
        "SELECT batter_id, p_throws, "
        "COUNT(*) AS pitches, "
        "SUM(CASE WHEN " + SWING + " THEN 1 ELSE 0 END) AS swings, "
        "SUM(CASE WHEN " + WHIFF + " THEN 1 ELSE 0 END) AS whiffs, "
        "SUM(CASE WHEN description='called_strike' THEN 1 ELSE 0 END) "
        "AS called_strikes, "
        "SUM(CASE WHEN " + VALID_EVENT + " THEN 1 ELSE 0 END) AS pa, "
        "SUM(CASE WHEN " + STRIKEOUT + " THEN 1 ELSE 0 END) AS strikeouts "
        "FROM statcast_events "
        "WHERE game_date>=:start AND game_date<=:end "
        "AND batter_id IS NOT NULL AND p_throws IN ('R','L') "
        "GROUP BY batter_id,p_throws"
    )
    return {
        (int(row["batter_id"]), str(row["p_throws"])): {
            name: int(row[name] or 0)
            for name in (
                "pitches",
                "swings",
                "whiffs",
                "called_strikes",
                "pa",
                "strikeouts",
            )
        }
        for row in session.execute(
            query,
            {"start": start, "end": end},
        ).mappings()
    }


def build_samples(session) -> tuple[list[dict], list[dict]]:
    samples = []
    windows = []
    for season, cutoff, holdout_end in WINDOWS:
        pre = _aggregate(session, dt.date(season, 1, 1), cutoff)
        holdout = _aggregate(
            session,
            cutoff + dt.timedelta(days=1),
            holdout_end,
        )
        blockers = collections.Counter()
        window_samples = []
        for (player_id, hand), actual in pre.items():
            future = holdout.get((player_id, hand))
            if actual["pa"] < 25:
                blockers["insufficient_pre_pa"] += 1
                continue
            if actual["swings"] < 50:
                blockers["insufficient_pre_swings"] += 1
                continue
            if future is None or future["pa"] < 20:
                blockers["insufficient_holdout_pa"] += 1
                continue
            sample = {
                "player_id": player_id,
                "season": season,
                "split": "vsR" if hand == "R" else "vsL",
                "cutoff": cutoff.isoformat(),
                "holdout_end": holdout_end.isoformat(),
                "holdout_pa": future["pa"],
                "holdout_k_rate": future["strikeouts"] / future["pa"],
                "pre_actual_k_rate": actual["strikeouts"] / actual["pa"],
                "pre_whiff_rate": actual["whiffs"] / actual["swings"],
                "pre_called_strike_rate":
                    actual["called_strikes"] / actual["pitches"],
                "pre_swinging_strike_rate":
                    actual["whiffs"] / actual["pitches"],
            }
            samples.append(sample)
            window_samples.append(sample)
        windows.append({
            "season": season,
            "cutoff": cutoff.isoformat(),
            "holdout_end": holdout_end.isoformat(),
            "sample_count": len(window_samples),
            "holdout_pa": sum(row["holdout_pa"] for row in window_samples),
            "blocker_counts": dict(blockers),
        })
    return samples, windows


def main() -> None:
    factory = _session_factory()
    session = factory()
    try:
        samples, windows = build_samples(session)
    finally:
        session.close()
    validation = evaluate_hitter_strikeout_skill_models(samples)
    bootstrap = bootstrap_hitter_strikeout_skill_differences(samples)
    print(json.dumps({
        **validation,
        "schema_version": "historical_shadow_hitter_strikeout_skill_audit_v1",
        "window_coverage": windows,
        "holdout_pa": sum(row["holdout_pa"] for row in samples),
        "sample_counts_by_season": dict(collections.Counter(
            row["season"] for row in samples
        )),
        "sample_counts_by_split": dict(collections.Counter(
            row["split"] for row in samples
        )),
        "terminal_event_policy": {
            "excluded_literal_values": ["", "nan", "none", "null"],
            "plate_appearance_identity": ["game_pk", "at_bat_number"],
        },
        "clustered_bootstrap": bootstrap,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
