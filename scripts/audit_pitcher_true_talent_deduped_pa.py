from __future__ import annotations

import csv
import datetime as dt
import json
import os
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mlb_app.database import PitchArsenal, PitcherAggregate, StatcastEvent, create_tables, get_engine, get_session
from mlb_app.db_utils import get_pitcher_aggregate
from mlb_app.matchup_generator import generate_matchups_for_date


COMPONENT_WEIGHTS_BASE = {
    "season_2023": 0.10,
    "season_2024": 0.20,
    "season_2025": 0.30,
    "season_current": 0.40,
}

COMPONENT_WEIGHTS_CURRENT_HEAVY = {
    "season_2023": 0.05,
    "season_2024": 0.15,
    "season_2025": 0.25,
    "season_current": 0.55,
}

MIN_PA_BY_COMPONENT = {
    "season_2023": 40,
    "season_2024": 40,
    "season_2025": 40,
    "season_current": 20,
}

METRICS = [
    "xba",
    "xwoba",
    "hard_hit_pct",
    "barrel_proxy_pct",
    "k_pct",
    "bb_pct",
    "hr_rate",
    "avg_velocity",
    "whiff_rate_proxy",
]

EXTREME_THRESHOLDS = {
    "xba": 0.030,
    "xwoba": 0.040,
    "hard_hit_pct": 0.080,
    "k_pct": 0.060,
    "bb_pct": 0.040,
    "hr_rate": 0.020,
    "avg_velocity": 2.0,
}


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


def _date_range(start: str, end: str) -> Iterable[str]:
    current = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)
    while current <= stop:
        yield current.isoformat()
        current += dt.timedelta(days=1)


def _distribution(values: List[Any]) -> Dict[str, Optional[float]]:
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return {
            "count": 0,
            "min": None,
            "median": None,
            "avg": None,
            "p75": None,
            "p90": None,
            "p95": None,
            "max": None,
        }

    def pct(p: float) -> float:
        if len(clean) == 1:
            return clean[0]
        idx = int(round((len(clean) - 1) * p))
        return clean[idx]

    return {
        "count": len(clean),
        "min": round(clean[0], 4),
        "median": round(float(median(clean)), 4),
        "avg": round(float(mean(clean)), 4),
        "p75": round(pct(0.75), 4),
        "p90": round(pct(0.90), 4),
        "p95": round(pct(0.95), 4),
        "max": round(clean[-1], 4),
    }


def _component_windows(target_date: dt.date) -> Dict[str, Tuple[dt.date, dt.date]]:
    return {
        "season_2023": (dt.date(2023, 3, 1), dt.date(2023, 12, 31)),
        "season_2024": (dt.date(2024, 3, 1), dt.date(2024, 12, 31)),
        "season_2025": (dt.date(2025, 3, 1), dt.date(2025, 12, 31)),
        "season_current": (dt.date(target_date.year, 3, 1), target_date),
    }


def _pitcher_rows(session, pitcher_id: int, start_date: dt.date, end_date: dt.date) -> List[StatcastEvent]:
    return (
        session.query(StatcastEvent)
        .filter(StatcastEvent.pitcher_id == pitcher_id)
        .filter(StatcastEvent.game_date >= start_date)
        .filter(StatcastEvent.game_date <= end_date)
        .all()
    )


def _is_swing(row: StatcastEvent) -> bool:
    return row.description in {
        "swinging_strike",
        "swinging_strike_blocked",
        "foul_tip",
        "hit_into_play",
        "foul",
        "foul_bunt",
        "foul_pitchout",
        "missed_bunt",
    }


def _is_whiff(row: StatcastEvent) -> bool:
    return row.description in {
        "swinging_strike",
        "swinging_strike_blocked",
        "foul_tip",
    }


def _is_hard_hit(row: StatcastEvent) -> bool:
    ev = _safe_float(row.launch_speed)
    return ev is not None and ev >= 95.0


def _is_barrel_proxy(row: StatcastEvent) -> bool:
    ev = _safe_float(row.launch_speed)
    la = _safe_float(row.launch_angle)
    return ev is not None and la is not None and ev >= 98.0 and 26.0 <= la <= 30.0


def _terminal_pa_key(row: StatcastEvent) -> Optional[Tuple[int, int]]:
    if row.game_pk is None or row.at_bat_number is None:
        return None
    return (int(row.game_pk), int(row.at_bat_number))


def _terminal_pa_key_strong(row: StatcastEvent) -> Optional[Tuple[int, int, int]]:
    if row.game_pk is None or row.inning is None or row.at_bat_number is None:
        return None
    return (int(row.game_pk), int(row.inning), int(row.at_bat_number))


def _dedupe_terminal_pas(rows: List[StatcastEvent]) -> Tuple[List[StatcastEvent], List[Dict[str, Any]]]:
    grouped: Dict[Tuple[int, int], List[StatcastEvent]] = defaultdict(list)

    for row in rows:
        if row.events is None:
            continue
        key = _terminal_pa_key(row)
        if key is None:
            continue
        grouped[key].append(row)

    deduped: List[StatcastEvent] = []
    duplicates: List[Dict[str, Any]] = []

    for key, group in grouped.items():
        group_sorted = sorted(
            group,
            key=lambda row: (
                row.pitch_number if row.pitch_number is not None else -1,
                row.id if row.id is not None else -1,
            ),
            reverse=True,
        )
        winner = group_sorted[0]
        deduped.append(winner)

        if len(group) > 1:
            strong_keys = sorted(
                {
                    str(_terminal_pa_key_strong(row))
                    for row in group
                    if _terminal_pa_key_strong(row) is not None
                }
            )
            duplicates.append(
                {
                    "game_pk": key[0],
                    "at_bat_number": key[1],
                    "duplicate_rows": len(group),
                    "chosen_row_id": winner.id,
                    "chosen_pitch_number": winner.pitch_number,
                    "chosen_events": winner.events,
                    "strong_keys": "|".join(strong_keys),
                    "row_ids": ",".join(str(row.id) for row in group_sorted),
                    "events_seen": ",".join(str(row.events) for row in group_sorted),
                    "pitch_numbers_seen": ",".join(str(row.pitch_number) for row in group_sorted),
                }
            )

    return deduped, duplicates


def _component_from_rows(rows: List[StatcastEvent], component: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    terminal_rows = [row for row in rows if row.events is not None]
    deduped_pas, duplicates = _dedupe_terminal_pas(rows)

    contact_rows = [
        row for row in rows
        if row.launch_speed is not None
        or row.launch_angle is not None
        or row.estimated_ba_using_speedangle is not None
        or row.estimated_woba_using_speedangle is not None
    ]

    xba_values = [
        _safe_float(row.estimated_ba_using_speedangle)
        for row in contact_rows
        if _safe_float(row.estimated_ba_using_speedangle) is not None
    ]
    xwoba_values = [
        _safe_float(row.estimated_woba_using_speedangle)
        for row in contact_rows
        if _safe_float(row.estimated_woba_using_speedangle) is not None
    ]
    velocity_values = [
        _safe_float(row.release_speed)
        for row in rows
        if _safe_float(row.release_speed) is not None
    ]

    strikeouts = sum(1 for row in deduped_pas if row.events in {"strikeout", "strikeout_double_play"})
    walks = sum(1 for row in deduped_pas if row.events in {"walk", "intent_walk"})
    homers = sum(1 for row in deduped_pas if row.events == "home_run")

    hard_hits = sum(1 for row in contact_rows if _is_hard_hit(row))
    barrels = sum(1 for row in contact_rows if _is_barrel_proxy(row))
    swings = sum(1 for row in rows if _is_swing(row))
    whiffs = sum(1 for row in rows if _is_whiff(row))

    pa = len(deduped_pas)
    duplicate_terminal_rows_before_dedup = len(terminal_rows) - pa

    component_profile = {
        "component": component,
        "pitch_count": len(rows),
        "terminal_rows_before_dedup": len(terminal_rows),
        "deduped_pa": pa,
        "duplicate_terminal_rows_before_dedup": duplicate_terminal_rows_before_dedup,
        "duplicate_pa_count": len(duplicates),
        "contact_quality_rows": len(contact_rows),
        "xba_rows": len(xba_values),
        "xwoba_rows": len(xwoba_values),
        "velocity_rows": len(velocity_values),
        "strikeout_pas": strikeouts,
        "walk_pas": walks,
        "home_run_pas": homers,
        "xba": round(float(mean(xba_values)), 4) if xba_values else None,
        "xwoba": round(float(mean(xwoba_values)), 4) if xwoba_values else None,
        "hard_hit_pct": round(hard_hits / len(contact_rows), 4) if contact_rows else None,
        "barrel_proxy_pct": round(barrels / len(contact_rows), 4) if contact_rows else None,
        "k_pct": round(strikeouts / pa, 4) if pa else None,
        "bb_pct": round(walks / pa, 4) if pa else None,
        "hr_rate": round(homers / pa, 4) if pa else None,
        "avg_velocity": round(float(mean(velocity_values)), 4) if velocity_values else None,
        "whiff_rate_proxy": round(whiffs / swings, 4) if swings else None,
    }

    return component_profile, duplicates


def _weighted_profile(components: List[Dict[str, Any]], weights: Dict[str, float]) -> Dict[str, Any]:
    usable = []
    for component in components:
        name = component["component"]
        pa = _safe_int(component.get("deduped_pa")) or 0
        min_pa = MIN_PA_BY_COMPONENT.get(name, 0)
        if weights.get(name, 0.0) <= 0:
            continue
        if pa < min_pa:
            continue
        if component.get("xwoba") is None and component.get("k_pct") is None:
            continue
        usable.append(component)

    total_weight = sum(weights.get(component["component"], 0.0) for component in usable)

    out: Dict[str, Any] = {
        "usable_component_count": len(usable),
        "used_components": ",".join(component["component"] for component in usable),
        "component_weight_sum": round(total_weight, 4),
    }

    if total_weight <= 0:
        for metric in METRICS:
            out[metric] = None
        out["deduped_pa"] = 0
        out["pitch_count"] = 0
        return out

    for metric in METRICS:
        numerator = 0.0
        denominator = 0.0
        for component in usable:
            value = _safe_float(component.get(metric))
            if value is None:
                continue
            weight = weights.get(component["component"], 0.0)
            numerator += value * weight
            denominator += weight
        out[metric] = round(numerator / denominator, 4) if denominator else None

    out["deduped_pa"] = sum(_safe_int(component.get("deduped_pa")) or 0 for component in usable)
    out["pitch_count"] = sum(_safe_int(component.get("pitch_count")) or 0 for component in usable)
    out["terminal_rows_before_dedup"] = sum(_safe_int(component.get("terminal_rows_before_dedup")) or 0 for component in usable)
    out["duplicate_terminal_rows_before_dedup"] = sum(
        _safe_int(component.get("duplicate_terminal_rows_before_dedup")) or 0 for component in usable
    )

    return out


def _current_recent_profile(session, pitcher_id: int) -> Dict[str, Any]:
    agg = get_pitcher_aggregate(session, pitcher_id, "90d")
    if not agg:
        return {
            "profile_present": False,
            "xba": None,
            "xwoba": None,
            "hard_hit_pct": None,
            "k_pct": None,
            "bb_pct": None,
            "avg_velocity": None,
        }

    return {
        "profile_present": True,
        "xba": agg.xba,
        "xwoba": agg.xwoba,
        "hard_hit_pct": agg.hard_hit_pct,
        "k_pct": agg.k_pct,
        "bb_pct": agg.bb_pct,
        "avg_velocity": agg.avg_velocity,
        "avg_spin_rate": agg.avg_spin_rate,
        "avg_horiz_break": agg.avg_horiz_break,
        "avg_vert_break": agg.avg_vert_break,
        "avg_release_extension": agg.avg_release_extension,
    }


def _bounded_modifier(base: Optional[float], current: Optional[float], scale: float, cap: float) -> Optional[float]:
    if base is None:
        return None
    if current is None:
        return round(base, 4)
    correction = max(-cap, min(cap, (current - base) * scale))
    return round(base + correction, 4)


def _season_plus_stuff_modifier(base: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    out["profile_family"] = "season_plus_stuff_modifier_deduped_profile"
    out["xba"] = _bounded_modifier(_safe_float(base.get("xba")), _safe_float(current.get("xba")), 0.20, 0.020)
    out["xwoba"] = _bounded_modifier(_safe_float(base.get("xwoba")), _safe_float(current.get("xwoba")), 0.20, 0.025)
    out["hard_hit_pct"] = _bounded_modifier(_safe_float(base.get("hard_hit_pct")), _safe_float(current.get("hard_hit_pct")), 0.20, 0.040)
    out["k_pct"] = _bounded_modifier(_safe_float(base.get("k_pct")), _safe_float(current.get("k_pct")), 0.20, 0.030)
    out["bb_pct"] = _bounded_modifier(_safe_float(base.get("bb_pct")), _safe_float(current.get("bb_pct")), 0.20, 0.020)
    out["avg_velocity"] = _bounded_modifier(_safe_float(base.get("avg_velocity")), _safe_float(current.get("avg_velocity")), 0.50, 1.000)
    return out


def _arsenal_summary(session, pitcher_id: int, season: int) -> Dict[str, Any]:
    rows = (
        session.query(PitchArsenal)
        .filter(PitchArsenal.pitcher_id == pitcher_id)
        .filter(PitchArsenal.season == season)
        .all()
    )

    if not rows:
        return {
            "arsenal_rows": 0,
            "arsenal_pitch_count": 0,
            "arsenal_weighted_rv_per_100": None,
            "arsenal_weighted_xwoba": None,
            "arsenal_weighted_hard_hit_pct": None,
            "primary_pitch_usage": None,
        }

    def weighted(metric: str) -> Optional[float]:
        numerator = 0.0
        denominator = 0.0
        for row in rows:
            value = _safe_float(getattr(row, metric, None))
            weight = _safe_float(row.usage_pct)
            if value is None or weight is None:
                continue
            numerator += value * weight
            denominator += weight
        return round(numerator / denominator, 4) if denominator else None

    return {
        "arsenal_rows": len(rows),
        "arsenal_pitch_count": sum(_safe_int(row.pitch_count) or 0 for row in rows),
        "arsenal_weighted_rv_per_100": weighted("rv_per_100"),
        "arsenal_weighted_xwoba": weighted("xwoba"),
        "arsenal_weighted_hard_hit_pct": weighted("hard_hit_pct"),
        "primary_pitch_usage": round(max((_safe_float(row.usage_pct) or 0.0) for row in rows), 4),
    }


def _profile_deltas(candidate: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for metric in METRICS:
        candidate_value = _safe_float(candidate.get(metric))
        current_value = _safe_float(current.get(metric))
        out[f"{metric}_delta_vs_current"] = (
            round(candidate_value - current_value, 4)
            if candidate_value is not None and current_value is not None
            else None
        )
    return out


def _is_extreme(row: Dict[str, Any]) -> bool:
    for metric, threshold in EXTREME_THRESHOLDS.items():
        value = _safe_float(row.get(f"{metric}_delta_vs_current"))
        if value is not None and abs(value) > threshold:
            return True
    return False


def _build_pitcher_profiles(
    session,
    pitcher_id: int,
    pitcher_name: Optional[str],
    target_date: dt.date,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    windows = _component_windows(target_date)
    components: List[Dict[str, Any]] = []
    duplicate_rows: List[Dict[str, Any]] = []

    for component, (start_date, end_date) in windows.items():
        rows = _pitcher_rows(session, pitcher_id, start_date, end_date)
        component_profile, duplicates = _component_from_rows(rows, component)
        component_profile["start_date"] = start_date.isoformat()
        component_profile["end_date"] = end_date.isoformat()
        components.append(component_profile)

        for dup in duplicates:
            duplicate_rows.append(
                {
                    "pitcher_id": pitcher_id,
                    "pitcher_name": pitcher_name,
                    "component": component,
                    **dup,
                }
            )

    current_recent = _current_recent_profile(session, pitcher_id)
    season_only = _weighted_profile(components, COMPONENT_WEIGHTS_BASE)
    season_current_heavy = _weighted_profile(components, COMPONENT_WEIGHTS_CURRENT_HEAVY)
    expected_stat = _weighted_profile(components, COMPONENT_WEIGHTS_CURRENT_HEAVY)
    stuff_modifier = _season_plus_stuff_modifier(season_current_heavy, current_recent)

    base = {
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "target_date": target_date.isoformat(),
        "component_summary": {
            component["component"]: {
                "deduped_pa": component.get("deduped_pa"),
                "pitch_count": component.get("pitch_count"),
                "duplicate_terminal_rows_before_dedup": component.get("duplicate_terminal_rows_before_dedup"),
                "k_pct": component.get("k_pct"),
                "bb_pct": component.get("bb_pct"),
                "hr_rate": component.get("hr_rate"),
                "xwoba": component.get("xwoba"),
                "xba": component.get("xba"),
                "avg_velocity": component.get("avg_velocity"),
            }
            for component in components
        },
    }

    profiles = {
        "current_recent_profile": {
            **base,
            **current_recent,
            "profile_family": "current_recent_profile",
            "usable_component_count": None,
            "used_components": "90d_pitcher_aggregate",
        },
        "season_only_deduped_profile": {
            **base,
            **season_only,
            "profile_family": "season_only_deduped_profile",
        },
        "season_current_heavy_deduped_profile": {
            **base,
            **season_current_heavy,
            "profile_family": "season_current_heavy_deduped_profile",
        },
        "season_plus_stuff_modifier_deduped_profile": {
            **base,
            **stuff_modifier,
        },
        "expected_stat_deduped_profile": {
            **base,
            **expected_stat,
            "profile_family": "expected_stat_deduped_profile",
        },
    }

    return profiles, components, duplicate_rows


def _collect_matchup_pitchers(session, start: str, end: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()

    for date_str in _date_range(start, end):
        for matchup in generate_matchups_for_date(session, date_str):
            for side in ("away", "home"):
                pitcher_id = _safe_int(matchup.get(f"{side}_pitcher_id"))
                if not pitcher_id:
                    continue

                key = (date_str, pitcher_id, side, matchup.get("game_pk"))
                if key in seen:
                    continue
                seen.add(key)

                rows.append(
                    {
                        "date": date_str,
                        "game_pk": matchup.get("game_pk"),
                        "side": side,
                        "team_id": matchup.get(f"{side}_team_id"),
                        "team_name": matchup.get(f"{side}_team_name"),
                        "pitcher_id": pitcher_id,
                        "pitcher_name": matchup.get(f"{side}_pitcher_name"),
                    }
                )

    return rows


def _summarize_profile_family(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    evaluated = len(rows)
    extreme_count = sum(1 for row in rows if row.get("extreme_delta_vs_current"))

    return {
        "pitchers_evaluated": evaluated,
        "profiles_with_xba": sum(1 for row in rows if row.get("xba") is not None),
        "profiles_with_xwoba": sum(1 for row in rows if row.get("xwoba") is not None),
        "profiles_with_hard_hit": sum(1 for row in rows if row.get("hard_hit_pct") is not None),
        "profiles_with_velocity": sum(1 for row in rows if row.get("avg_velocity") is not None),
        "profiles_with_k_pct": sum(1 for row in rows if row.get("k_pct") is not None),
        "profiles_with_bb_pct": sum(1 for row in rows if row.get("bb_pct") is not None),
        "profiles_with_hr_rate": sum(1 for row in rows if row.get("hr_rate") is not None),
        "extreme_delta_count": extreme_count,
        "extreme_delta_rate": round(extreme_count / evaluated, 4) if evaluated else None,
        "delta_distributions": {
            metric: _distribution([row.get(f"{metric}_delta_vs_current") for row in rows])
            for metric in METRICS
        },
        "metric_distributions": {
            metric: _distribution([row.get(metric) for row in rows])
            for metric in METRICS
        },
        "sample_size_distributions": {
            "deduped_pa": _distribution([row.get("deduped_pa") for row in rows]),
            "pitch_count": _distribution([row.get("pitch_count") for row in rows]),
            "usable_component_count": _distribution([row.get("usable_component_count") for row in rows]),
            "duplicate_terminal_rows_before_dedup": _distribution(
                [row.get("duplicate_terminal_rows_before_dedup") for row in rows]
            ),
        },
    }


def _validation_diagnostics(component_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    k_values = [_safe_float(row.get("k_pct")) for row in component_rows if _safe_float(row.get("k_pct")) is not None]
    bb_values = [_safe_float(row.get("bb_pct")) for row in component_rows if _safe_float(row.get("bb_pct")) is not None]
    hr_values = [_safe_float(row.get("hr_rate")) for row in component_rows if _safe_float(row.get("hr_rate")) is not None]

    return {
        "components_with_duplicate_terminal_rows_before_dedup": sum(
            1 for row in component_rows if (_safe_int(row.get("duplicate_terminal_rows_before_dedup")) or 0) > 0
        ),
        "duplicate_terminal_row_distribution": _distribution(
            [row.get("duplicate_terminal_rows_before_dedup") for row in component_rows]
        ),
        "invalid_k_rate_count_after_dedup": sum(1 for value in k_values if value < 0 or value > 1),
        "invalid_bb_rate_count_after_dedup": sum(1 for value in bb_values if value < 0 or value > 1),
        "invalid_hr_rate_count_after_dedup": sum(1 for value in hr_values if value < 0 or value > 1),
        "max_k_pct_after_dedup": round(max(k_values), 4) if k_values else None,
        "max_bb_pct_after_dedup": round(max(bb_values), 4) if bb_values else None,
        "max_hr_rate_after_dedup": round(max(hr_values), 4) if hr_values else None,
    }


def _diagnose(summary_by_family: Dict[str, Dict[str, Any]], validation: Dict[str, Any]) -> Dict[str, Any]:
    invalid_rates = (
        validation["invalid_k_rate_count_after_dedup"]
        + validation["invalid_bb_rate_count_after_dedup"]
        + validation["invalid_hr_rate_count_after_dedup"]
    )

    if invalid_rates > 0:
        return {
            "diagnosis": "deduped_rates_still_invalid",
            "recommended_next_step": "K%/BB%/HR rates are still invalid after dedupe. Use stronger PA key or deeper duplicate-row audit.",
            "recommended_profile_candidates": [],
        }

    season = summary_by_family.get("season_only_deduped_profile", {})
    stuff = summary_by_family.get("season_plus_stuff_modifier_deduped_profile", {})

    season_extreme = season.get("extreme_delta_rate")
    stuff_extreme = stuff.get("extreme_delta_rate")

    if season_extreme is not None and season_extreme <= 0.35:
        return {
            "diagnosis": "deduped_pitcher_pa_construction_fixed_rates",
            "recommended_next_step": "Deduped PA construction fixed rate validity and produced usable historical pitcher candidates. Move to season weighting audit.",
            "recommended_profile_candidates": [
                "season_only_deduped_profile",
                "season_current_heavy_deduped_profile",
                "season_plus_stuff_modifier_deduped_profile",
            ],
        }

    if stuff_extreme is not None and season_extreme is not None and stuff_extreme < season_extreme:
        return {
            "diagnosis": "deduped_rates_valid_but_profiles_still_stale",
            "recommended_next_step": "Rates are valid, but historical profiles still differ from current form. Move to bounded stuff/current modifier grid.",
            "recommended_profile_candidates": ["season_plus_stuff_modifier_deduped_profile"],
        }

    return {
        "diagnosis": "historical_pitcher_profiles_need_role_segmentation",
        "recommended_next_step": "Rates are valid, but historical profile deltas remain high. Move to role-aware historical pitcher profile audit.",
        "recommended_profile_candidates": [],
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row.keys() if not isinstance(row.get(key), (dict, list))})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    start = os.getenv("BACKTEST_START", "2026-04-20")
    end = os.getenv("BACKTEST_END", "2026-05-03")
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")

    engine = get_engine(database_url)
    create_tables(engine)
    SessionLocal = get_session(engine)

    profile_rows: List[Dict[str, Any]] = []
    matchup_rows: List[Dict[str, Any]] = []
    component_rows: List[Dict[str, Any]] = []
    duplicate_rows: List[Dict[str, Any]] = []

    with SessionLocal() as session:
        matchup_pitchers = _collect_matchup_pitchers(session, start, end)
        matchup_rows.extend(matchup_pitchers)

        for matchup_pitcher in matchup_pitchers:
            target_date = dt.date.fromisoformat(matchup_pitcher["date"])
            pitcher_id = int(matchup_pitcher["pitcher_id"])
            pitcher_name = matchup_pitcher.get("pitcher_name")

            profiles, components, duplicates = _build_pitcher_profiles(
                session=session,
                pitcher_id=pitcher_id,
                pitcher_name=pitcher_name,
                target_date=target_date,
            )

            current_profile = profiles["current_recent_profile"]
            arsenal = _arsenal_summary(session, pitcher_id, target_date.year)

            for component in components:
                component_rows.append({**matchup_pitcher, **component})

            for duplicate in duplicates:
                duplicate_rows.append({**matchup_pitcher, **duplicate})

            for family, profile in profiles.items():
                row = {
                    **matchup_pitcher,
                    **profile,
                    **arsenal,
                    **_profile_deltas(profile, current_profile),
                }
                row["extreme_delta_vs_current"] = _is_extreme(row)
                profile_rows.append(row)

    rows_by_family = defaultdict(list)
    for row in profile_rows:
        rows_by_family[row.get("profile_family")].append(row)

    summary_by_family = {
        family: _summarize_profile_family(rows)
        for family, rows in rows_by_family.items()
    }

    validation = _validation_diagnostics(component_rows)
    diagnosis = _diagnose(summary_by_family, validation)

    payload = {
        "start": start,
        "end": end,
        "database_url": database_url,
        "matchup_pitchers": len(matchup_rows),
        "profile_families": sorted(summary_by_family.keys()),
        "summary_by_family": summary_by_family,
        "validation_diagnostics": validation,
        "duplicate_pa_rows": len(duplicate_rows),
        "arsenal_coverage": {
            "rows_with_arsenal": sum(1 for row in profile_rows if row.get("arsenal_rows", 0) > 0),
            "arsenal_weighted_xwoba_distribution": _distribution([row.get("arsenal_weighted_xwoba") for row in profile_rows]),
            "arsenal_weighted_rv_per_100_distribution": _distribution([row.get("arsenal_weighted_rv_per_100") for row in profile_rows]),
            "arsenal_weighted_hard_hit_pct_distribution": _distribution([row.get("arsenal_weighted_hard_hit_pct") for row in profile_rows]),
        },
        **diagnosis,
        "recommendation": (
            "Audit only. Do not wire deduped historical pitcher profiles into production until profile "
            "candidate calibration and simulation backtests support the change."
        ),
    }

    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"pitcher_true_talent_deduped_pa_{start}_to_{end}"

    json_path = output_dir / f"{prefix}.json"
    pitchers_csv_path = output_dir / f"{prefix}_pitchers.csv"
    components_csv_path = output_dir / f"{prefix}_components.csv"
    duplicate_csv_path = output_dir / f"{prefix}_duplicate_pas.csv"

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    _write_csv(pitchers_csv_path, profile_rows)
    _write_csv(components_csv_path, component_rows)
    _write_csv(duplicate_csv_path, duplicate_rows)

    print(json.dumps(
        {
            "diagnosis": payload["diagnosis"],
            "recommended_next_step": payload["recommended_next_step"],
            "recommended_profile_candidates": payload["recommended_profile_candidates"],
            "matchup_pitchers": payload["matchup_pitchers"],
            "validation_diagnostics": payload["validation_diagnostics"],
            "duplicate_pa_rows": payload["duplicate_pa_rows"],
            "summary_by_family": {
                family: {
                    "pitchers_evaluated": summary.get("pitchers_evaluated"),
                    "profiles_with_k_pct": summary.get("profiles_with_k_pct"),
                    "profiles_with_bb_pct": summary.get("profiles_with_bb_pct"),
                    "profiles_with_hr_rate": summary.get("profiles_with_hr_rate"),
                    "profiles_with_xwoba": summary.get("profiles_with_xwoba"),
                    "profiles_with_velocity": summary.get("profiles_with_velocity"),
                    "extreme_delta_count": summary.get("extreme_delta_count"),
                    "extreme_delta_rate": summary.get("extreme_delta_rate"),
                    "sample_size_distributions": summary.get("sample_size_distributions"),
                    "delta_distributions": {
                        "xwoba": summary.get("delta_distributions", {}).get("xwoba"),
                        "k_pct": summary.get("delta_distributions", {}).get("k_pct"),
                        "bb_pct": summary.get("delta_distributions", {}).get("bb_pct"),
                        "avg_velocity": summary.get("delta_distributions", {}).get("avg_velocity"),
                    },
                }
                for family, summary in summary_by_family.items()
            },
            "recommendation": payload["recommendation"],
        },
        indent=2,
        sort_keys=True,
        default=str,
    ))

    print(f"Wrote {json_path}")
    print(f"Wrote {pitchers_csv_path}")
    print(f"Wrote {components_csv_path}")
    print(f"Wrote {duplicate_csv_path}")


if __name__ == "__main__":
    main()
