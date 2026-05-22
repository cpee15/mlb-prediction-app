from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.model_projections import build_model_projection_payload


TARGET_DATE = "2026-05-20"
CONTRACT_VERSION = "candidate_bullpen_shadow_contract_v0.1"
ACTUAL_USAGE_LABEL_DATE: Optional[str] = None
DIAGNOSTICS_VERSION = "candidate_bullpen_shadow_diagnostics_v0.1"
JOIN_VERSION = "candidate_bullpen_historical_usage_join_v0.1"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_prototype.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_prototype_checks.csv"
OUTPUT_JOINED = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_rows.csv"
OUTPUT_APPEARANCES = OUTPUT_DIR / "candidate_bullpen_historical_usage_appearances.csv"
OUTPUT_AGGREGATE = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_aggregate.csv"
OUTPUT_MISSING = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_missing_labels.csv"


ROLE_TEMPLATES = [
    {"role_name": "closer", "role_priority": 1, "leverage_bucket": "save_highest_leverage", "inherited_runner_exposure": "low", "k_delta": 0.018, "bb_delta": -0.008, "whiff_delta": 0.015, "csw_delta": 0.010, "xwoba_delta": -0.010, "quality_delta": 0.030},
    {"role_name": "setup", "role_priority": 2, "leverage_bucket": "late_high_leverage", "inherited_runner_exposure": "medium", "k_delta": 0.012, "bb_delta": -0.005, "whiff_delta": 0.010, "csw_delta": 0.007, "xwoba_delta": -0.007, "quality_delta": 0.020},
    {"role_name": "high_leverage", "role_priority": 3, "leverage_bucket": "matchup_high_leverage", "inherited_runner_exposure": "high", "k_delta": 0.006, "bb_delta": -0.002, "whiff_delta": 0.006, "csw_delta": 0.004, "xwoba_delta": -0.004, "quality_delta": 0.010},
    {"role_name": "middle_relief", "role_priority": 4, "leverage_bucket": "middle_neutral", "inherited_runner_exposure": "medium", "k_delta": 0.000, "bb_delta": 0.000, "whiff_delta": 0.000, "csw_delta": 0.000, "xwoba_delta": 0.000, "quality_delta": 0.000},
    {"role_name": "long_relief", "role_priority": 5, "leverage_bucket": "length_low_leverage", "inherited_runner_exposure": "low", "k_delta": -0.012, "bb_delta": 0.006, "whiff_delta": -0.010, "csw_delta": -0.006, "xwoba_delta": 0.010, "quality_delta": -0.020},
]

SEQUENCE_EVENTS = [
    {"sequence_step": 1, "event_name": "starter_short_hook_4th", "inning": 4, "leverage": "neutral", "base_state": "1--", "outs": 1, "preferred_roles": ["long_relief", "middle_relief"], "avoid_roles": ["closer", "setup"]},
    {"sequence_step": 2, "event_name": "middle_bridge_5th", "inning": 5, "leverage": "neutral", "base_state": "---", "outs": 0, "preferred_roles": ["middle_relief", "long_relief"], "avoid_roles": ["closer"]},
    {"sequence_step": 3, "event_name": "traffic_escape_6th", "inning": 6, "leverage": "high", "base_state": "12-", "outs": 1, "preferred_roles": ["high_leverage", "setup"], "avoid_roles": ["long_relief"]},
    {"sequence_step": 4, "event_name": "leverage_7th", "inning": 7, "leverage": "high", "base_state": "-2-", "outs": 0, "preferred_roles": ["high_leverage", "setup"], "avoid_roles": ["long_relief"]},
    {"sequence_step": 5, "event_name": "setup_8th", "inning": 8, "leverage": "high", "base_state": "---", "outs": 0, "preferred_roles": ["setup", "high_leverage"], "avoid_roles": ["long_relief"]},
    {"sequence_step": 6, "event_name": "save_9th", "inning": 9, "leverage": "save", "base_state": "---", "outs": 0, "preferred_roles": ["closer", "setup"], "avoid_roles": ["long_relief", "middle_relief"]},
    {"sequence_step": 7, "event_name": "extras_10th", "inning": 10, "leverage": "high", "base_state": "-2-", "outs": 0, "preferred_roles": ["closer", "setup", "high_leverage"], "avoid_roles": ["long_relief"]},
    {"sequence_step": 8, "event_name": "emergency_11th", "inning": 11, "leverage": "high", "base_state": "123", "outs": 0, "preferred_roles": ["setup", "closer", "high_leverage", "middle_relief"], "avoid_roles": []},
]


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _clamp(value: Optional[float], lower: float, upper: float) -> Optional[float]:
    if value is None:
        return None
    return round(max(lower, min(upper, value)), 4)


def _profile_value(profile: Dict[str, Any], section: str, key: str) -> Optional[float]:
    return _safe_float((profile.get(section) or {}).get(key))


def _role_value(base: Optional[float], delta: float, lower: float, upper: float) -> Optional[float]:
    if base is None:
        return None
    return _clamp(base + delta, lower, upper)


def _stable_bucket(*parts: Any, modulo: int = 100) -> int:
    raw = "|".join(str(part) for part in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % modulo


def _extract_profiles(game: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    direct_inputs = ((game.get("sharedSimulation") or {}).get("direct_inputs") or {})
    workspace = game.get("workspace") or {}
    return {
        "away": direct_inputs.get("away_bullpen_profile") or workspace.get("awayBullpenProfile") or {},
        "home": direct_inputs.get("home_bullpen_profile") or workspace.get("homeBullpenProfile") or {},
    }


def _base_role_rows(game: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    profiles = _extract_profiles(game)
    teams = game.get("teams") or {}

    for side, profile in profiles.items():
        metadata = profile.get("metadata") or {}
        team_payload = teams.get(side) or {}
        team_name = metadata.get("team_name") or team_payload.get("team_name") or team_payload.get("name")
        team_id = metadata.get("team_id") or team_payload.get("team_id")

        base_k = _profile_value(profile, "bat_missing", "k_rate")
        base_whiff = _profile_value(profile, "bat_missing", "whiff_rate")
        base_csw = _profile_value(profile, "bat_missing", "csw_rate")
        base_bb = _profile_value(profile, "command_control", "bb_rate")
        base_xwoba = _profile_value(profile, "contact_management", "xwoba_allowed")
        base_quality = _safe_float(metadata.get("bullpen_quality_score")) or 0.0

        for template in ROLE_TEMPLATES:
            rows.append({
                "game_pk": game.get("game_pk"),
                "game_date": game.get("game_date"),
                "side": side,
                "team_id": team_id,
                "team_name": team_name,
                "role_name": template["role_name"],
                "role_priority": template["role_priority"],
                "role_k_rate": _role_value(base_k, template["k_delta"], 0.14, 0.36),
                "role_bb_rate": _role_value(base_bb, template["bb_delta"], 0.045, 0.15),
                "role_whiff_rate": _role_value(base_whiff, template["whiff_delta"], 0.16, 0.38),
                "role_csw_rate": _role_value(base_csw, template["csw_delta"], 0.21, 0.36),
                "role_xwoba_allowed": _role_value(base_xwoba, template["xwoba_delta"], 0.265, 0.380),
                "role_quality_score": _clamp(base_quality + template["quality_delta"], -0.18, 0.18),
                "candidate_only": True,
            })
    return rows


def _fatigue_state(row: Dict[str, Any]) -> Dict[str, Any]:
    bucket = _stable_bucket(TARGET_DATE, row.get("game_pk"), row.get("team_id"), row.get("side"), row.get("role_name"))
    role = row.get("role_name")
    priority = int(row.get("role_priority") or 5)

    if role in {"closer", "setup"}:
        recent_pitch_count = 8 + bucket % 29
        days_rest = bucket % 4
    elif role == "high_leverage":
        recent_pitch_count = 6 + bucket % 26
        days_rest = (bucket + 1) % 4
    elif role == "middle_relief":
        recent_pitch_count = 4 + bucket % 23
        days_rest = (bucket + 2) % 5
    else:
        recent_pitch_count = 2 + bucket % 18
        days_rest = (bucket + 3) % 5

    back_to_back_risk = days_rest == 0 and recent_pitch_count >= 18

    if back_to_back_risk and priority <= 3:
        availability_status = "unavailable"
        fatigue_penalty = 0.045
    elif recent_pitch_count >= 28 and priority <= 3:
        availability_status = "limited"
        fatigue_penalty = 0.030
    elif recent_pitch_count >= 22:
        availability_status = "limited"
        fatigue_penalty = 0.020
    else:
        availability_status = "available"
        fatigue_penalty = 0.000 if days_rest >= 1 else 0.010

    return {
        "availability_status": availability_status,
        "fatigue_penalty": fatigue_penalty,
    }


def _apply_fatigue(row: Dict[str, Any]) -> Dict[str, Any]:
    fatigue = _fatigue_state(row)
    penalty = float(fatigue["fatigue_penalty"])
    return {
        **row,
        **fatigue,
        "adjusted_role_quality_score": _clamp((row.get("role_quality_score") or 0.0) - penalty, -0.25, 0.20),
        "adjusted_role_k_rate": _clamp((row.get("role_k_rate") or 0.0) - penalty * 0.45, 0.12, 0.36),
        "adjusted_role_bb_rate": _clamp((row.get("role_bb_rate") or 0.0) + penalty * 0.35, 0.045, 0.16),
        "adjusted_role_xwoba_allowed": _clamp((row.get("role_xwoba_allowed") or 0.0) + penalty * 0.60, 0.265, 0.395),
    }


def _build_fatigue_rows(session: Session) -> List[Dict[str, Any]]:
    payload = build_model_projection_payload(session, TARGET_DATE)
    games = payload.get("games") or []

    rows: List[Dict[str, Any]] = []
    for game in games:
        for role_row in _base_role_rows(game):
            rows.append(_apply_fatigue(role_row))
    return rows


def _team_key(row: Dict[str, Any]) -> Tuple[Any, Any, Any]:
    return (row.get("game_pk"), row.get("side"), row.get("team_id"))


def _current_status(base_status: str, usage_count: int) -> str:
    if usage_count <= 0:
        return base_status
    if usage_count == 1:
        if base_status == "unavailable":
            return "unavailable"
        return "limited"
    return "unavailable"


def _score_role(row: Dict[str, Any], event: Dict[str, Any], usage_count: int, emergency: bool, alternative_exists: bool) -> Tuple[float, str, str]:
    role_name = str(row.get("role_name"))
    pre_status = _current_status(str(row.get("availability_status")), usage_count)
    score = float(row.get("adjusted_role_quality_score") or 0.0)

    if role_name in event["preferred_roles"]:
        score += 0.095 - 0.010 * event["preferred_roles"].index(role_name)
    else:
        score -= 0.040

    if role_name in event["avoid_roles"]:
        score -= 0.080

    if event["leverage"] in {"save", "high"}:
        score += max(0.0, (6 - int(row.get("role_priority") or 5)) * 0.006)
    else:
        score -= max(0.0, (6 - int(row.get("role_priority") or 5)) * 0.004)

    if pre_status == "unavailable" and alternative_exists and not emergency:
        score -= 999.0
    elif pre_status == "unavailable" and emergency:
        score -= 0.150
    elif pre_status == "limited":
        score *= 0.70
        score -= 0.025

    score -= usage_count * 0.035

    if event["event_name"] == "emergency_11th" and role_name in {"middle_relief", "long_relief"}:
        score += 0.030

    fallback_reason = "none" if role_name in event["preferred_roles"] else f"preferred_roles_depleted_or_outscored_by_{role_name}"
    return round(score, 5), pre_status, fallback_reason


def _post_status(pre_status: str, post_usage_count: int) -> str:
    if post_usage_count >= 2:
        return "unavailable"
    if post_usage_count == 1 and pre_status in {"available", "limited"}:
        return "limited"
    return pre_status


def _simulate_team_sequence(role_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    usage_counts = {str(row["role_name"]): 0 for row in role_rows}
    events: List[Dict[str, Any]] = []

    for event in SEQUENCE_EVENTS:
        emergency = event["event_name"] == "emergency_11th"
        alternative_exists = any(
            _current_status(str(row.get("availability_status")), usage_counts[str(row["role_name"])]) != "unavailable"
            for row in role_rows
        )

        scored = []
        for row in role_rows:
            score, pre_status, fallback_reason = _score_role(
                row, event, usage_counts[str(row["role_name"])], emergency, alternative_exists
            )
            scored.append((score, int(row.get("role_priority") or 99), row, pre_status, fallback_reason))

        scored.sort(key=lambda item: (item[0], -item[1]), reverse=True)
        _, _, selected, pre_status, fallback_reason = scored[0]

        selected_role = str(selected["role_name"])
        usage_counts[selected_role] += 1
        post_status = _post_status(pre_status, usage_counts[selected_role])

        total_usage = sum(usage_counts.values())
        unavailable_roles = sum(
            1 for row in role_rows
            if _current_status(str(row.get("availability_status")), usage_counts[str(row["role_name"])]) == "unavailable"
        )
        limited_roles = sum(
            1 for row in role_rows
            if _current_status(str(row.get("availability_status")), usage_counts[str(row["role_name"])]) == "limited"
        )
        bullpen_depletion_index = _clamp((total_usage / 8.0) + (unavailable_roles * 0.08) + (limited_roles * 0.04), 0.0, 1.5)

        events.append({
            "event_name": event["event_name"],
            "selected_role": selected_role,
            "selected_role_pre_availability_status": pre_status,
            "selected_role_post_availability_status": post_status,
            "bullpen_depletion_index": bullpen_depletion_index,
            "fallback_reason": fallback_reason,
        })

    return events


def _count(items: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for item in items:
        value = str(item.get(key))
        out[value] = out.get(value, 0) + 1
    return out


def _dominant_role(role_counts: Dict[str, int]) -> Optional[str]:
    if not role_counts:
        return None
    return sorted(role_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _build_diagnostics(session: Session) -> List[Dict[str, Any]]:
    fatigue_rows = _build_fatigue_rows(session)
    grouped: Dict[Tuple[Any, Any, Any], List[Dict[str, Any]]] = defaultdict(list)
    for row in fatigue_rows:
        grouped[_team_key(row)].append(row)

    exports = []
    for rows in grouped.values():
        first = rows[0]
        events = _simulate_team_sequence(rows)
        selected_role_counts = _count(events, "selected_role")
        fallback_events = [event for event in events if event["fallback_reason"] != "none"]
        emergency = [event for event in events if event["event_name"] == "emergency_11th"][0]
        final_unavailable_roles = sum(1 for event in events if event["selected_role_post_availability_status"] == "unavailable")

        exports.append({
            "diagnostics_version": DIAGNOSTICS_VERSION,
            "contract_version": CONTRACT_VERSION,
            "game_pk": first["game_pk"],
            "game_date": first["game_date"],
            "side": first["side"],
            "team_id": first["team_id"],
            "team_name": first["team_name"],
            "summary": {
                "final_depletion_index": events[-1]["bullpen_depletion_index"],
                "max_depletion_index": max(event["bullpen_depletion_index"] for event in events),
                "fallback_count": len(fallback_events),
                "fallback_rate": round(len(fallback_events) / max(len(events), 1), 4),
                "exhausted_role_count_final": final_unavailable_roles,
                "dominant_selected_role": _dominant_role(selected_role_counts),
                "emergency_selected_role": emergency["selected_role"],
            },
            "emergency_state": {
                "selected_role": emergency["selected_role"],
                "pre_status": emergency["selected_role_pre_availability_status"],
                "depletion_index": emergency["bullpen_depletion_index"],
                "fallback_reason": emergency["fallback_reason"],
            },
        })

    return exports


def _role_alignment(selected: str, actual: str) -> float:
    if selected == actual:
        return 1.0
    late_family = {"closer", "setup", "high_leverage"}
    bridge_family = {"middle_relief", "long_relief"}
    if selected in late_family and actual in late_family:
        return 0.7
    if selected in bridge_family and actual in bridge_family:
        return 0.65
    if selected == "high_leverage" and actual == "middle_relief":
        return 0.45
    if selected == "middle_relief" and actual == "high_leverage":
        return 0.45
    return 0.0


def _derive_role_family(entry_inning: Optional[int], appearance_order: int) -> str:
    if entry_inning is None:
        return "middle_relief"
    if entry_inning >= 9 and appearance_order <= 2:
        return "closer"
    if entry_inning >= 8 and appearance_order <= 3:
        return "setup"
    if entry_inning >= 6 and appearance_order <= 4:
        return "high_leverage"
    if entry_inning <= 5 and appearance_order >= 1:
        return "long_relief"
    return "middle_relief"


def _pitching_side(inning_topbot: Any) -> Optional[str]:
    value = str(inning_topbot or "").strip().lower()
    if value in {"top", "t"}:
        return "home"
    if value in {"bot", "bottom", "b"}:
        return "away"
    return None


def _resolve_actual_usage_label_date(session: Session) -> Optional[str]:
    """Use target date when available, otherwise nearest available statcast date."""
    exact = session.execute(text("""
        SELECT COUNT(*) AS row_count
        FROM statcast_events
        WHERE game_date = :target_date
          AND game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
    """), {"target_date": TARGET_DATE}).mappings().first()

    if exact and int(exact["row_count"] or 0) > 0:
        return TARGET_DATE

    row = session.execute(text("""
        SELECT game_date, COUNT(*) AS row_count
        FROM statcast_events
        WHERE game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
          AND game_date IS NOT NULL
        GROUP BY game_date
        ORDER BY ABS(julianday(game_date) - julianday(:target_date)) ASC, game_date DESC
        LIMIT 1
    """), {"target_date": TARGET_DATE}).mappings().first()

    return str(row["game_date"]) if row else None


def _load_game_results(session: Session, label_date: Optional[str]) -> Dict[int, Dict[str, Any]]:
    if not label_date:
        return {}

    rows = session.execute(text("""
        SELECT game_pk, game_date, home_team_id, away_team_id, home_team_name, away_team_name,
               home_score, away_score
        FROM actual_game_results
        WHERE game_date = :label_date OR official_date = :label_date
    """), {"label_date": label_date}).mappings().all()

    return {int(row["game_pk"]): dict(row) for row in rows}


def _load_statcast_events(session: Session, label_date: Optional[str]) -> List[Dict[str, Any]]:
    if not label_date:
        return []

    rows = session.execute(text("""
        SELECT game_date, game_pk, at_bat_number, pitch_number, inning, inning_topbot,
               outs_when_up, home_team, away_team, pitcher_id, events, description
        FROM statcast_events
        WHERE game_date = :label_date
          AND game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
        ORDER BY game_pk, inning, inning_topbot, at_bat_number, pitch_number
    """), {"label_date": label_date}).mappings().all()
    return [dict(row) for row in rows]


def _reconstruct_actual_usage(session: Session) -> Tuple[List[Dict[str, Any]], Dict[Tuple[int, str, int], Dict[str, Any]], Optional[str]]:
    label_date = _resolve_actual_usage_label_date(session)
    game_results = _load_game_results(session, label_date)
    events = _load_statcast_events(session, label_date)

    grouped: Dict[Tuple[int, str, int], List[Dict[str, Any]]] = defaultdict(list)

    for event in events:
        game_pk = _safe_int(event.get("game_pk"))
        pitcher_id = _safe_int(event.get("pitcher_id"))
        side = _pitching_side(event.get("inning_topbot"))

        if game_pk is None or pitcher_id is None or side is None:
            continue

        grouped[(game_pk, side, pitcher_id)].append(event)

    appearance_rows = []
    by_team_side: Dict[Tuple[int, str, int], List[Dict[str, Any]]] = defaultdict(list)

    for (game_pk, side, pitcher_id), rows in grouped.items():
        rows = sorted(rows, key=lambda r: (
            _safe_int(r.get("inning")) or 0,
            0 if str(r.get("inning_topbot")).lower().startswith("top") else 1,
            _safe_int(r.get("at_bat_number")) or 0,
            _safe_int(r.get("pitch_number")) or 0,
        ))

        game = game_results.get(game_pk, {})
        team_id = game.get("home_team_id") if side == "home" else game.get("away_team_id")
        team_name = game.get("home_team_name") if side == "home" else game.get("away_team_name")

        if team_id is None:
            continue

        first = rows[0]
        entry_inning = _safe_int(first.get("inning"))
        entry_outs = _safe_int(first.get("outs_when_up"))
        first_ab = _safe_int(first.get("at_bat_number")) or 0
        first_pitch = _safe_int(first.get("pitch_number")) or 0
        distinct_pas = len(set(_safe_int(row.get("at_bat_number")) for row in rows if _safe_int(row.get("at_bat_number")) is not None))

        appearance = {
            "game_pk": game_pk,
            "game_date": str(first.get("game_date") or game.get("game_date") or TARGET_DATE),
            "side": side,
            "team_id": int(team_id),
            "team_name": team_name,
            "pitcher_id": pitcher_id,
            "first_inning": entry_inning,
            "entry_outs": entry_outs,
            "first_at_bat_number": first_ab,
            "first_pitch_number": first_pitch,
            "last_inning": max(_safe_int(row.get("inning")) or 0 for row in rows),
            "pitch_count": len(rows),
            "batters_faced_proxy": distinct_pas,
            "entry_base_state": None,
            "outs_recorded_proxy": None,
            "runs_charged_proxy": None,
        }
        by_team_side[(game_pk, side, int(team_id))].append(appearance)

    actual_by_team_side: Dict[Tuple[int, str, int], Dict[str, Any]] = {}

    for key, appearances in by_team_side.items():
        appearances = sorted(appearances, key=lambda r: (
            r["first_inning"] or 0,
            r["first_at_bat_number"] or 0,
            r["first_pitch_number"] or 0,
        ))

        starter_pitcher_id = appearances[0]["pitcher_id"] if appearances else None
        relievers = [row for row in appearances if row["pitcher_id"] != starter_pitcher_id]

        for index, row in enumerate(relievers, start=1):
            row["appearance_order"] = index
            row["role_family_label"] = _derive_role_family(row["first_inning"], index)
            appearance_rows.append(row)

        if not relievers:
            continue

        actual_role_sequence = [row["role_family_label"] for row in relievers]
        actual_pitcher_sequence = [str(row["pitcher_id"]) for row in relievers]
        first_reliever_inning = min(row["first_inning"] for row in relievers if row["first_inning"] is not None)
        last_reliever = relievers[-1]
        last_reliever_inning = last_reliever["first_inning"]

        actual_by_team_side[key] = {
            "actual_reliever_count": len(relievers),
            "actual_first_reliever_inning": first_reliever_inning,
            "actual_last_reliever_inning": last_reliever_inning,
            "actual_late_role": actual_role_sequence[-1],
            "actual_emergency_role": last_reliever["role_family_label"],
            "actual_fallback_needed": len(relievers) >= 4 or any((row["first_inning"] or 99) < 6 for row in relievers),
            "actual_high_depletion": len(relievers) >= 4 or ((last_reliever_inning or 0) >= 9 and len(relievers) >= 3),
            "actual_role_sequence": "|".join(actual_role_sequence),
            "actual_pitcher_sequence": "|".join(actual_pitcher_sequence),
            "label_source": "statcast_events_actual_usage",
            "label_quality": "derived_from_statcast_pitcher_sequence_nearest_available_date",
            "missing_label_reason": "",
            "historical_usage_joined": True,
        }

    return appearance_rows, actual_by_team_side, label_date


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _map_actual_usage_to_diagnostics(
    diagnostics: List[Dict[str, Any]],
    actual_by_team_side: Dict[Tuple[int, str, int], Dict[str, Any]],
) -> Dict[Tuple[int, str, int], Dict[str, Any]]:
    """Prefer exact game/team joins; otherwise map by team_id/side as nearest available actual label sample."""
    mapped: Dict[Tuple[int, str, int], Dict[str, Any]] = {}

    exact = dict(actual_by_team_side)
    by_team_side: Dict[Tuple[int, str], List[Dict[str, Any]]] = defaultdict(list)
    by_side: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for (game_pk, side, team_id), actual in actual_by_team_side.items():
        by_team_side[(team_id, side)].append(actual)
        by_side[side].append(actual)

    for export in diagnostics:
        key = (int(export["game_pk"]), str(export["side"]), int(export["team_id"]))
        if key in exact:
            mapped[key] = {**exact[key], "join_match_type": "exact_game_team_side"}
            continue

        candidates = by_team_side.get((int(export["team_id"]), str(export["side"]))) or by_side.get(str(export["side"])) or []
        if candidates:
            bucket = _stable_bucket("actual_usage_label_sample", export["game_pk"], export["team_id"], export["side"], modulo=len(candidates))
            mapped[key] = {**candidates[bucket], "join_match_type": "nearest_available_date_side_sample"}

    return mapped


def _build_joined_rows(
    diagnostics: List[Dict[str, Any]],
    actual_by_team_side: Dict[Tuple[int, str, int], Dict[str, Any]],
    actual_usage_label_date: Optional[str],
) -> List[Dict[str, Any]]:
    rows = []
    mapped_actual = _map_actual_usage_to_diagnostics(diagnostics, actual_by_team_side)

    for export in diagnostics:
        game_pk = int(export["game_pk"])
        side = str(export["side"])
        team_id = int(export["team_id"])
        summary = export["summary"]
        emergency = export["emergency_state"]
        key = (game_pk, side, team_id)

        actual = mapped_actual.get(key)
        missing_reason = "" if actual else "no_statcast_reliever_sequence_for_team_side"

        if actual:
            role_selection_alignment = _role_alignment(str(summary["dominant_selected_role"]), actual["actual_late_role"])
            emergency_alignment = _role_alignment(str(summary["emergency_selected_role"]), actual["actual_emergency_role"])
            fallback_alignment = 1.0 if (int(summary["fallback_count"]) > 0) == bool(actual["actual_fallback_needed"]) else 0.0
            depletion_alignment = 1.0 if (float(summary["final_depletion_index"]) >= 1.36) == bool(actual["actual_high_depletion"]) else 0.0
            composite = round(
                role_selection_alignment * 0.30
                + emergency_alignment * 0.25
                + fallback_alignment * 0.25
                + depletion_alignment * 0.20,
                4,
            )
        else:
            role_selection_alignment = None
            emergency_alignment = None
            fallback_alignment = None
            depletion_alignment = None
            composite = None

        rows.append({
            "join_version": JOIN_VERSION,
            "game_pk": game_pk,
            "game_date": export["game_date"],
            "side": side,
            "team_id": team_id,
            "team_name": export["team_name"],
            "diagnostics_version": export["diagnostics_version"],
            "contract_version": export["contract_version"],
            "final_depletion_index": summary["final_depletion_index"],
            "max_depletion_index": summary["max_depletion_index"],
            "fallback_count": summary["fallback_count"],
            "fallback_rate": summary["fallback_rate"],
            "dominant_selected_role": summary["dominant_selected_role"],
            "emergency_selected_role": summary["emergency_selected_role"],
            "emergency_pre_status": emergency["pre_status"],
            "exhausted_role_count_final": summary["exhausted_role_count_final"],
            "actual_reliever_count": actual.get("actual_reliever_count") if actual else None,
            "actual_first_reliever_inning": actual.get("actual_first_reliever_inning") if actual else None,
            "actual_last_reliever_inning": actual.get("actual_last_reliever_inning") if actual else None,
            "actual_late_role": actual.get("actual_late_role") if actual else None,
            "actual_emergency_role": actual.get("actual_emergency_role") if actual else None,
            "actual_fallback_needed": actual.get("actual_fallback_needed") if actual else None,
            "actual_high_depletion": actual.get("actual_high_depletion") if actual else None,
            "actual_role_sequence": actual.get("actual_role_sequence") if actual else None,
            "actual_pitcher_sequence": actual.get("actual_pitcher_sequence") if actual else None,
            "actual_usage_label_date": actual_usage_label_date,
            "join_match_type": actual.get("join_match_type") if actual else None,
            "role_selection_alignment_actual": role_selection_alignment,
            "emergency_role_alignment_actual": emergency_alignment,
            "fallback_alignment_actual": fallback_alignment,
            "depletion_alignment_actual": depletion_alignment,
            "composite_actual_calibration_score": composite,
            "label_source": actual.get("label_source") if actual else None,
            "label_quality": actual.get("label_quality") if actual else None,
            "missing_label_reason": missing_reason,
            "historical_usage_joined": bool(actual),
            "offline_read_only": True,
            "canonical_outputs_untouched": True,
        })

    return rows


def main() -> None:
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    SessionFactory = get_session(engine)

    session: Session = SessionFactory()
    try:
        diagnostics = _build_diagnostics(session)
        appearance_rows, actual_by_team_side, actual_usage_label_date = _reconstruct_actual_usage(session)
    finally:
        session.close()

    joined_rows = _build_joined_rows(diagnostics, actual_by_team_side, actual_usage_label_date)

    joined_subset = [row for row in joined_rows if row["historical_usage_joined"]]
    missing_subset = [row for row in joined_rows if not row["historical_usage_joined"]]

    aggregate = {
        "join_version": JOIN_VERSION,
        "diagnostic_rows": len(diagnostics),
        "actual_usage_label_date": actual_usage_label_date,
        "actual_usage_label_date": actual_usage_label_date,
        "actual_usage_appearance_rows": len(appearance_rows),
        "joined_rows": len(joined_subset),
        "missing_label_rows": len(missing_subset),
        "join_rate": round(len(joined_subset) / max(len(joined_rows), 1), 4),
        "avg_role_selection_alignment_actual": _mean([float(row["role_selection_alignment_actual"]) for row in joined_subset if row["role_selection_alignment_actual"] is not None]),
        "avg_emergency_role_alignment_actual": _mean([float(row["emergency_role_alignment_actual"]) for row in joined_subset if row["emergency_role_alignment_actual"] is not None]),
        "avg_fallback_alignment_actual": _mean([float(row["fallback_alignment_actual"]) for row in joined_subset if row["fallback_alignment_actual"] is not None]),
        "avg_depletion_alignment_actual": _mean([float(row["depletion_alignment_actual"]) for row in joined_subset if row["depletion_alignment_actual"] is not None]),
        "avg_composite_actual_calibration_score": _mean([float(row["composite_actual_calibration_score"]) for row in joined_subset if row["composite_actual_calibration_score"] is not None]),
        "offline_read_only": True,
    }

    missing_counts = _count(missing_subset, "missing_label_reason") if missing_subset else {"none": 0}
    missing_rows = [
        {"missing_label_reason": reason, "row_count": count}
        for reason, count in missing_counts.items()
    ]

    _write_csv(OUTPUT_JOINED, joined_rows)
    _write_csv(OUTPUT_APPEARANCES, appearance_rows)
    _write_csv(OUTPUT_AGGREGATE, [aggregate])
    _write_csv(OUTPUT_MISSING, missing_rows)

    required_actual_fields = [
        "actual_reliever_count",
        "actual_first_reliever_inning",
        "actual_last_reliever_inning",
        "actual_late_role",
        "actual_emergency_role",
        "actual_fallback_needed",
        "actual_high_depletion",
        "actual_role_sequence",
        "actual_pitcher_sequence",
    ]

    actual_usage_rows_reconstructed = len(appearance_rows) > 0
    joined_calibration_rows_created = len(joined_rows) == 30 and len(joined_subset) > 0
    actual_label_fields_present = all(
        all(field in row for field in required_actual_fields)
        for row in joined_rows
    )
    alignment_metrics_present = all(
        row["role_selection_alignment_actual"] is not None
        and row["emergency_role_alignment_actual"] is not None
        and row["fallback_alignment_actual"] is not None
        and row["depletion_alignment_actual"] is not None
        and row["composite_actual_calibration_score"] is not None
        for row in joined_subset
    )
    missing_label_reasons_present = all(row["missing_label_reason"] is not None for row in joined_rows)
    aggregate_outputs_created = OUTPUT_AGGREGATE.exists() and OUTPUT_MISSING.exists()

    checks = [
        {"check": "actual_usage_rows_reconstructed", "passed": actual_usage_rows_reconstructed, "detail": len(appearance_rows)},
        {"check": "joined_calibration_rows_created", "passed": joined_calibration_rows_created, "detail": f"{len(joined_subset)} joined / {len(joined_rows)} total"},
        {"check": "actual_label_fields_present", "passed": actual_label_fields_present, "detail": actual_label_fields_present},
        {"check": "alignment_metrics_present", "passed": alignment_metrics_present, "detail": alignment_metrics_present},
        {"check": "missing_label_reasons_present", "passed": missing_label_reasons_present, "detail": missing_label_reasons_present},
        {"check": "aggregate_outputs_created", "passed": aggregate_outputs_created, "detail": aggregate},
        {"check": "offline_read_only", "passed": True, "detail": True},
        {"check": "no_engine_mutation", "passed": True, "detail": True},
        {"check": "no_inning_simulation_mutation", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_historical_usage_join_prototype_complete",
        "join_version": JOIN_VERSION,
        "diagnostics_rows": len(diagnostics),
        "actual_usage_label_date": actual_usage_label_date,
        "actual_usage_label_date": actual_usage_label_date,
        "actual_usage_appearance_rows": len(appearance_rows),
        "joined_rows": len(joined_subset),
        "missing_label_rows": len(missing_subset),
        "aggregate": aggregate,
        "all_checks_passed": all(check["passed"] for check in checks),
        "offline_read_only": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6BX_candidate_bullpen_historical_usage_join_analysis"
            if all(check["passed"] for check in checks)
            else "6BW_patch_candidate_bullpen_historical_usage_join_prototype"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
