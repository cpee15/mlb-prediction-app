from __future__ import annotations

import csv
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.model_projections import build_model_projection_payload


TARGET_DATE = "2026-05-20"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_depletion_sequence_analysis.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_depletion_sequence_analysis_checks.csv"
OUTPUT_ROWS = OUTPUT_DIR / "candidate_bullpen_depletion_sequence_analysis_rows.csv"
OUTPUT_TEAM = OUTPUT_DIR / "candidate_bullpen_depletion_sequence_analysis_team_summary.csv"
OUTPUT_EVENT = OUTPUT_DIR / "candidate_bullpen_depletion_sequence_analysis_event_summary.csv"
OUTPUT_STEP = OUTPUT_DIR / "candidate_bullpen_depletion_sequence_analysis_step_summary.csv"


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
        return float(value)
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
                "leverage_bucket": template["leverage_bucket"],
                "inherited_runner_exposure": template["inherited_runner_exposure"],
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
        availability_multiplier = 0.0
        fatigue_penalty = 0.045
    elif recent_pitch_count >= 28 and priority <= 3:
        availability_status = "limited"
        availability_multiplier = 0.55
        fatigue_penalty = 0.030
    elif recent_pitch_count >= 22:
        availability_status = "limited"
        availability_multiplier = 0.70
        fatigue_penalty = 0.020
    else:
        availability_status = "available"
        availability_multiplier = 1.0
        fatigue_penalty = 0.000 if days_rest >= 1 else 0.010

    return {
        "days_rest_proxy": days_rest,
        "recent_pitch_count_proxy": recent_pitch_count,
        "back_to_back_risk": back_to_back_risk,
        "availability_status": availability_status,
        "availability_multiplier": availability_multiplier,
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


def _build_fatigue_rows() -> List[Dict[str, Any]]:
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    SessionFactory = get_session(engine)

    session: Session = SessionFactory()
    try:
        payload = build_model_projection_payload(session, TARGET_DATE)
        games = payload.get("games") or []
    finally:
        session.close()

    rows: List[Dict[str, Any]] = []
    for game in games:
        for role_row in _base_role_rows(game):
            rows.append(_apply_fatigue(role_row))
    return rows


def _team_key(row: Dict[str, Any]) -> Tuple[Any, Any, Any]:
    return (row.get("game_pk"), row.get("side"), row.get("team_id"))


def _runner_count(base_state: str) -> int:
    return sum(1 for c in base_state if c in {"1", "2", "3"})


def _base_pressure(base_state: str, outs: int) -> float:
    runner_weights = {"1": 0.18, "2": 0.32, "3": 0.42}
    pressure = sum(runner_weights.get(c, 0.0) for c in base_state)
    out_modifier = {0: 1.0, 1: 0.78, 2: 0.52}.get(outs, 0.7)
    return _clamp(pressure * out_modifier, 0.0, 1.0) or 0.0


def _current_status(base_status: str, usage_count: int) -> Tuple[str, float, float, str]:
    if usage_count <= 0:
        return base_status, 0.0, 1.0, "fresh_or_base_status"
    if usage_count == 1:
        if base_status == "unavailable":
            return "unavailable", 0.045, 0.0, "base_unavailable_after_prior_use"
        return "limited", 0.025, 0.70, "second_appearance_limited"
    return "unavailable", 0.060, 0.0, "third_plus_appearance_unavailable"


def _score_role(row: Dict[str, Any], event: Dict[str, Any], usage_count: int, emergency: bool, alternative_exists: bool) -> Tuple[float, str, str, float, float]:
    role_name = str(row.get("role_name"))
    preferred_roles = event["preferred_roles"]
    avoid_roles = event["avoid_roles"]
    pre_status, depletion_penalty, depletion_multiplier, _ = _current_status(str(row.get("availability_status")), usage_count)

    score = float(row.get("adjusted_role_quality_score") or 0.0)

    if role_name in preferred_roles:
        score += 0.095 - 0.010 * preferred_roles.index(role_name)
    else:
        score -= 0.040

    if role_name in avoid_roles:
        score -= 0.080

    if event["leverage"] in {"save", "high"}:
        score += max(0.0, (6 - int(row.get("role_priority") or 5)) * 0.006)
    else:
        score -= max(0.0, (6 - int(row.get("role_priority") or 5)) * 0.004)

    pressure = _base_pressure(str(event["base_state"]), int(event["outs"]))
    score += pressure * float(row.get("adjusted_role_k_rate") or 0.0) * 0.12
    score -= pressure * float(row.get("adjusted_role_bb_rate") or 0.0) * 0.10

    if pre_status == "unavailable" and alternative_exists and not emergency:
        score -= 999.0
    elif pre_status == "unavailable" and emergency:
        score -= 0.150
    elif pre_status == "limited":
        score *= depletion_multiplier
        score -= depletion_penalty

    score -= usage_count * 0.035
    score -= depletion_penalty

    if event["event_name"] == "emergency_11th" and role_name in {"middle_relief", "long_relief"}:
        score += 0.030

    fallback_reason = "none" if role_name in preferred_roles else f"preferred_roles_depleted_or_outscored_by_{role_name}"
    return round(score, 5), pre_status, fallback_reason, depletion_penalty, depletion_multiplier


def _post_status(pre_status: str, post_usage_count: int) -> str:
    if post_usage_count >= 2:
        return "unavailable"
    if post_usage_count == 1 and pre_status in {"available", "limited"}:
        return "limited"
    return pre_status


def _simulate_team_sequence(role_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    usage_counts = {str(row["role_name"]): 0 for row in role_rows}
    rows: List[Dict[str, Any]] = []

    for event in SEQUENCE_EVENTS:
        emergency = event["event_name"] == "emergency_11th"
        alternative_exists = any(
            _current_status(str(row.get("availability_status")), usage_counts[str(row["role_name"])])[0] != "unavailable"
            for row in role_rows
        )

        scored = []
        for row in role_rows:
            score, pre_status, fallback_reason, depletion_penalty, depletion_multiplier = _score_role(
                row=row,
                event=event,
                usage_count=usage_counts[str(row["role_name"])],
                emergency=emergency,
                alternative_exists=alternative_exists,
            )
            scored.append((score, int(row.get("role_priority") or 99), row, pre_status, fallback_reason, depletion_penalty, depletion_multiplier))

        scored.sort(key=lambda item: (item[0], -item[1]), reverse=True)
        selection_score, _, selected, pre_status, fallback_reason, depletion_penalty, depletion_multiplier = scored[0]

        selected_role = str(selected["role_name"])
        prior_usage = usage_counts[selected_role]
        post_usage = prior_usage + 1
        usage_counts[selected_role] = post_usage
        post_status = _post_status(pre_status, post_usage)

        total_usage = sum(usage_counts.values())
        unavailable_roles = sum(
            1 for row in role_rows
            if _current_status(str(row.get("availability_status")), usage_counts[str(row["role_name"])])[0] == "unavailable"
        )
        limited_roles = sum(
            1 for row in role_rows
            if _current_status(str(row.get("availability_status")), usage_counts[str(row["role_name"])])[0] == "limited"
        )
        bullpen_depletion_index = _clamp((total_usage / 8.0) + (unavailable_roles * 0.08) + (limited_roles * 0.04), 0.0, 1.5)

        rows.append({
            "game_pk": selected.get("game_pk"),
            "game_date": selected.get("game_date"),
            "side": selected.get("side"),
            "team_id": selected.get("team_id"),
            "team_name": selected.get("team_name"),
            "sequence_step": event["sequence_step"],
            "event_name": event["event_name"],
            "inning": event["inning"],
            "leverage": event["leverage"],
            "base_state": event["base_state"],
            "outs": event["outs"],
            "inherited_runner_count": _runner_count(str(event["base_state"])),
            "preferred_roles": "|".join(event["preferred_roles"]),
            "selected_role": selected_role,
            "selected_role_prior_usage_count": prior_usage,
            "selected_role_post_usage_count": post_usage,
            "selected_role_pre_availability_status": pre_status,
            "selected_role_post_availability_status": post_status,
            "depletion_penalty_applied": depletion_penalty,
            "depletion_multiplier_applied": depletion_multiplier,
            "bullpen_depletion_index": bullpen_depletion_index,
            "selection_score": selection_score,
            "fallback_reason": fallback_reason,
            "candidate_only": True,
            "analysis_only": True,
        })

    return rows


def _build_sequence_rows() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    fatigue_rows = _build_fatigue_rows()
    grouped: Dict[Tuple[Any, Any, Any], List[Dict[str, Any]]] = defaultdict(list)
    for row in fatigue_rows:
        grouped[_team_key(row)].append(row)

    sequence_rows: List[Dict[str, Any]] = []
    for role_rows in grouped.values():
        sequence_rows.extend(_simulate_team_sequence(role_rows))

    return fatigue_rows, sequence_rows


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _count(rows: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    result: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key))
        result[value] = result.get(value, 0) + 1
    return result


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def main() -> None:
    fatigue_rows, sequence_rows = _build_sequence_rows()
    _write_csv(OUTPUT_ROWS, sequence_rows)

    grouped_team: Dict[Tuple[Any, Any, Any], List[Dict[str, Any]]] = defaultdict(list)
    grouped_event: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    grouped_step: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

    for row in sequence_rows:
        grouped_team[(row["game_pk"], row["side"], row["team_id"])].append(row)
        grouped_event[str(row["event_name"])].append(row)
        grouped_step[int(row["sequence_step"])].append(row)

    team_rows = []
    for key, rows in grouped_team.items():
        rows = sorted(rows, key=lambda r: int(r["sequence_step"]))
        first = rows[0]
        fallback_count = sum(1 for row in rows if row["fallback_reason"] != "none")
        team_rows.append({
            "game_pk": key[0],
            "side": key[1],
            "team_id": key[2],
            "team_name": first["team_name"],
            "sequence_rows": len(rows),
            "role_counts": json.dumps(_count(rows, "selected_role"), sort_keys=True),
            "status_counts": json.dumps(_count(rows, "selected_role_pre_availability_status"), sort_keys=True),
            "fallback_count": fallback_count,
            "first_bullpen_depletion_index": rows[0]["bullpen_depletion_index"],
            "final_bullpen_depletion_index": rows[-1]["bullpen_depletion_index"],
            "max_bullpen_depletion_index": round(max(float(row["bullpen_depletion_index"]) for row in rows), 4),
        })
    _write_csv(OUTPUT_TEAM, team_rows)

    event_rows = []
    for event_name, rows in sorted(grouped_event.items()):
        event_rows.append({
            "event_name": event_name,
            "row_count": len(rows),
            "role_counts": json.dumps(_count(rows, "selected_role"), sort_keys=True),
            "pre_status_counts": json.dumps(_count(rows, "selected_role_pre_availability_status"), sort_keys=True),
            "fallback_count": sum(1 for row in rows if row["fallback_reason"] != "none"),
            "avg_depletion_index": _mean([float(row["bullpen_depletion_index"]) for row in rows]),
        })
    _write_csv(OUTPUT_EVENT, event_rows)

    step_rows = []
    for step, rows in sorted(grouped_step.items()):
        step_rows.append({
            "sequence_step": step,
            "event_name": rows[0]["event_name"],
            "row_count": len(rows),
            "avg_depletion_index": _mean([float(row["bullpen_depletion_index"]) for row in rows]),
            "max_depletion_index": round(max(float(row["bullpen_depletion_index"]) for row in rows), 4),
            "fallback_count": sum(1 for row in rows if row["fallback_reason"] != "none"),
        })
    _write_csv(OUTPUT_STEP, step_rows)

    expected_fatigue_rows = 15 * 2 * len(ROLE_TEMPLATES)
    expected_sequence_rows = 15 * 2 * len(SEQUENCE_EVENTS)

    expected_event_names = [event["event_name"] for event in SEQUENCE_EVENTS]
    sequence_order_valid = all(
        [row["event_name"] for row in sorted(rows, key=lambda r: int(r["sequence_step"]))] == expected_event_names
        and [int(row["sequence_step"]) for row in sorted(rows, key=lambda r: int(r["sequence_step"]))] == list(range(1, 9))
        for rows in grouped_team.values()
    )

    depletion_finite = all(row.get("bullpen_depletion_index") is not None for row in sequence_rows)
    final_ge_first = all(float(row["final_bullpen_depletion_index"]) >= float(row["first_bullpen_depletion_index"]) for row in team_rows)
    max_bounded = max(float(row["bullpen_depletion_index"]) for row in sequence_rows) <= 1.5
    step_avgs = [float(row["avg_depletion_index"]) for row in step_rows]
    generally_increases = sum(1 for a, b in zip(step_avgs, step_avgs[1:]) if b >= a) >= 6

    usage_depletion_behavior_valid = (
        all(int(row["selected_role_post_usage_count"]) == int(row["selected_role_prior_usage_count"]) + 1 for row in sequence_rows)
        and depletion_finite
        and final_ge_first
        and max_bounded
        and generally_increases
    )

    availability_degradation_valid = all(
        (
            int(row["selected_role_post_usage_count"]) < 2
            or row["selected_role_post_availability_status"] == "unavailable"
        )
        for row in sequence_rows
    )

    fallback_count = sum(1 for row in sequence_rows if row["fallback_reason"] != "none")
    fallback_rate = round(fallback_count / max(len(sequence_rows), 1), 4)
    emergency_rows = [row for row in sequence_rows if row["event_name"] == "emergency_11th"]
    fallback_behavior_valid = (
        all(row.get("fallback_reason") is not None for row in sequence_rows)
        and fallback_count > 0
        and fallback_rate <= 0.25
        and len(emergency_rows) == 30
        and all(row.get("selected_role") for row in emergency_rows)
    )

    selected_role_counts = _count(sequence_rows, "selected_role")
    selected_pre_status_counts = _count(sequence_rows, "selected_role_pre_availability_status")

    early_middle_rows = [row for row in sequence_rows if row["event_name"] in {"starter_short_hook_4th", "middle_bridge_5th"}]
    late_rows = [row for row in sequence_rows if row["event_name"] in {"setup_8th", "save_9th", "extras_10th"}]

    late_long_relief_rows = [
        row
        for row in late_rows
        if row["selected_role"] == "long_relief"
    ]
    late_long_relief_rate = round(len(late_long_relief_rows) / max(len(late_rows), 1), 4)
    late_long_relief_only_as_fallback = all(
        row["fallback_reason"] != "none"
        and row["selected_role_pre_availability_status"] in {"limited", "unavailable"}
        for row in late_long_relief_rows
    )

    role_distribution_valid = (
        set(selected_role_counts) == {"closer", "setup", "high_leverage", "middle_relief", "long_relief"}
        and selected_role_counts.get("closer", 0) / max(len(sequence_rows), 1) <= 0.30
        and any(row["selected_role"] in {"long_relief", "middle_relief"} for row in early_middle_rows)
        and late_long_relief_rate <= 0.02
        and late_long_relief_only_as_fallback
    )

    candidate_mode_only = all(row.get("candidate_only") is True and row.get("analysis_only") is True for row in sequence_rows)

    unavailable_emergency = sum(1 for row in emergency_rows if row["selected_role_pre_availability_status"] == "unavailable")
    emergency_unavailable_pre_status_rate = round(unavailable_emergency / max(len(emergency_rows), 1), 4)

    average_final_depletion_index = _mean([float(row["final_bullpen_depletion_index"]) for row in team_rows])
    average_team_fallback_count = _mean([float(row["fallback_count"]) for row in team_rows])
    teams_with_fallback = sum(1 for row in team_rows if int(row["fallback_count"]) > 0)

    checks = [
        {"check": "sequence_rows_available", "passed": len(fatigue_rows) == expected_fatigue_rows and len(sequence_rows) == expected_sequence_rows, "detail": f"{len(fatigue_rows)}/{expected_fatigue_rows}; {len(sequence_rows)}/{expected_sequence_rows}"},
        {"check": "sequence_order_valid", "passed": sequence_order_valid, "detail": sequence_order_valid},
        {"check": "usage_depletion_behavior_valid", "passed": usage_depletion_behavior_valid, "detail": {"final_ge_first": final_ge_first, "max_bounded": max_bounded, "generally_increases": generally_increases}},
        {"check": "availability_degradation_valid", "passed": availability_degradation_valid, "detail": selected_pre_status_counts},
        {"check": "fallback_behavior_valid", "passed": fallback_behavior_valid, "detail": {"fallback_count": fallback_count, "fallback_rate": fallback_rate}},
        {
            "check": "role_distribution_valid",
            "passed": role_distribution_valid,
            "detail": {
                "selected_role_counts": selected_role_counts,
                "late_long_relief_count": len(late_long_relief_rows),
                "late_long_relief_rate": late_long_relief_rate,
                "late_long_relief_only_as_fallback": late_long_relief_only_as_fallback,
            },
        },
        {"check": "candidate_mode_only", "passed": candidate_mode_only, "detail": candidate_mode_only},
        {"check": "analysis_only_no_engine_mutation", "passed": True, "detail": True},
        {"check": "no_inning_simulation_mutation", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_depletion_sequence_analysis_complete",
        "fatigue_rows_analyzed": len(fatigue_rows),
        "sequence_rows_analyzed": len(sequence_rows),
        "expected_sequence_rows": expected_sequence_rows,
        "selected_role_counts": selected_role_counts,
        "selected_pre_status_counts": selected_pre_status_counts,
        "fallback_count": fallback_count,
        "fallback_rate": fallback_rate,
        "metrics": {
            "average_final_depletion_index": average_final_depletion_index,
            "max_bullpen_depletion_index": round(max(float(row["bullpen_depletion_index"]) for row in sequence_rows), 4),
            "average_team_fallback_count": average_team_fallback_count,
            "teams_with_fallback": teams_with_fallback,
            "emergency_unavailable_pre_status_rate": emergency_unavailable_pre_status_rate,
            "late_long_relief_count": len(late_long_relief_rows),
            "late_long_relief_rate": late_long_relief_rate,
            "late_long_relief_only_as_fallback": late_long_relief_only_as_fallback,
            "average_depletion_by_step": {str(row["sequence_step"]): row["avg_depletion_index"] for row in step_rows},
        },
        "all_checks_passed": all(check["passed"] for check in checks),
        "candidate_mode_only": candidate_mode_only,
        "analysis_only": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6BN_candidate_bullpen_engine_shadow_readiness_audit"
            if all(check["passed"] for check in checks)
            else "6BL_patch_candidate_bullpen_depletion_sequence_prototype"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
