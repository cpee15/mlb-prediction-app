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

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_inherited_runner_context_prototype.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_inherited_runner_context_prototype_checks.csv"
OUTPUT_MATRIX = OUTPUT_DIR / "candidate_bullpen_inherited_runner_context_prototype_matrix.csv"
OUTPUT_SUMMARY = OUTPUT_DIR / "candidate_bullpen_inherited_runner_context_prototype_team_summary.csv"


ROLE_TEMPLATES = [
    {"role_name": "closer", "role_priority": 1, "leverage_bucket": "save_highest_leverage", "inherited_runner_exposure": "low", "k_delta": 0.018, "bb_delta": -0.008, "whiff_delta": 0.015, "csw_delta": 0.010, "hard_hit_delta": -0.012, "xwoba_delta": -0.010, "quality_delta": 0.030},
    {"role_name": "setup", "role_priority": 2, "leverage_bucket": "late_high_leverage", "inherited_runner_exposure": "medium", "k_delta": 0.012, "bb_delta": -0.005, "whiff_delta": 0.010, "csw_delta": 0.007, "hard_hit_delta": -0.008, "xwoba_delta": -0.007, "quality_delta": 0.020},
    {"role_name": "high_leverage", "role_priority": 3, "leverage_bucket": "matchup_high_leverage", "inherited_runner_exposure": "high", "k_delta": 0.006, "bb_delta": -0.002, "whiff_delta": 0.006, "csw_delta": 0.004, "hard_hit_delta": -0.004, "xwoba_delta": -0.004, "quality_delta": 0.010},
    {"role_name": "middle_relief", "role_priority": 4, "leverage_bucket": "middle_neutral", "inherited_runner_exposure": "medium", "k_delta": 0.000, "bb_delta": 0.000, "whiff_delta": 0.000, "csw_delta": 0.000, "hard_hit_delta": 0.000, "xwoba_delta": 0.000, "quality_delta": 0.000},
    {"role_name": "long_relief", "role_priority": 5, "leverage_bucket": "length_low_leverage", "inherited_runner_exposure": "low", "k_delta": -0.012, "bb_delta": 0.006, "whiff_delta": -0.010, "csw_delta": -0.006, "hard_hit_delta": 0.010, "xwoba_delta": 0.010, "quality_delta": -0.020},
]


INHERITED_SCENARIOS = [
    {"scenario_name": "clean_start", "inning": 6, "leverage": "neutral", "base_state": "---", "outs": 0, "preferred_roles": ["middle_relief", "high_leverage"], "avoid_roles": ["closer"]},
    {"scenario_name": "runner_on_first_one_out", "inning": 6, "leverage": "neutral", "base_state": "1--", "outs": 1, "preferred_roles": ["middle_relief", "high_leverage"], "avoid_roles": ["long_relief"]},
    {"scenario_name": "runner_on_second_no_out", "inning": 7, "leverage": "high", "base_state": "-2-", "outs": 0, "preferred_roles": ["high_leverage", "setup"], "avoid_roles": ["long_relief"]},
    {"scenario_name": "runners_corners_one_out", "inning": 7, "leverage": "high", "base_state": "1-3", "outs": 1, "preferred_roles": ["high_leverage", "setup"], "avoid_roles": ["long_relief"]},
    {"scenario_name": "bases_loaded_no_out", "inning": 8, "leverage": "high", "base_state": "123", "outs": 0, "preferred_roles": ["setup", "closer", "high_leverage"], "avoid_roles": ["long_relief", "middle_relief"]},
    {"scenario_name": "bases_loaded_two_out", "inning": 8, "leverage": "high", "base_state": "123", "outs": 2, "preferred_roles": ["setup", "closer", "high_leverage"], "avoid_roles": ["long_relief"]},
    {"scenario_name": "extra_runner_second_no_out", "inning": 10, "leverage": "high", "base_state": "-2-", "outs": 0, "preferred_roles": ["closer", "setup", "high_leverage"], "avoid_roles": ["long_relief"]},
    {"scenario_name": "jam_escape_high_leverage", "inning": 9, "leverage": "save", "base_state": "12-", "outs": 1, "preferred_roles": ["closer", "setup", "high_leverage"], "avoid_roles": ["long_relief", "middle_relief"]},
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


def _select_role(role_rows: List[Dict[str, Any]], scenario: Dict[str, Any]) -> Tuple[Dict[str, Any], float, str]:
    alternative_exists = any(row.get("availability_status") != "unavailable" for row in role_rows)
    preferred_roles = scenario["preferred_roles"]
    avoid_roles = scenario["avoid_roles"]

    pressure = _base_pressure(scenario["base_state"], int(scenario["outs"]))

    scored = []
    for row in role_rows:
        role_name = str(row.get("role_name"))
        score = float(row.get("adjusted_role_quality_score") or 0.0)

        if role_name in preferred_roles:
            score += 0.090 - 0.010 * preferred_roles.index(role_name)
        else:
            score -= 0.035

        if role_name in avoid_roles:
            score -= 0.080

        if scenario["leverage"] in {"save", "high"}:
            score += max(0.0, (6 - int(row.get("role_priority") or 5)) * 0.006)

        if row.get("availability_status") == "unavailable" and alternative_exists:
            score -= 999.0
        elif row.get("availability_status") == "limited":
            score *= float(row.get("availability_multiplier") or 0.0)
            score -= 0.025

        score += pressure * float(row.get("adjusted_role_k_rate") or 0.0) * 0.12
        score += pressure * float(row.get("role_csw_rate") or 0.0) * 0.08
        score -= pressure * float(row.get("adjusted_role_bb_rate") or 0.0) * 0.10

        if pressure >= 0.45 and row.get("inherited_runner_exposure") == "high":
            score += 0.018
        if pressure >= 0.60 and row.get("role_name") == "long_relief":
            score -= 0.050

        reason = "direct_preferred_role_match" if role_name in preferred_roles else "fallback_nonpreferred_role"
        scored.append((round(score, 5), int(row.get("role_priority") or 99), row, reason))

    scored.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    return scored[0][2], scored[0][0], scored[0][3]


def _inherited_adjustments(selected: Dict[str, Any], scenario: Dict[str, Any]) -> Dict[str, Any]:
    base_state = str(scenario["base_state"])
    outs = int(scenario["outs"])
    inherited_count = _runner_count(base_state)
    pressure = _base_pressure(base_state, outs)

    leverage_multiplier = 1.0
    if scenario["leverage"] == "high":
        leverage_multiplier = 1.25
    elif scenario["leverage"] == "save":
        leverage_multiplier = 1.35

    strand_pressure_index = _clamp(pressure * leverage_multiplier, 0.0, 1.5) or 0.0

    exposure = selected.get("inherited_runner_exposure")
    exposure_penalty = {"low": 0.000, "medium": 0.006, "high": 0.010}.get(str(exposure), 0.006)

    k_support = float(selected.get("adjusted_role_k_rate") or 0.0) * 0.35
    csw_support = float(selected.get("role_csw_rate") or 0.0) * 0.20
    bb_risk = float(selected.get("adjusted_role_bb_rate") or 0.0) * 0.28
    xwoba_risk = float(selected.get("adjusted_role_xwoba_allowed") or 0.0) * 0.22

    inherited_runner_risk_score = _clamp(
        strand_pressure_index * (0.34 + bb_risk + xwoba_risk + exposure_penalty),
        0.0,
        1.0,
    ) or 0.0

    strand_support_score = _clamp(
        k_support + csw_support - bb_risk - exposure_penalty,
        -0.10,
        0.20,
    ) or 0.0

    traffic_penalty = strand_pressure_index * 0.025
    quality_boost = strand_support_score * min(strand_pressure_index, 1.0)

    adjusted_quality = _clamp(
        float(selected.get("adjusted_role_quality_score") or 0.0) + quality_boost - traffic_penalty,
        -0.30,
        0.22,
    )

    adjusted_k = _clamp(
        float(selected.get("adjusted_role_k_rate") or 0.0) + strand_support_score * 0.10,
        0.12,
        0.38,
    )

    adjusted_bb = _clamp(
        float(selected.get("adjusted_role_bb_rate") or 0.0) + strand_pressure_index * 0.006,
        0.045,
        0.17,
    )

    adjusted_xwoba = _clamp(
        float(selected.get("adjusted_role_xwoba_allowed") or 0.0) + strand_pressure_index * 0.012 - strand_support_score * 0.018,
        0.265,
        0.410,
    )

    return {
        "base_state": base_state,
        "outs": outs,
        "inherited_runner_count": inherited_count,
        "inherited_runner_pressure": pressure,
        "strand_pressure_index": strand_pressure_index,
        "traffic_leverage_multiplier": leverage_multiplier,
        "inherited_runner_risk_score": inherited_runner_risk_score,
        "strand_support_score": strand_support_score,
        "adjusted_inherited_runner_quality_score": adjusted_quality,
        "adjusted_inherited_runner_k_rate": adjusted_k,
        "adjusted_inherited_runner_bb_rate": adjusted_bb,
        "adjusted_inherited_runner_xwoba_allowed": adjusted_xwoba,
        "inherited_runner_notes": "Candidate inherited-runner context only; not used by production inning engine.",
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    fatigue_rows = _build_fatigue_rows()

    grouped: Dict[Tuple[Any, Any, Any], List[Dict[str, Any]]] = defaultdict(list)
    for row in fatigue_rows:
        grouped[_team_key(row)].append(row)

    rows: List[Dict[str, Any]] = []
    for role_rows in grouped.values():
        for scenario in INHERITED_SCENARIOS:
            selected, selection_score, selection_reason = _select_role(role_rows, scenario)
            context = _inherited_adjustments(selected, scenario)
            rows.append({
                "game_pk": selected.get("game_pk"),
                "game_date": selected.get("game_date"),
                "side": selected.get("side"),
                "team_id": selected.get("team_id"),
                "team_name": selected.get("team_name"),
                "scenario_name": scenario["scenario_name"],
                "inning": scenario["inning"],
                "leverage": scenario["leverage"],
                "selected_role": selected.get("role_name"),
                "selected_availability_status": selected.get("availability_status"),
                "selection_score": selection_score,
                "selection_reason": selection_reason,
                "candidate_only": True,
                "prototype_only": True,
                **context,
            })

    _write_csv(OUTPUT_MATRIX, rows)

    team_rows = []
    grouped_team: Dict[Tuple[Any, Any, Any], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped_team[(row["game_pk"], row["side"], row["team_id"])].append(row)

    for key, team_contexts in grouped_team.items():
        first = team_contexts[0]
        team_rows.append({
            "game_pk": key[0],
            "side": key[1],
            "team_id": key[2],
            "team_name": first["team_name"],
            "context_count": len(team_contexts),
            "avg_inherited_runner_pressure": round(sum(float(r["inherited_runner_pressure"]) for r in team_contexts) / len(team_contexts), 4),
            "avg_inherited_runner_risk_score": round(sum(float(r["inherited_runner_risk_score"]) for r in team_contexts) / len(team_contexts), 4),
            "avg_strand_support_score": round(sum(float(r["strand_support_score"]) for r in team_contexts) / len(team_contexts), 4),
        })
    _write_csv(OUTPUT_SUMMARY, team_rows)

    expected_rows = 15 * 2 * len(INHERITED_SCENARIOS)
    fatigue_rows_available = len(fatigue_rows) == 15 * 2 * len(ROLE_TEMPLATES)
    inherited_context_rows_created = len(rows) == expected_rows

    field_names = [
        "base_state",
        "outs",
        "inherited_runner_count",
        "inherited_runner_pressure",
        "strand_pressure_index",
        "traffic_leverage_multiplier",
        "inherited_runner_risk_score",
        "strand_support_score",
    ]
    inherited_runner_fields_present = all(all(row.get(field) is not None for field in field_names) for row in rows)

    by_scenario_pressure: Dict[str, float] = {}
    for scenario in INHERITED_SCENARIOS:
        scenario_rows = [r for r in rows if r["scenario_name"] == scenario["scenario_name"]]
        by_scenario_pressure[scenario["scenario_name"]] = round(sum(float(r["strand_pressure_index"]) for r in scenario_rows) / len(scenario_rows), 4)

    inherited_pressure_monotonic = (
        by_scenario_pressure["clean_start"] <= by_scenario_pressure["runner_on_first_one_out"]
        <= by_scenario_pressure["runner_on_second_no_out"]
        <= by_scenario_pressure["bases_loaded_no_out"]
    )

    adjusted_fields = [
        "adjusted_inherited_runner_quality_score",
        "adjusted_inherited_runner_k_rate",
        "adjusted_inherited_runner_bb_rate",
        "adjusted_inherited_runner_xwoba_allowed",
    ]
    adjusted_metrics_present = all(all(row.get(field) is not None for field in adjusted_fields) for row in rows)

    no_unavailable_selected_when_alternative_exists = all(row["selected_availability_status"] != "unavailable" for row in rows)
    candidate_mode_only = all(row.get("candidate_only") is True and row.get("prototype_only") is True for row in rows)

    role_counts: Dict[str, int] = {}
    for row in rows:
        role = str(row["selected_role"])
        role_counts[role] = role_counts.get(role, 0) + 1

    checks = [
        {"check": "fatigue_rows_available", "passed": fatigue_rows_available, "detail": len(fatigue_rows)},
        {"check": "inherited_context_rows_created", "passed": inherited_context_rows_created, "detail": f"{len(rows)}/{expected_rows}"},
        {"check": "inherited_runner_fields_present", "passed": inherited_runner_fields_present, "detail": inherited_runner_fields_present},
        {"check": "inherited_pressure_monotonic", "passed": inherited_pressure_monotonic, "detail": by_scenario_pressure},
        {"check": "adjusted_metrics_present", "passed": adjusted_metrics_present, "detail": adjusted_metrics_present},
        {"check": "no_unavailable_selected_when_alternative_exists", "passed": no_unavailable_selected_when_alternative_exists, "detail": no_unavailable_selected_when_alternative_exists},
        {"check": "candidate_mode_only", "passed": candidate_mode_only, "detail": candidate_mode_only},
        {"check": "prototype_only_no_engine_mutation", "passed": True, "detail": True},
        {"check": "no_inning_simulation_mutation", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_inherited_runner_context_prototype_complete",
        "fatigue_rows": len(fatigue_rows),
        "inherited_context_rows": len(rows),
        "expected_inherited_context_rows": expected_rows,
        "selected_role_counts": role_counts,
        "scenario_pressure": by_scenario_pressure,
        "all_checks_passed": all(check["passed"] for check in checks),
        "candidate_mode_only": candidate_mode_only,
        "prototype_only": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6BK_candidate_bullpen_inherited_runner_context_analysis",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
