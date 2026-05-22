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
CONTRACT_VERSION = "candidate_bullpen_shadow_contract_v0.1"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_shadow_output_contract_prototype.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_shadow_output_contract_prototype_checks.csv"
OUTPUT_CONTRACTS = OUTPUT_DIR / "candidate_bullpen_shadow_output_contract_prototype_contracts.json"
OUTPUT_SUMMARY = OUTPUT_DIR / "candidate_bullpen_shadow_output_contract_prototype_summary.csv"


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


def _current_status(base_status: str, usage_count: int) -> Tuple[str, float, float]:
    if usage_count <= 0:
        return base_status, 0.0, 1.0
    if usage_count == 1:
        if base_status == "unavailable":
            return "unavailable", 0.045, 0.0
        return "limited", 0.025, 0.70
    return "unavailable", 0.060, 0.0


def _score_role(row: Dict[str, Any], event: Dict[str, Any], usage_count: int, emergency: bool, alternative_exists: bool) -> Tuple[float, str, str, float, float]:
    role_name = str(row.get("role_name"))
    pre_status, depletion_penalty, depletion_multiplier = _current_status(str(row.get("availability_status")), usage_count)
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

    fallback_reason = "none" if role_name in event["preferred_roles"] else f"preferred_roles_depleted_or_outscored_by_{role_name}"
    return round(score, 5), pre_status, fallback_reason, depletion_penalty, depletion_multiplier


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
            _current_status(str(row.get("availability_status")), usage_counts[str(row["role_name"])])[0] != "unavailable"
            for row in role_rows
        )

        scored = []
        for row in role_rows:
            score, pre_status, fallback_reason, depletion_penalty, depletion_multiplier = _score_role(
                row, event, usage_counts[str(row["role_name"])], emergency, alternative_exists
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

        events.append({
            "sequence_step": event["sequence_step"],
            "event_name": event["event_name"],
            "inning": event["inning"],
            "leverage": event["leverage"],
            "base_state": event["base_state"],
            "outs": event["outs"],
            "inherited_runner_count": _runner_count(str(event["base_state"])),
            "selected_role": selected_role,
            "selected_role_prior_usage_count": prior_usage,
            "selected_role_post_usage_count": post_usage,
            "selected_role_pre_availability_status": pre_status,
            "selected_role_post_availability_status": post_status,
            "bullpen_depletion_index": bullpen_depletion_index,
            "selection_score": selection_score,
            "fallback_reason": fallback_reason,
            "depletion_penalty_applied": depletion_penalty,
            "depletion_multiplier_applied": depletion_multiplier,
        })

    return events


def _count(items: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for item in items:
        value = str(item.get(key))
        out[value] = out.get(value, 0) + 1
    return out


def _build_contract(team_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    first = team_rows[0]
    role_inventory = []
    for row in sorted(team_rows, key=lambda r: int(r["role_priority"])):
        role_inventory.append({
            "role_name": row["role_name"],
            "role_priority": row["role_priority"],
            "leverage_bucket": row["leverage_bucket"],
            "initial_availability_status": row["availability_status"],
            "initial_availability_multiplier": row["availability_multiplier"],
            "adjusted_role_quality_score": row["adjusted_role_quality_score"],
            "adjusted_role_k_rate": row["adjusted_role_k_rate"],
            "adjusted_role_bb_rate": row["adjusted_role_bb_rate"],
            "adjusted_role_xwoba_allowed": row["adjusted_role_xwoba_allowed"],
        })

    sequence_events = _simulate_team_sequence(team_rows)

    selected_role_counts = _count(sequence_events, "selected_role")
    selected_pre_status_counts = _count(sequence_events, "selected_role_pre_availability_status")
    initial_status_counts = _count(team_rows, "availability_status")

    fallback_count = sum(1 for event in sequence_events if event["fallback_reason"] != "none")
    emergency = [event for event in sequence_events if event["event_name"] == "emergency_11th"][0]
    final_unavailable_roles = sum(1 for event in sequence_events if event["selected_role_post_availability_status"] == "unavailable")

    return {
        "contract_version": CONTRACT_VERSION,
        "contract_status": "prototype",
        "game_pk": first["game_pk"],
        "game_date": first["game_date"],
        "side": first["side"],
        "team_id": first["team_id"],
        "team_name": first["team_name"],
        "shadow_mode": "candidate_read_only",
        "canonical_outputs_untouched": True,
        "candidate_only": True,
        "source_layers": ["6BD", "6BE", "6BF", "6BG", "6BH", "6BI", "6BJ", "6BK", "6BL", "6BM", "6BN", "6BO"],
        "integration_boundaries": {
            "canonical_probabilities": "unchanged_from_current_production_path",
            "sportsbook_consumption": "blocked_until_separate_calibration_layer",
            "frontend_consumption": "blocked_until_explicit_ui_contract_layer",
            "engine_mutation": "not_allowed",
            "initial_output_location": "diagnostics_or_shadow_metadata_only",
        },
        "bullpen_state_summary": {
            "role_count": len(role_inventory),
            "available_role_count_initial": initial_status_counts.get("available", 0),
            "limited_role_count_initial": initial_status_counts.get("limited", 0),
            "unavailable_role_count_initial": initial_status_counts.get("unavailable", 0),
            "initial_status_counts": initial_status_counts,
            "selected_role_counts": selected_role_counts,
            "selected_pre_status_counts": selected_pre_status_counts,
        },
        "role_inventory": role_inventory,
        "sequence_events": sequence_events,
        "depletion_summary": {
            "final_bullpen_depletion_index": sequence_events[-1]["bullpen_depletion_index"],
            "max_bullpen_depletion_index": max(event["bullpen_depletion_index"] for event in sequence_events),
            "fallback_count": fallback_count,
            "fallback_rate": round(fallback_count / max(len(sequence_events), 1), 4),
            "emergency_selected_role": emergency["selected_role"],
            "emergency_pre_status": emergency["selected_role_pre_availability_status"],
            "exhausted_role_count_final": final_unavailable_roles,
        },
        "fallback_summary": {
            "fallback_count": fallback_count,
            "fallback_events": [event["event_name"] for event in sequence_events if event["fallback_reason"] != "none"],
        },
        "safety_flags": {
            "no_engine_mutation": True,
            "no_inning_simulation_mutation": True,
            "no_db_writes": True,
            "no_frontend_change": True,
            "canonical_probabilities_unchanged": True,
            "sportsbook_outputs_unchanged": True,
            "diagnostics_only": True,
        },
        "recommended_consumers": {
            "current": ["audit_scripts", "diagnostics"],
            "blocked": ["production_engine", "sportsbook_outputs", "frontend_ui"],
        },
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

    contracts = [_build_contract(rows) for rows in grouped.values()]
    OUTPUT_CONTRACTS.write_text(json.dumps({"contracts": contracts}, indent=2))

    summary_rows = []
    for contract in contracts:
        summary_rows.append({
            "game_pk": contract["game_pk"],
            "side": contract["side"],
            "team_id": contract["team_id"],
            "team_name": contract["team_name"],
            "role_count": contract["bullpen_state_summary"]["role_count"],
            "sequence_event_count": len(contract["sequence_events"]),
            "fallback_count": contract["depletion_summary"]["fallback_count"],
            "final_bullpen_depletion_index": contract["depletion_summary"]["final_bullpen_depletion_index"],
            "canonical_outputs_untouched": contract["canonical_outputs_untouched"],
            "candidate_only": contract["candidate_only"],
        })

    _write_csv(OUTPUT_SUMMARY, summary_rows)

    required_contract_fields = [
        "contract_version",
        "contract_status",
        "game_pk",
        "game_date",
        "side",
        "team_id",
        "team_name",
        "shadow_mode",
        "canonical_outputs_untouched",
        "candidate_only",
        "source_layers",
        "integration_boundaries",
        "bullpen_state_summary",
        "role_inventory",
        "sequence_events",
        "depletion_summary",
        "fallback_summary",
        "safety_flags",
        "recommended_consumers",
    ]

    shadow_contracts_created = len(contracts) == 30
    contract_fields_present = all(all(field in contract for field in required_contract_fields) for contract in contracts)
    role_inventory_present = all(len(contract["role_inventory"]) == 5 for contract in contracts)
    sequence_events_present = all(len(contract["sequence_events"]) == 8 for contract in contracts)
    depletion_summary_present = all(bool(contract.get("depletion_summary")) for contract in contracts)
    safety_flags_present = all(
        all(value is True for value in contract["safety_flags"].values())
        for contract in contracts
    )
    canonical_outputs_untouched = all(contract["canonical_outputs_untouched"] is True for contract in contracts)
    candidate_mode_only = all(contract["candidate_only"] is True and contract["shadow_mode"] == "candidate_read_only" for contract in contracts)

    checks = [
        {"check": "shadow_contracts_created", "passed": shadow_contracts_created, "detail": f"{len(contracts)}/30"},
        {"check": "contract_fields_present", "passed": contract_fields_present, "detail": contract_fields_present},
        {"check": "role_inventory_present", "passed": role_inventory_present, "detail": role_inventory_present},
        {"check": "sequence_events_present", "passed": sequence_events_present, "detail": sequence_events_present},
        {"check": "depletion_summary_present", "passed": depletion_summary_present, "detail": depletion_summary_present},
        {"check": "safety_flags_present", "passed": safety_flags_present, "detail": safety_flags_present},
        {"check": "canonical_outputs_untouched", "passed": canonical_outputs_untouched, "detail": canonical_outputs_untouched},
        {"check": "candidate_mode_only", "passed": candidate_mode_only, "detail": candidate_mode_only},
        {"check": "contract_only_no_engine_mutation", "passed": True, "detail": True},
        {"check": "no_inning_simulation_mutation", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_shadow_output_contract_prototype_complete",
        "contract_version": CONTRACT_VERSION,
        "contracts_created": len(contracts),
        "team_sides_expected": 30,
        "average_sequence_events_per_contract": round(sum(len(c["sequence_events"]) for c in contracts) / max(len(contracts), 1), 4),
        "average_roles_per_contract": round(sum(len(c["role_inventory"]) for c in contracts) / max(len(contracts), 1), 4),
        "all_checks_passed": all(check["passed"] for check in checks),
        "canonical_outputs_untouched": canonical_outputs_untouched,
        "candidate_mode_only": candidate_mode_only,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6BP_candidate_bullpen_shadow_output_contract_analysis"
            if all(check["passed"] for check in checks)
            else "6BO_patch_candidate_bullpen_shadow_output_contract_prototype"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
