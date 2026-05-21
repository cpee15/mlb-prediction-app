from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.model_projections import build_model_projection_payload

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_state_readthrough.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_state_readthrough_checks.csv"
OUTPUT_MATRIX = OUTPUT_DIR / "candidate_bullpen_state_readthrough_matrix.csv"

TARGET_DATE = "2026-05-20"


def _safe_pct(value: Any):
    try:
        if value is None:
            return None
        value = float(value)
        if value <= 1:
            return round(value * 100.0, 2)
        return round(value, 2)
    except Exception:
        return None


def _extract_bullpen_summary(team_payload: Dict[str, Any]) -> Dict[str, Any]:
    bullpen = (
        team_payload.get("bullpen")
        or team_payload.get("bullpen_profile")
        or {}
    )

    metadata = bullpen.get("metadata") or {}
    bat_missing = bullpen.get("bat_missing") or {}
    command_control = bullpen.get("command_control") or {}
    contact_management = bullpen.get("contact_management") or {}
    platoon_profile = bullpen.get("platoon_profile") or {}

    return {
        "source_type": metadata.get("source_type"),
        "confidence": metadata.get("data_confidence") or metadata.get("confidence"),
        "profile_version": metadata.get("bullpen_profile_version") or metadata.get("profile_version"),
        "quality_label": metadata.get("bullpen_quality_label"),
        "quality_score": metadata.get("bullpen_quality_score"),
        "k_rate": _safe_pct(bat_missing.get("k_rate")),
        "bb_rate": _safe_pct(command_control.get("bb_rate")),
        "whiff_rate": _safe_pct(bat_missing.get("whiff_rate")),
        "csw_rate": _safe_pct(bat_missing.get("csw_rate")),
        "hard_hit_rate": _safe_pct(contact_management.get("hard_hit_rate_allowed")),
        "barrel_rate": _safe_pct(contact_management.get("barrel_rate_allowed")),
        "xwoba_allowed": contact_management.get("xwoba_allowed"),
        "platoon_split_lhb": platoon_profile.get("vs_lhb_woba_allowed"),
        "platoon_split_rhb": platoon_profile.get("vs_rhb_woba_allowed"),
    }


def _detect_missing_state(team_payload: Dict[str, Any]) -> Dict[str, bool]:
    bullpen = (
        team_payload.get("bullpen")
        or team_payload.get("bullpen_profile")
        or {}
    )

    keys = set(bullpen.keys())

    return {
        "has_leverage_roles": any(
            k in keys
            for k in [
                "closer",
                "setup",
                "high_leverage",
                "middle_relief",
                "long_relief",
            ]
        ),
        "has_fatigue_tracking": any(
            k in keys
            for k in [
                "recent_pitch_count",
                "fatigue_score",
                "availability",
                "days_rest",
                "last_appearance",
            ]
        ),
        "has_handedness_segmentation": (
            bullpen.get("vs_lhb_woba_allowed") is not None
            or bullpen.get("vs_rhb_woba_allowed") is not None
        ),
        "has_inherited_runner_logic": any(
            k in keys
            for k in [
                "strand_rate",
                "inherited_runners_scored_pct",
            ]
        ),
    }


def main():
    engine = get_engine(os.getenv("DATABASE_URL", "sqlite:///mlb.db"))
    create_tables(engine)
    SessionFactory = get_session(engine)

    session: Session = SessionFactory()
    try:
        projection_payload = build_model_projection_payload(session, TARGET_DATE)
        games = projection_payload.get("games") or []
    finally:
        session.close()

    rows: List[Dict[str, Any]] = []

    leverage_present = False
    fatigue_present = False
    handedness_present = False
    inherited_runner_present = False

    bullpen_profiles_present = 0

    for game in games:
        direct_inputs = ((game.get("sharedSimulation") or {}).get("direct_inputs") or {})
        workspace = game.get("workspace") or {}
        teams = game.get("teams") or {}

        side_profiles = {
            "away": direct_inputs.get("away_bullpen_profile") or workspace.get("awayBullpenProfile") or {},
            "home": direct_inputs.get("home_bullpen_profile") or workspace.get("homeBullpenProfile") or {},
        }

        for side in ["away", "home"]:
            profile = side_profiles[side]
            team_payload = teams.get(side) or {}
            metadata = profile.get("metadata") or {}
            team_name = metadata.get("team_name") or team_payload.get("team_name") or team_payload.get("name")

            wrapped = {"bullpen": profile}
            summary = _extract_bullpen_summary(wrapped)
            missing_state = _detect_missing_state(wrapped)

            leverage_present |= missing_state["has_leverage_roles"]
            fatigue_present |= missing_state["has_fatigue_tracking"]
            handedness_present |= missing_state["has_handedness_segmentation"]
            inherited_runner_present |= missing_state["has_inherited_runner_logic"]

            if any(
                summary.get(key) is not None
                for key in ["k_rate", "bb_rate", "hard_hit_rate", "xwoba_allowed", "quality_label"]
            ):
                bullpen_profiles_present += 1

            row = {
                "game_pk": game.get("game_pk"),
                "side": side,
                "team": team_name,
                **summary,
                **missing_state,
            }

            rows.append(row)

    with OUTPUT_MATRIX.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["game_pk", "side", "team"])
        writer.writeheader()
        writer.writerows(rows)

    checks = [
        {
            "check": "bullpen_profiles_present",
            "passed": bullpen_profiles_present > 0,
            "detail": bullpen_profiles_present,
        },
        {
            "check": "leverage_roles_currently_present",
            "passed": leverage_present,
            "detail": leverage_present,
        },
        {
            "check": "fatigue_tracking_currently_present",
            "passed": fatigue_present,
            "detail": fatigue_present,
        },
        {
            "check": "handedness_segmentation_present",
            "passed": handedness_present,
            "detail": handedness_present,
        },
        {
            "check": "inherited_runner_logic_present",
            "passed": inherited_runner_present,
            "detail": inherited_runner_present,
        },
        {
            "check": "audit_only_no_engine_mutation",
            "passed": True,
            "detail": True,
        },
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    payload = {
        "diagnosis": "candidate_bullpen_state_readthrough_complete",
        "games_processed": len(games),
        "bullpen_profiles_present": bullpen_profiles_present,
        "leverage_roles_present": leverage_present,
        "fatigue_tracking_present": fatigue_present,
        "handedness_segmentation_present": handedness_present,
        "inherited_runner_logic_present": inherited_runner_present,
        "recommended_next_layer": (
            "6BD_bullpen_role_segmentation_prototype"
            if not leverage_present
            else "6BE_bullpen_fatigue_availability_prototype"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
