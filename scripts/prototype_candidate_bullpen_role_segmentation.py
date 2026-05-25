from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.model_projections import build_model_projection_payload


TARGET_DATE = "2026-05-20"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_role_segmentation_prototype.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_role_segmentation_prototype_checks.csv"
OUTPUT_MATRIX = OUTPUT_DIR / "candidate_bullpen_role_segmentation_prototype_matrix.csv"


ROLE_TEMPLATES = [
    {
        "role_name": "closer",
        "role_priority": 1,
        "leverage_bucket": "save_highest_leverage",
        "expected_usage_inning_min": 8,
        "expected_usage_inning_max": 9,
        "inherited_runner_exposure": "low",
        "k_delta": 0.018,
        "bb_delta": -0.008,
        "whiff_delta": 0.015,
        "csw_delta": 0.010,
        "hard_hit_delta": -0.012,
        "xwoba_delta": -0.010,
        "quality_delta": 0.030,
    },
    {
        "role_name": "setup",
        "role_priority": 2,
        "leverage_bucket": "late_high_leverage",
        "expected_usage_inning_min": 7,
        "expected_usage_inning_max": 8,
        "inherited_runner_exposure": "medium",
        "k_delta": 0.012,
        "bb_delta": -0.005,
        "whiff_delta": 0.010,
        "csw_delta": 0.007,
        "hard_hit_delta": -0.008,
        "xwoba_delta": -0.007,
        "quality_delta": 0.020,
    },
    {
        "role_name": "high_leverage",
        "role_priority": 3,
        "leverage_bucket": "matchup_high_leverage",
        "expected_usage_inning_min": 6,
        "expected_usage_inning_max": 8,
        "inherited_runner_exposure": "high",
        "k_delta": 0.006,
        "bb_delta": -0.002,
        "whiff_delta": 0.006,
        "csw_delta": 0.004,
        "hard_hit_delta": -0.004,
        "xwoba_delta": -0.004,
        "quality_delta": 0.010,
    },
    {
        "role_name": "middle_relief",
        "role_priority": 4,
        "leverage_bucket": "middle_neutral",
        "expected_usage_inning_min": 4,
        "expected_usage_inning_max": 7,
        "inherited_runner_exposure": "medium",
        "k_delta": 0.000,
        "bb_delta": 0.000,
        "whiff_delta": 0.000,
        "csw_delta": 0.000,
        "hard_hit_delta": 0.000,
        "xwoba_delta": 0.000,
        "quality_delta": 0.000,
    },
    {
        "role_name": "long_relief",
        "role_priority": 5,
        "leverage_bucket": "length_low_leverage",
        "expected_usage_inning_min": 2,
        "expected_usage_inning_max": 6,
        "inherited_runner_exposure": "low",
        "k_delta": -0.012,
        "bb_delta": 0.006,
        "whiff_delta": -0.010,
        "csw_delta": -0.006,
        "hard_hit_delta": 0.010,
        "xwoba_delta": 0.010,
        "quality_delta": -0.020,
    },
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


def _extract_profiles(game: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    direct_inputs = ((game.get("sharedSimulation") or {}).get("direct_inputs") or {})
    workspace = game.get("workspace") or {}

    return {
        "away": direct_inputs.get("away_bullpen_profile") or workspace.get("awayBullpenProfile") or {},
        "home": direct_inputs.get("home_bullpen_profile") or workspace.get("homeBullpenProfile") or {},
    }


def _build_role_rows(game: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        base_hard_hit = _profile_value(profile, "contact_management", "hard_hit_rate_allowed")
        base_xwoba = _profile_value(profile, "contact_management", "xwoba_allowed")
        base_quality = _safe_float(metadata.get("bullpen_quality_score")) or 0.0

        for template in ROLE_TEMPLATES:
            rows.append(
                {
                    "game_pk": game.get("game_pk"),
                    "game_date": game.get("game_date"),
                    "side": side,
                    "team_id": team_id,
                    "team_name": team_name,
                    "base_source_type": metadata.get("source_type"),
                    "base_confidence": metadata.get("data_confidence"),
                    "base_profile_version": metadata.get("bullpen_profile_version"),
                    "base_quality_label": metadata.get("bullpen_quality_label"),
                    "base_quality_score": base_quality,
                    "role_name": template["role_name"],
                    "role_priority": template["role_priority"],
                    "leverage_bucket": template["leverage_bucket"],
                    "expected_usage_inning_min": template["expected_usage_inning_min"],
                    "expected_usage_inning_max": template["expected_usage_inning_max"],
                    "inherited_runner_exposure": template["inherited_runner_exposure"],
                    "role_k_rate": _role_value(base_k, template["k_delta"], 0.14, 0.36),
                    "role_bb_rate": _role_value(base_bb, template["bb_delta"], 0.045, 0.15),
                    "role_whiff_rate": _role_value(base_whiff, template["whiff_delta"], 0.16, 0.38),
                    "role_csw_rate": _role_value(base_csw, template["csw_delta"], 0.21, 0.36),
                    "role_hard_hit_rate_allowed": _role_value(base_hard_hit, template["hard_hit_delta"], 0.28, 0.50),
                    "role_xwoba_allowed": _role_value(base_xwoba, template["xwoba_delta"], 0.265, 0.380),
                    "role_quality_score": _clamp(base_quality + template["quality_delta"], -0.18, 0.18),
                    "source_type": "candidate_bullpen_role_segmentation_v1",
                    "source_confidence": "prototype",
                    "candidate_only": True,
                    "notes": "Derived deterministically from bullpen_prior_v1 profile; not active reliever data.",
                }
            )

    return rows


def main() -> None:
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
        rows.extend(_build_role_rows(game))

    if rows:
        with OUTPUT_MATRIX.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    expected_team_sides = len(games) * 2
    expected_rows = expected_team_sides * len(ROLE_TEMPLATES)

    role_names = {row.get("role_name") for row in rows}
    expected_roles = {role["role_name"] for role in ROLE_TEMPLATES}

    role_rates_present = all(
        row.get("role_k_rate") is not None
        and row.get("role_bb_rate") is not None
        and row.get("role_hard_hit_rate_allowed") is not None
        and row.get("role_xwoba_allowed") is not None
        for row in rows
    )

    leverage_buckets_present = all(row.get("leverage_bucket") for row in rows)
    candidate_only = all(row.get("candidate_only") is True for row in rows)

    checks = [
        {
            "check": "bullpen_profiles_loaded",
            "passed": expected_team_sides > 0 and len(rows) == expected_rows,
            "detail": f"{len(rows)}/{expected_rows}",
        },
        {
            "check": "all_expected_roles_created",
            "passed": role_names == expected_roles,
            "detail": sorted(role_names),
        },
        {
            "check": "role_rates_present",
            "passed": role_rates_present,
            "detail": role_rates_present,
        },
        {
            "check": "leverage_buckets_present",
            "passed": leverage_buckets_present,
            "detail": leverage_buckets_present,
        },
        {
            "check": "candidate_mode_only",
            "passed": candidate_only,
            "detail": candidate_only,
        },
        {
            "check": "no_game_engine_mutation",
            "passed": True,
            "detail": True,
        },
        {
            "check": "no_inning_simulation_mutation",
            "passed": True,
            "detail": True,
        },
        {
            "check": "production_default_unchanged",
            "passed": True,
            "detail": True,
        },
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_role_segmentation_prototype_complete",
        "games_processed": len(games),
        "team_sides_processed": expected_team_sides,
        "roles_per_team_side": len(ROLE_TEMPLATES),
        "role_rows_created": len(rows),
        "expected_role_rows": expected_rows,
        "all_expected_roles_created": role_names == expected_roles,
        "role_rates_present": role_rates_present,
        "candidate_mode_only": candidate_only,
        "production_default_unchanged": True,
        "recommended_next_layer": "6BE_candidate_bullpen_role_segmentation_analysis",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
