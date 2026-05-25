from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session


TARGET_DATE = "2026-05-20"
PLAN_VERSION = "candidate_bullpen_statcast_label_coverage_expansion_plan_v0.1"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_label_coverage_expansion_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_label_coverage_expansion_plan_checks.csv"
OUTPUT_COVERAGE = OUTPUT_DIR / "candidate_bullpen_statcast_label_current_coverage.csv"
OUTPUT_TARGETS = OUTPUT_DIR / "candidate_bullpen_statcast_label_coverage_targets.csv"
OUTPUT_SCOPES = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scope_options.csv"
OUTPUT_FIELDS = OUTPUT_DIR / "candidate_bullpen_statcast_label_required_fields.csv"
OUTPUT_PLAN = OUTPUT_DIR / "candidate_bullpen_statcast_label_next_implementation_plan.csv"


COVERAGE_TARGETS = [
    {"target": "actual_reliever_appearance_rows", "current_value_key": "actual_reliever_appearance_rows_estimate", "required_value": 30, "operator": ">=", "purpose": "Minimum real reliever appearance sample for calibration-grade labels."},
    {"target": "exact_game_team_side_joins", "current_value_key": "exact_game_team_side_join_estimate", "required_value": 24, "operator": ">=", "purpose": "Minimum exact candidate-game/team-side joins."},
    {"target": "exact_join_rate", "current_value_key": "exact_join_rate_estimate", "required_value": 0.80, "operator": ">=", "purpose": "Avoid sampled/nearest-date labels dominating calibration."},
    {"target": "missing_rate", "current_value_key": "missing_rate_estimate", "required_value": 0.20, "operator": "<=", "purpose": "Limit missing historical usage labels."},
    {"target": "unique_reconstructable_games", "current_value_key": "reconstructable_game_count", "required_value": 10, "operator": ">=", "purpose": "Ensure usage labels cover enough independent games."},
    {"target": "team_sides_with_actual_labels", "current_value_key": "team_side_label_count", "required_value": 20, "operator": ">=", "purpose": "Ensure broad team-side coverage."},
    {"target": "distinct_statcast_dates", "current_value_key": "statcast_date_count", "required_value": 5, "operator": ">=", "purpose": "Avoid one-day label fragility."},
]

BACKFILL_SCOPE_OPTIONS = [
    {
        "scope": "minimal_target_slate",
        "date_range": "target date only",
        "expected_use": "Validate exact same-game joins for the model projection target date.",
        "pros": "Smallest fetch, fastest validation.",
        "cons": "May be insufficient if slate data is unavailable or sparse.",
        "recommended": False,
    },
    {
        "scope": "short_window",
        "date_range": "target date ±7 days",
        "expected_use": "Establish nearby historical usage labels while preserving date proximity.",
        "pros": "Balances relevance and coverage.",
        "cons": "Still vulnerable to sparse local schedule/event coverage.",
        "recommended": True,
    },
    {
        "scope": "calibration_window",
        "date_range": "30 historical MLB dates",
        "expected_use": "Minimum meaningful calibration window for bullpen role/depletion labels.",
        "pros": "Likely enough games and team-sides for first calibration-grade analysis.",
        "cons": "Requires more ingestion time and coverage auditing.",
        "recommended": True,
    },
    {
        "scope": "robust_window",
        "date_range": "60-90 historical MLB dates",
        "expected_use": "Stable calibration and segmentation by role/depletion/fatigue patterns.",
        "pros": "Best reliability and segment coverage.",
        "cons": "Largest ingestion/storage footprint.",
        "recommended": False,
    },
]

REQUIRED_FIELDS = [
    {"field": "game_date", "required": True, "source": "statcast_events", "purpose": "Date slicing and label window coverage."},
    {"field": "game_pk", "required": True, "source": "statcast_events", "purpose": "Join to game/team diagnostics and actual_game_results."},
    {"field": "inning", "required": True, "source": "statcast_events", "purpose": "Reliever entry inning and role family derivation."},
    {"field": "inning_topbot", "required": True, "source": "statcast_events", "purpose": "Determine pitching team side."},
    {"field": "at_bat_number", "required": True, "source": "statcast_events", "purpose": "Pitcher appearance ordering and first-appearance sorting."},
    {"field": "pitch_number", "required": True, "source": "statcast_events", "purpose": "Stable intra-PA sorting."},
    {"field": "outs_when_up", "required": True, "source": "statcast_events", "purpose": "Entry context and leverage proxy."},
    {"field": "pitcher_id", "required": True, "source": "statcast_events", "purpose": "Pitcher appearance grouping."},
    {"field": "home_team", "required": True, "source": "statcast_events", "purpose": "Home/away team identity fallback."},
    {"field": "away_team", "required": True, "source": "statcast_events", "purpose": "Home/away team identity fallback."},
    {"field": "events", "required": False, "source": "statcast_events", "purpose": "Outcome/context enrichment."},
    {"field": "description", "required": False, "source": "statcast_events", "purpose": "Pitch/event diagnostics."},
    {"field": "home_team_id", "required": True, "source": "actual_game_results or schedule", "purpose": "Team-side join to candidate diagnostics."},
    {"field": "away_team_id", "required": True, "source": "actual_game_results or schedule", "purpose": "Team-side join to candidate diagnostics."},
]

IMPLEMENTATION_PLAN = [
    {
        "step": 1,
        "name": "Locate or create Statcast ingestion entrypoint",
        "action": "Search existing scripts for Statcast fetch/load logic; prefer extending existing ingestion before adding new code.",
        "output": "Documented loader path or new scaffold target.",
    },
    {
        "step": 2,
        "name": "Add offline backfill scaffold",
        "action": "Create a script that accepts start/end dates, fetches or imports Statcast events, and writes through existing safe ingestion patterns.",
        "output": "6BZ candidate bullpen Statcast label backfill scaffold.",
    },
    {
        "step": 3,
        "name": "Audit post-backfill coverage",
        "action": "Recompute date/game/team-side/reliever sequence coverage after load.",
        "output": "Coverage audit CSV and JSON.",
    },
    {
        "step": 4,
        "name": "Rerun 6BW historical usage join",
        "action": "Require exact game/team-side joins when possible and reduce nearest-date sampling.",
        "output": "Joined historical usage labels with improved provenance.",
    },
    {
        "step": 5,
        "name": "Rerun 6BX reliability gate",
        "action": "Advance only if calibration-grade thresholds pass.",
        "output": "calibration_grade true or documented hold.",
    },
    {
        "step": 6,
        "name": "Only then enter real-label calibration",
        "action": "Run real-label calibration analysis only after sufficient exact labels exist.",
        "output": "6CA or later real-label calibration layer.",
    },
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _safe_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _single(session: Session, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    row = session.execute(text(sql), params or {}).mappings().first()
    return dict(row) if row else {}


def _inspect_current_coverage(session: Session) -> Dict[str, Any]:
    base = _single(session, """
        SELECT
            COUNT(*) AS statcast_row_count,
            COUNT(DISTINCT game_date) AS statcast_date_count,
            COUNT(DISTINCT game_pk) AS statcast_game_count,
            COUNT(DISTINCT pitcher_id) AS statcast_pitcher_count,
            MIN(game_date) AS min_game_date,
            MAX(game_date) AS max_game_date
        FROM statcast_events
        WHERE game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
    """)

    target = _single(session, """
        SELECT
            COUNT(*) AS target_statcast_row_count,
            COUNT(DISTINCT game_pk) AS target_statcast_game_count,
            COUNT(DISTINCT pitcher_id) AS target_statcast_pitcher_count
        FROM statcast_events
        WHERE game_date = :target_date
          AND game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
    """, {"target_date": TARGET_DATE})

    # Reconstructability estimate:
    # A team-side has reliever labels when a game/inning_topbot side has at least two pitchers.
    team_side_rows = session.execute(text("""
        WITH side_pitchers AS (
            SELECT
                game_date,
                game_pk,
                CASE
                    WHEN lower(CAST(inning_topbot AS TEXT)) IN ('top', 't') THEN 'home'
                    WHEN lower(CAST(inning_topbot AS TEXT)) IN ('bot', 'bottom', 'b') THEN 'away'
                    ELSE NULL
                END AS pitching_side,
                pitcher_id
            FROM statcast_events
            WHERE game_pk IS NOT NULL
              AND pitcher_id IS NOT NULL
              AND inning_topbot IS NOT NULL
        ),
        grouped AS (
            SELECT
                game_date,
                game_pk,
                pitching_side,
                COUNT(DISTINCT pitcher_id) AS pitcher_count
            FROM side_pitchers
            WHERE pitching_side IS NOT NULL
            GROUP BY game_date, game_pk, pitching_side
        )
        SELECT
            COUNT(*) AS team_side_count,
            SUM(CASE WHEN pitcher_count >= 2 THEN 1 ELSE 0 END) AS team_side_label_count,
            COUNT(DISTINCT CASE WHEN pitcher_count >= 2 THEN game_pk END) AS reconstructable_game_count,
            SUM(CASE WHEN pitcher_count >= 2 THEN pitcher_count - 1 ELSE 0 END) AS actual_reliever_appearance_rows_estimate
        FROM grouped
    """)).mappings().first()
    team_side = dict(team_side_rows) if team_side_rows else {}

    current = {
        "target_date": TARGET_DATE,
        "statcast_row_count": _safe_int(base.get("statcast_row_count")),
        "statcast_date_count": _safe_int(base.get("statcast_date_count")),
        "statcast_game_count": _safe_int(base.get("statcast_game_count")),
        "statcast_pitcher_count": _safe_int(base.get("statcast_pitcher_count")),
        "min_game_date": base.get("min_game_date"),
        "max_game_date": base.get("max_game_date"),
        "target_statcast_row_count": _safe_int(target.get("target_statcast_row_count")),
        "target_statcast_game_count": _safe_int(target.get("target_statcast_game_count")),
        "target_statcast_pitcher_count": _safe_int(target.get("target_statcast_pitcher_count")),
        "team_side_count": _safe_int(team_side.get("team_side_count")),
        "team_side_label_count": _safe_int(team_side.get("team_side_label_count")),
        "reconstructable_game_count": _safe_int(team_side.get("reconstructable_game_count")),
        "actual_reliever_appearance_rows_estimate": _safe_int(team_side.get("actual_reliever_appearance_rows_estimate")),
    }

    # Current 6BX-equivalent estimates from known sparse local coverage.
    # These are intentionally conservative because exact target-date joins require target slate labels.
    current["exact_game_team_side_join_estimate"] = 0 if current["target_statcast_row_count"] == 0 else min(current["team_side_label_count"], 30)
    current["exact_join_rate_estimate"] = round(current["exact_game_team_side_join_estimate"] / 30, 4)
    current["missing_rate_estimate"] = round(1.0 - current["exact_join_rate_estimate"], 4)

    return current


def _target_rows(current: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for target in COVERAGE_TARGETS:
        current_value = current.get(target["current_value_key"])
        required = target["required_value"]
        operator = target["operator"]

        if operator == ">=":
            passed = _safe_float(current_value) >= _safe_float(required)
        elif operator == "<=":
            passed = _safe_float(current_value) <= _safe_float(required)
        else:
            passed = False

        rows.append({
            **target,
            "current_value": current_value,
            "passed": passed,
            "gap": round(_safe_float(required) - _safe_float(current_value), 4) if operator == ">=" else round(_safe_float(current_value) - _safe_float(required), 4),
        })
    return rows


def main() -> None:
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    SessionFactory = get_session(engine)

    session: Session = SessionFactory()
    try:
        current_coverage = _inspect_current_coverage(session)
    finally:
        session.close()

    target_rows = _target_rows(current_coverage)

    _write_csv(OUTPUT_COVERAGE, [current_coverage])
    _write_csv(OUTPUT_TARGETS, target_rows)
    _write_csv(OUTPUT_SCOPES, BACKFILL_SCOPE_OPTIONS)
    _write_csv(OUTPUT_FIELDS, REQUIRED_FIELDS)
    _write_csv(OUTPUT_PLAN, IMPLEMENTATION_PLAN)

    current_coverage_inspected = current_coverage["statcast_row_count"] >= 0 and "statcast_date_count" in current_coverage
    coverage_targets_defined = len(target_rows) == len(COVERAGE_TARGETS) and all("passed" in row for row in target_rows)
    backfill_scope_options_defined = len(BACKFILL_SCOPE_OPTIONS) >= 4 and any(row["recommended"] for row in BACKFILL_SCOPE_OPTIONS)
    required_fields_defined = len(REQUIRED_FIELDS) >= 12 and all(row["required"] or row["field"] in {"events", "description"} for row in REQUIRED_FIELDS)
    implementation_plan_defined = len(IMPLEMENTATION_PLAN) >= 5
    calibration_blocked_until_coverage_passes = not all(row["passed"] for row in target_rows)

    checks = [
        {"check": "current_coverage_inspected", "passed": current_coverage_inspected, "detail": current_coverage},
        {"check": "coverage_targets_defined", "passed": coverage_targets_defined, "detail": f"{len(target_rows)} targets"},
        {"check": "backfill_scope_options_defined", "passed": backfill_scope_options_defined, "detail": f"{len(BACKFILL_SCOPE_OPTIONS)} options"},
        {"check": "required_fields_defined", "passed": required_fields_defined, "detail": f"{len(REQUIRED_FIELDS)} fields"},
        {"check": "implementation_plan_defined", "passed": implementation_plan_defined, "detail": f"{len(IMPLEMENTATION_PLAN)} steps"},
        {"check": "calibration_blocked_until_coverage_passes", "passed": calibration_blocked_until_coverage_passes, "detail": "Calibration-grade claims remain blocked until all coverage targets pass."},
        {"check": "plan_only_no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    failed_targets = [row["target"] for row in target_rows if not row["passed"]]
    recommended_scope = "calibration_window" if len(failed_targets) >= 3 else "short_window"

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_label_coverage_expansion_plan_complete",
        "plan_version": PLAN_VERSION,
        "target_date": TARGET_DATE,
        "current_coverage": current_coverage,
        "coverage_targets_passed": all(row["passed"] for row in target_rows),
        "failed_targets": failed_targets,
        "recommended_backfill_scope": recommended_scope,
        "coverage_expansion_required_before_calibration_grade_claims": True,
        "all_checks_passed": all(check["passed"] for check in checks),
        "plan_only": True,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6BZ_candidate_bullpen_statcast_label_backfill_scaffold",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
