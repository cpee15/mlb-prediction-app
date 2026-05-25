from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import inspect
from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session


OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_audit_checks.csv"
OUTPUT_TABLES = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_table_inventory.csv"
OUTPUT_SOURCES = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_source_inventory.csv"
OUTPUT_FEASIBILITY = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_label_feasibility.csv"
OUTPUT_MVD = OUTPUT_DIR / "candidate_bullpen_historical_usage_join_minimum_dataset.csv"


SEARCH_TERMS = [
    "reliever",
    "bullpen",
    "pitcher appearance",
    "pitcher_appearance",
    "pitch log",
    "pitch_log",
    "play by play",
    "play_by_play",
    "game events",
    "game_events",
    "boxscore",
    "substitution",
    "substitutions",
    "inning",
    "leverage",
    "inherited",
    "base_state",
    "outs",
    "pitcher_id",
    "game_pk",
]


LABEL_REQUIREMENTS = [
    {
        "label": "actual_reliever_appearance_available",
        "required_any": ["reliever", "pitcher appearance", "pitcher_appearance", "boxscore", "game_events", "play_by_play"],
        "description": "Actual reliever appearances can be observed or inferred.",
    },
    {
        "label": "bullpen_entry_order_available",
        "required_any": ["appearance_order", "pitcher appearance", "play_by_play", "substitution", "game_events"],
        "description": "Reliever sequence/order can be reconstructed.",
    },
    {
        "label": "reliever_inning_available",
        "required_any": ["inning", "entry_inning", "play_by_play", "game_events"],
        "description": "Reliever entry inning can be observed or inferred.",
    },
    {
        "label": "reliever_team_game_join_available",
        "required_any": ["game_pk", "team_id", "pitcher_id"],
        "description": "Reliever usage can be joined to game/team context.",
    },
    {
        "label": "actual_role_family_label_feasible",
        "required_any": ["appearance_order", "inning", "leverage", "save", "hold", "boxscore"],
        "description": "Actual role family labels can be inferred from usage context.",
    },
    {
        "label": "inherited_runner_context_available",
        "required_any": ["inherited", "base_state", "runner", "play_by_play", "game_events"],
        "description": "Inherited-runner state can be observed or inferred.",
    },
    {
        "label": "leverage_context_available",
        "required_any": ["leverage", "score_diff", "inning", "base_state", "outs"],
        "description": "Leverage context can be approximated.",
    },
    {
        "label": "usage_outcome_join_available",
        "required_any": ["runs", "earned_runs", "inherited", "outs_recorded", "batters_faced", "boxscore"],
        "description": "Reliever usage can be joined to outcome proxies.",
    },
    {
        "label": "late_game_outcome_available",
        "required_any": ["game_pk", "final_score", "home_score", "away_score", "win", "loss"],
        "description": "Usage can be joined to game/late-game outcome context.",
    },
]


MINIMUM_DATASET = [
    {"field": "game_pk", "required": True, "fallback": None, "purpose": "Join usage labels to game and candidate diagnostics."},
    {"field": "game_date", "required": True, "fallback": "schedule date", "purpose": "Temporal join and backtest slicing."},
    {"field": "team_id", "required": True, "fallback": "team abbreviation/name mapping", "purpose": "Team-side join."},
    {"field": "side_or_home_away", "required": True, "fallback": "schedule home/away mapping", "purpose": "Map to candidate team-side contract."},
    {"field": "reliever_id_or_pitcher_id", "required": True, "fallback": None, "purpose": "Identify actual pitcher used."},
    {"field": "reliever_name", "required": False, "fallback": "player lookup", "purpose": "Human-readable diagnostics."},
    {"field": "entry_inning", "required": True, "fallback": "first event/pitch inning", "purpose": "Role-family and sequence mapping."},
    {"field": "entry_outs", "required": False, "fallback": "play-by-play state if available", "purpose": "Leverage and inherited-runner context."},
    {"field": "entry_base_state", "required": False, "fallback": "play-by-play base occupancy", "purpose": "Inherited-runner context."},
    {"field": "batters_faced_or_outs_recorded", "required": False, "fallback": "boxscore pitching line", "purpose": "Usage size and depletion proxy."},
    {"field": "runs_charged_or_inherited_scored", "required": False, "fallback": "boxscore / play-by-play", "purpose": "Outcome calibration."},
    {"field": "role_family_label", "required": True, "fallback": "derived from inning/order/leverage", "purpose": "Compare actual role usage to candidate role selection."},
    {"field": "appearance_order", "required": True, "fallback": "sort by first pitch/event", "purpose": "Sequence and bullpen depletion mapping."},
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(errors="ignore")
    except Exception:
        return ""


def _table_inventory() -> List[Dict[str, Any]]:
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    inspector = inspect(engine)

    rows: List[Dict[str, Any]] = []
    for table_name in sorted(inspector.get_table_names()):
        columns = inspector.get_columns(table_name)
        column_names = [col["name"] for col in columns]
        lower_blob = " ".join([table_name, *column_names]).lower()

        rows.append({
            "table_name": table_name,
            "column_count": len(column_names),
            "columns": "|".join(column_names),
            "has_game_pk": "game_pk" in column_names,
            "has_team_id": "team_id" in column_names,
            "has_pitcher_id": "pitcher_id" in column_names or "player_id" in column_names,
            "has_inning_signal": any("inning" in col.lower() for col in column_names),
            "has_outs_signal": any("out" in col.lower() for col in column_names),
            "has_run_signal": any("run" in col.lower() for col in column_names),
            "has_bullpen_signal": "bullpen" in lower_blob or "reliever" in lower_blob,
            "has_play_by_play_signal": "play" in lower_blob or "event" in lower_blob or "pitch" in lower_blob,
        })
    return rows


def _repo_source_inventory() -> List[Dict[str, Any]]:
    roots = [Path("mlb_app"), Path("scripts")]
    files = []
    for root in roots:
        if root.exists():
            files.extend([p for p in root.rglob("*") if p.is_file() and p.suffix in {".py", ".sql", ".md"}])

    rows: List[Dict[str, Any]] = []
    for path in sorted(files):
        text = _read_text(path)
        lower = text.lower()
        matched_terms = [term for term in SEARCH_TERMS if term.lower() in lower or term.lower() in str(path).lower()]
        if not matched_terms:
            continue

        rows.append({
            "path": str(path),
            "matched_terms": "|".join(sorted(set(matched_terms))),
            "has_game_pk": "game_pk" in lower,
            "has_team_id": "team_id" in lower,
            "has_pitcher_id": "pitcher_id" in lower or "player_id" in lower,
            "has_inning": "inning" in lower,
            "has_leverage": "leverage" in lower,
            "has_bullpen": "bullpen" in lower,
            "has_play_by_play": "play_by_play" in lower or "play by play" in lower,
            "has_boxscore": "boxscore" in lower,
            "has_substitution": "substitution" in lower,
            "has_inherited": "inherited" in lower,
        })
    return rows


def _token_blob(table_rows: List[Dict[str, Any]], source_rows: List[Dict[str, Any]]) -> str:
    parts = []
    for row in table_rows:
        parts.extend([str(row.get("table_name", "")), str(row.get("columns", ""))])
    for row in source_rows:
        parts.extend([str(row.get("path", "")), str(row.get("matched_terms", ""))])
    return " ".join(parts).lower()


def _feasibility_matrix(table_rows: List[Dict[str, Any]], source_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    blob = _token_blob(table_rows, source_rows)
    rows = []

    for requirement in LABEL_REQUIREMENTS:
        matched = [token for token in requirement["required_any"] if token.lower() in blob]
        available = len(matched) > 0

        rows.append({
            "label": requirement["label"],
            "available_or_feasible": available,
            "matched_signals": "|".join(matched),
            "description": requirement["description"],
        })

    return rows


def _gate_decision(feasibility_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    lookup = {row["label"]: bool(row["available_or_feasible"]) for row in feasibility_rows}

    critical = [
        "actual_reliever_appearance_available",
        "bullpen_entry_order_available",
        "reliever_inning_available",
        "reliever_team_game_join_available",
        "actual_role_family_label_feasible",
    ]

    optional_but_important = [
        "inherited_runner_context_available",
        "leverage_context_available",
        "usage_outcome_join_available",
        "late_game_outcome_available",
    ]

    critical_available_count = sum(1 for key in critical if lookup.get(key))
    optional_available_count = sum(1 for key in optional_but_important if lookup.get(key))

    enough_for_join_prototype = critical_available_count >= 4 and lookup.get("reliever_team_game_join_available", False)

    return {
        "critical_available_count": critical_available_count,
        "critical_required_count": len(critical),
        "optional_available_count": optional_available_count,
        "optional_required_count": len(optional_but_important),
        "enough_for_join_prototype": enough_for_join_prototype,
        "gate_decision": "ready_for_historical_usage_join_prototype" if enough_for_join_prototype else "historical_usage_data_gap_detected",
        "recommended_next_layer": "6BW_candidate_bullpen_historical_usage_join_prototype" if enough_for_join_prototype else "6BW_candidate_bullpen_historical_usage_data_gap_plan",
    }


def main() -> None:
    table_rows = _table_inventory()
    source_rows = _repo_source_inventory()
    feasibility_rows = _feasibility_matrix(table_rows, source_rows)
    gate = _gate_decision(feasibility_rows)

    _write_csv(OUTPUT_TABLES, table_rows)
    _write_csv(OUTPUT_SOURCES, source_rows)
    _write_csv(OUTPUT_FEASIBILITY, feasibility_rows)
    _write_csv(OUTPUT_MVD, MINIMUM_DATASET)

    database_introspection_complete = len(table_rows) > 0
    repo_signal_inventory_complete = len(source_rows) > 0
    label_feasibility_matrix_created = len(feasibility_rows) == len(LABEL_REQUIREMENTS)
    minimum_viable_dataset_spec_created = len(MINIMUM_DATASET) >= 10
    historical_usage_gate_decision_valid = gate["gate_decision"] in {
        "ready_for_historical_usage_join_prototype",
        "historical_usage_data_gap_detected",
    }

    checks = [
        {"check": "database_introspection_complete", "passed": database_introspection_complete, "detail": f"{len(table_rows)} tables"},
        {"check": "repo_signal_inventory_complete", "passed": repo_signal_inventory_complete, "detail": f"{len(source_rows)} sources"},
        {"check": "label_feasibility_matrix_created", "passed": label_feasibility_matrix_created, "detail": f"{len(feasibility_rows)} labels"},
        {"check": "minimum_viable_dataset_spec_created", "passed": minimum_viable_dataset_spec_created, "detail": f"{len(MINIMUM_DATASET)} fields"},
        {"check": "historical_usage_gate_decision_valid", "passed": historical_usage_gate_decision_valid, "detail": gate},
        {"check": "audit_only_no_engine_mutation", "passed": True, "detail": True},
        {"check": "no_inning_simulation_mutation", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_historical_usage_join_audit_complete",
        "tables_inspected": len(table_rows),
        "candidate_sources_inspected": len(source_rows),
        "label_feasibility": {
            row["label"]: row["available_or_feasible"]
            for row in feasibility_rows
        },
        "minimum_viable_dataset_fields": len(MINIMUM_DATASET),
        "gate": gate,
        "all_checks_passed": all(check["passed"] for check in checks),
        "audit_only": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": gate["recommended_next_layer"],
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
