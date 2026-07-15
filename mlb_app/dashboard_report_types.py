"""Explicit report-type contracts for the dashboard object model."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List


REPORT_TYPES: Dict[str, Dict[str, Any]] = {
    "all_active_hitters": {"label": "All Active Hitters", "ui_object": "hitters", "base_object": "dashboard_player_current", "population": {"is_active": True, "player_type": "hitter"}, "relationships": []},
    "all_active_pitchers": {"label": "All Active Pitchers", "ui_object": "pitchers", "base_object": "dashboard_player_current", "population": {"is_active": True, "player_type": "pitcher"}, "relationships": []},
    "hitters_current_matchup": {"label": "Hitters with Current Matchup Metrics", "ui_object": "hitters", "base_object": "dashboard_players", "population": {"is_active": True, "player_type": "hitter"}, "relationships": ["current_matchup_snapshot"]},
    "hitters_arsenal_splits": {"label": "Hitters with Arsenal Splits", "ui_object": "hitters", "base_object": "dashboard_players", "population": {"is_active": True, "player_type": "hitter"}, "relationships": ["batter_pitch_type_matchups"]},
    "players_lineup_history": {"label": "Players with Lineup History", "ui_object": "overall_players", "base_object": "dashboard_players", "population": {}, "relationships": ["lineup_appearances"]},
    "teams_daily_analysis": {"label": "Teams with Daily Analysis", "ui_object": "teams", "base_object": "teams", "population": {}, "relationships": ["daily_analytical_snapshot"]},
    "games_totals_analysis": {"label": "Games with Totals Analysis", "ui_object": "totals", "base_object": "games", "population": {}, "relationships": ["totals_projection", "run_environment_snapshot"]},
}

FIELD_CATALOG: Dict[str, List[Dict[str, Any]]] = {
    key: [
        {"name": "mlb_player_id", "label": "MLB Player ID", "data_type": "id", "group": "Identity", "sortable": True, "filterable": True, "nillable": False, "source_object": config["base_object"], "relationship_path": None, "supported_operators": ["eq", "in"], "description": "Canonical MLBAM player identifier.", "freshness": "canonical"},
        {"name": "full_name", "label": "Player Name", "data_type": "string", "group": "Identity", "sortable": True, "filterable": True, "nillable": False, "source_object": config["base_object"], "relationship_path": None, "supported_operators": ["eq", "contains"], "description": "Resolved canonical player name.", "freshness": "canonical"},
    ] if config["base_object"] in {"dashboard_players", "dashboard_player_current"} else []
    for key, config in REPORT_TYPES.items()
}


def describe_report_type(report_type: str) -> Dict[str, Any]:
    if report_type not in REPORT_TYPES:
        raise ValueError(f"Unsupported report type: {report_type}")
    return {**deepcopy(REPORT_TYPES[report_type]), "api_name": report_type, "fields": deepcopy(FIELD_CATALOG[report_type])}
