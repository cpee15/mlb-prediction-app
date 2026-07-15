"""Explicit report-type contracts for the dashboard object model."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional


REPORT_TYPES: Dict[str, Dict[str, Any]] = {
    "all_active_hitters": {"label": "All Active Hitters", "ui_object": "hitters", "base_object": "dashboard_player_current", "population": {"is_active": True, "player_type": "hitter"}, "relationships": [], "queryable": True},
    "all_active_pitchers": {"label": "All Active Pitchers", "ui_object": "pitchers", "base_object": "dashboard_player_current", "population": {"is_active": True, "player_type": "pitcher"}, "relationships": [], "queryable": True},
    "hitters_current_matchup": {"label": "Hitters with Current Matchup Metrics", "ui_object": "hitters", "base_object": "dashboard_players", "population": {"is_active": True, "player_type": "hitter"}, "relationships": ["current_matchup_snapshot"]},
    "hitters_arsenal_splits": {"label": "Hitters with Arsenal Splits", "ui_object": "hitters", "base_object": "dashboard_players", "population": {"is_active": True, "player_type": "hitter"}, "relationships": ["batter_pitch_type_matchups"]},
    "players_lineup_history": {"label": "Players with Lineup History", "ui_object": "overall_players", "base_object": "dashboard_players", "population": {}, "relationships": ["lineup_appearances"]},
    "teams_daily_analysis": {"label": "Teams with Daily Analysis", "ui_object": "teams", "base_object": "teams", "population": {}, "relationships": ["daily_analytical_snapshot"]},
    "games_totals_analysis": {"label": "Games with Totals Analysis", "ui_object": "totals", "base_object": "games", "population": {}, "relationships": ["totals_projection", "run_environment_snapshot"]},
}


def _field(
    name: str,
    label: str,
    data_type: str,
    group: str,
    *,
    sortable: bool = True,
    filterable: bool = True,
    nillable: bool = True,
    operators: Optional[List[str]] = None,
    description: str,
    freshness: str = "current_projection",
    weight_aliases: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "name": name,
        "label": label,
        "data_type": data_type,
        "group": group,
        "sortable": sortable,
        "filterable": filterable,
        "nillable": nillable,
        "source_object": "dashboard_player_current",
        "relationship_path": None,
        "description": description,
        "supported_operators": operators or (["eq", "in"] if data_type == "id" else (["eq", "neq", "in"] if data_type == "string" else ["eq", "gt", "gte", "lt", "lte", "is_null", "is_not_null"])),
        "freshness": freshness,
        "weight_aliases": weight_aliases or [],
    }


CURRENT_PLAYER_FIELDS: List[Dict[str, Any]] = [
    _field("mlb_player_id", "MLB Player ID", "id", "Identity", nillable=False, description="Canonical MLBAM player identifier.", freshness="canonical"),
    _field("full_name", "Player Name", "string", "Identity", nillable=False, operators=["eq", "neq", "contains", "in"], description="Resolved canonical player name.", freshness="canonical"),
    _field("player_type", "Player Type", "string", "Identity", nillable=False, description="Canonical hitter or pitcher classification.", freshness="canonical"),
    _field("team_id", "Team ID", "id", "Team", description="Current MLB team identifier.", freshness="canonical"),
    _field("team_name", "Team", "string", "Team", operators=["eq", "neq", "contains", "in"], description="Current MLB team name or source abbreviation.", freshness="canonical"),
    _field("primary_position", "Primary Position", "string", "Identity", description="Current primary-position abbreviation.", freshness="canonical"),
    _field("model_score", "Model Score", "double", "Scoring", description="Approved base model score before request-scoped weights.", weight_aliases=["Score"]),
    _field("confidence", "Confidence", "string", "Scoring", operators=["eq", "neq", "in", "gt", "gte", "lt", "lte"], description="Approved model confidence label."),
    _field("xwoba", "xwOBA", "double", "Hitting", description="Current approved expected weighted on-base average.", weight_aliases=["xwOBA", "xwOBA Allowed"]),
    _field("xba", "xBA", "double", "Hitting", description="Current approved expected batting average.", weight_aliases=["xBA", "xBA Allowed"]),
    _field("exit_velocity", "Exit Velocity", "double", "Contact", description="Current approved average exit velocity.", weight_aliases=["EV", "Exit Velocity"]),
    _field("launch_angle", "Launch Angle", "double", "Contact", description="Current approved average launch angle.", weight_aliases=["LA", "Launch Angle"]),
    _field("hard_hit_rate", "Hard-Hit Rate", "double", "Contact", description="Current approved hard-hit rate.", weight_aliases=["HardHit", "HardHit Allowed"]),
    _field("barrel_rate", "Barrel Rate", "double", "Contact", description="Current approved barrel rate.", weight_aliases=["Barrel"]),
    _field("strikeout_rate", "Strikeout Rate", "double", "Discipline", description="Current approved strikeout rate.", weight_aliases=["K%"]),
    _field("walk_rate", "Walk Rate", "double", "Discipline", description="Current approved walk rate.", weight_aliases=["BB%"]),
    _field("iso", "ISO", "double", "Production", description="Current approved isolated power.", weight_aliases=["ISO"]),
    _field("obp", "OBP", "double", "Production", description="Current approved on-base percentage.", weight_aliases=["OBP"]),
    _field("slg", "SLG", "double", "Production", description="Current approved slugging percentage.", weight_aliases=["SLG"]),
    _field("plate_appearances", "Plate Appearances", "integer", "Sample", description="Plate appearances for the approved analytical context.", weight_aliases=["PA", "Pitches Seen"]),
    _field("projection_version", "Projection Version", "string", "Audit", filterable=False, description="Atomic projection batch version."),
    _field("promoted_at", "Promoted At", "datetime", "Audit", filterable=False, description="Timestamp when this current row was promoted."),
    _field("updated_at", "Updated At", "datetime", "Audit", filterable=False, description="Timestamp when this current row last changed."),
    _field("metrics", "Extended Metrics", "json", "Audit", sortable=False, filterable=False, description="Explicitly supported extensible scalar metrics."),
]


FIELD_CATALOG: Dict[str, List[Dict[str, Any]]] = {}
for key, config in REPORT_TYPES.items():
    if config["base_object"] == "dashboard_player_current":
        FIELD_CATALOG[key] = deepcopy(CURRENT_PLAYER_FIELDS)
    elif config["base_object"] == "dashboard_players":
        FIELD_CATALOG[key] = deepcopy(CURRENT_PLAYER_FIELDS[:6])
        for field in FIELD_CATALOG[key]:
            field["source_object"] = "dashboard_players"
    else:
        FIELD_CATALOG[key] = []


def describe_report_type(report_type: str) -> Dict[str, Any]:
    if report_type not in REPORT_TYPES:
        raise ValueError(f"Unsupported report type: {report_type}")
    return {
        **deepcopy(REPORT_TYPES[report_type]),
        "api_name": report_type,
        "queryable": bool(REPORT_TYPES[report_type].get("queryable")),
        "fields": deepcopy(FIELD_CATALOG[report_type]),
    }


def list_report_types() -> List[Dict[str, Any]]:
    return [describe_report_type(name) for name in REPORT_TYPES]
