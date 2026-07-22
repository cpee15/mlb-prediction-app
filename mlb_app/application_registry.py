"""Code-owned registry of user-facing MLBGPT application surfaces."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List


APPLICATION_SURFACES: List[Dict[str, Any]] = [
    {"key": "matchups", "label": "Matchups", "route": "/", "visibility": "public", "feature_status": "available", "health_classification": "core"},
    {"key": "daily_odds", "label": "Daily Odds", "route": "/daily-odds", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "bet105", "label": "Bet105 Sportsbook", "route": "/sportsbook/bet105", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "news", "label": "News", "route": "/news", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "model_projections", "label": "Model Projections", "route": "/models/projections", "visibility": "public", "feature_status": "available", "health_classification": "core"},
    {"key": "my_dashboard", "label": "My Dashboard", "route": "/my-dashboard", "visibility": "authenticated", "feature_status": "available", "health_classification": "core"},
    {"key": "standings", "label": "Standings", "route": "/standings", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "pitcher", "label": "Pitcher", "route": "/pitcher", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "batter", "label": "Batter", "route": "/batter", "visibility": "public", "feature_status": "conditional", "health_classification": "guarded", "feature_flag": "VITE_ENABLE_BATTER_PAGE"},
    {"key": "team", "label": "Team", "route": "/team", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "calendar", "label": "Calendar", "route": "/calendar", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "ai_data_assistant", "label": "AI Data Assistant", "route": "/ai-data-assistant", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "live", "label": "Live", "route": "/live", "visibility": "public", "feature_status": "available", "health_classification": "standard"},
    {"key": "model_tracker", "label": "Model Tracker", "route": "/model-tracker", "visibility": "public", "feature_status": "available", "health_classification": "operational"},
    {"key": "control_center", "label": "Control Center", "route": "/admin", "visibility": "admin", "feature_status": "phase_1_read_only", "health_classification": "private"},
]


def list_application_surfaces() -> List[Dict[str, Any]]:
    return deepcopy(APPLICATION_SURFACES)
