from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, List, Tuple

from sqlalchemy import inspect
from sqlalchemy.orm import Session
from sqlalchemy.sql.sqltypes import Date, DateTime, Float, Integer, Numeric, String, Text

from .best_plays_engine import build_best_plays_payload, normalize_best_play_rows
from .matchup_generator import generate_matchups_for_date
from .model_projections import build_model_projection_payload
from .my_dashboard_solver import build_dashboard_solver_payload
from .model_tracker_bet105 import as_model_signals, build_bet105_decisions
from .model_tracker import (
    AI_TRACKER_PROMPTS,
    DASHBOARD_COMPONENTS,
    ModelTrackerSnapshot,
    _safe_float,
    _safe_int,
    _tracker_key,
    normalize_ai_data_assistant_rows,
    normalize_daily_odds_rows,
    normalize_dashboard_rows,
    normalize_matchup_rows,
    normalize_model_projection_rows,
)
from .model_tracker_bet105 import as_model_signals, build_bet105_decisions

JSON_COLUMNS = {"reasoning_json", "features_used_json", "missing_inputs_json", "raw_payload_json", "actual_result_json"}
INTEGER_COLUMNS = {"game_pk", "team_id", "player_id"}
FLOAT_COLUMNS = {
    "model_probability",
    "market_implied_probability",
    "edge",
    "score",
    "confidence",
    "line",
    "price",
    "expected_value",
    "projected_total",
    "projected_home_runs",
    "projected_away_runs",
    "home_win_probability",
    "away_win_probability",
}


def _json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str, sort_keys=True)
    except Exception:
        return json.dumps(str(value))


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list, tuple, set)):
        return _json(value)
    return str(value)


def _db_column_types(session: Session) -> Dict[str, Any]:
    """Return the live database column types for model_tracker_snapshots.

    Production can keep an older physical table even if the ORM definition changed.
    The snapshot insert path must therefore coerce values based on the actual DB
    column type, not only the Python model attribute name.
    """
    try:
        bind = session.get_bind()
        inspector = inspect(bind)
        columns = inspector.get_columns(ModelTrackerSnapshot.__tablename__)
        return {str(column.get("name")): column.get("type") for column in columns}
    except Exception:
        return {}


def _coerce_by_db_type(key: str, value: Any, db_type: Any) -> Any:
    if value is None:
        return None
    if isinstance(db_type, Integer):
        return _safe_int(value)
    if isinstance(db_type, (Float, Numeric)):
        return _safe_float(value)
    if isinstance(db_type, DateTime):
        return value
    if isinstance(db_type, Date):
        if isinstance(value, str):
            return dt.date.fromisoformat(value[:10])
        return value
    if isinstance(db_type, (Text, String)):
        return _stringify(value)
    if key in JSON_COLUMNS:
        return _json(value)
    if isinstance(value, (dict, list, tuple, set)):
        return _json(value)
    return value


def _coerce_value(key: str, value: Any, db_types: Dict[str, Any]) -> Any:
    if key in db_types:
        return _coerce_by_db_type(key, value, db_types[key])
    if key == "snapshot_date" and isinstance(value, str):
        return dt.date.fromisoformat(value[:10])
    if key in INTEGER_COLUMNS:
        return _safe_int(value)
    if key in FLOAT_COLUMNS:
        return _safe_float(value)
    if key in JSON_COLUMNS:
        return _json(value)
    if isinstance(value, (dict, list, tuple, set)):
        return _json(value)
    return value


def _schema_mismatches(db_types: Dict[str, Any]) -> List[Dict[str, str]]:
    expected_text = {"player_name", "team_name", "opponent_name", "away_team", "home_team", "pick_label", "primary_reason", "grade_reason"}
    mismatches: List[Dict[str, str]] = []
    for column in expected_text:
        db_type = db_types.get(column)
        if db_type is not None and not isinstance(db_type, (Text, String)):
            mismatches.append({"column": column, "expected": "text/string", "actual": str(db_type)})
    return mismatches


def safe_upsert_tracker_rows(session: Session, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    inserted = 0
    updated = 0
    now = dt.datetime.utcnow()
    db_types = _db_column_types(session)
    mismatches = _schema_mismatches(db_types)
    for row in rows:
        tracker_key = row.get("tracker_key") or _tracker_key(row)
        existing = session.query(ModelTrackerSnapshot).filter(ModelTrackerSnapshot.tracker_key == tracker_key).first()
        if existing:
            target = existing
            updated += 1
        else:
            target = ModelTrackerSnapshot(tracker_key=tracker_key)
            session.add(target)
            inserted += 1
        for key, value in row.items():
            if not hasattr(target, key):
                continue
            setattr(target, key, _coerce_value(key, value, db_types))
        target.updated_at = now
    session.commit()
    return {
        "inserted": inserted,
        "updated": updated,
        "total_rows_seen": len(rows),
        "schema_mismatches": mismatches,
    }


def build_tracker_snapshot_safe(session: Session, target_date: str) -> Dict[str, Any]:
    """Store only Bet105 decisions and explicit model-only MyDashboard signals."""
    target_date = target_date[:10]
    dt.date.fromisoformat(target_date)
    errors: List[Dict[str, Any]] = []
    model_rows: List[Dict[str, Any]] = []

    try:
        payload = build_model_projection_payload(session, target_date)
        model_rows.extend(normalize_model_projection_rows(payload, target_date))
    except Exception as exc:
        errors.append({"source": "model_projections", "error": str(exc), "type": exc.__class__.__name__})

    for component in DASHBOARD_COMPONENTS:
        try:
            payload = build_dashboard_solver_payload(session=session, date=target_date, component=component)
            model_rows.extend(normalize_dashboard_rows(component, payload, target_date))
        except Exception as exc:
            errors.append({"source": "my_dashboard", "component": component, "error": str(exc), "type": exc.__class__.__name__})

    bet105_rows: List[Dict[str, Any]] = []
    try:
        from .sportsbook_bet105_service import fetch_board
        board = fetch_board(date=target_date, raw=True)
        bet105_rows = build_bet105_decisions(board, model_rows, target_date)
        if board.get("status") not in {"ok", "incomplete_normalization"}:
            errors.append({"source": "bet105", "error": f"Bet105 board status: {board.get('status')}"})
    except Exception as exc:
        errors.append({"source": "bet105", "error": str(exc), "type": exc.__class__.__name__})

    rows = bet105_rows + as_model_signals(model_rows, target_date)
    upsert_result = safe_upsert_tracker_rows(session, rows)
    if upsert_result.get("schema_mismatches"):
        errors.append({"source": "model_tracker_schema", "error": "Production schema values were coerced.", "mismatches": upsert_result["schema_mismatches"]})
    return {
        "date": target_date,
        "rows_collected": len(rows),
        "reportable_decision_count": len(bet105_rows),
        "model_signal_count": len(rows) - len(bet105_rows),
        "upsert": upsert_result,
        "errors": errors,
    }

