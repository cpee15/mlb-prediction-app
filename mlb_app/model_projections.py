from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from .db_utils import get_pitch_arsenal_with_fallback, get_team_split
from .matchup_generator import generate_matchups_for_date
from .model_projection_formulas import bullpen_collapse_index, offensive_firepower_score, pitch_identity_disruption_score, pitching_volatility_score, safe_float


def _obj_to_dict(obj: Any, fields: List[str]) -> Dict[str, Any]:
    return {field: getattr(obj, field, None) for field in fields} if obj is not None else {}


def _arsenal_records_to_dict(records: List[Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for record in records or []:
        pitch_type = getattr(record, "pitch_type", None) or getattr(record, "pitch_name", None) or "unknown"
        out[pitch_type] = {
            "pitch_type": getattr(record, "pitch_type", None),
            "pitch_name": getattr(record, "pitch_name", None),
            "pitch_count": getattr(record, "pitch_count", None),
            "usage_pct": getattr(record, "usage_pct", None),
            "whiff_pct": getattr(record, "whiff_pct", None),
            "strikeout_pct": getattr(record, "strikeout_pct", None),
            "rv_per_100": getattr(record, "rv_per_100", None),
            "xwoba": getattr(record, "xwoba", None),
            "hard_hit_pct": getattr(record, "hard_hit_pct", None),
        }
    return out


def _team_split_inputs(session: Session, team_id: Optional[int], season: int) -> Dict[str, Any]:
    if not team_id:
        return {"source": "missing_team_id"}
    row = get_team_split(session, int(team_id), season, "vsR") or get_team_split(session, int(team_id), season, "vsL")
    data = _obj_to_dict(row, ["pa", "hits", "doubles", "triples", "home_runs", "walks", "strikeouts", "batting_avg", "on_base_pct", "slugging_pct", "iso", "k_pct", "bb_pct"])
    data.update({
        "team_id": team_id,
        "split": getattr(row, "split", None) if row else None,
        "lineup_source": "team_splits_fallback_not_confirmed_lineup" if row else None,
        "player_count_used": None,
        "sample_blend": {"type": "team_split", "season": season, "split": getattr(row, "split", None)} if row else None,
        "source": "team_splits" if row else "missing_team_splits",
    })
    return data


def _find_column(columns: List[str], aliases: List[str]) -> Optional[str]:
    normalized = {col.lower().replace("_", "").replace("/", ""): col for col in columns}
    for alias in aliases:
        key = alias.lower().replace("_", "").replace("/", "")
        if key in normalized:
            return normalized[key]
    return None


def _bullpen_inputs(session: Session, team_id: Optional[int], team_name: Optional[str]) -> Dict[str, Any]:
    try:
        inspector = inspect(session.bind)
        table_names = set(inspector.get_table_names())
    except Exception:
        return {"source_table": None}
    table = next((name for name in ["bullpen_stats", "team_bullpen_stats", "table_layerseven", "layerseven", "team_pitching_bullpen", "team_pitching_stats"] if name in table_names), None)
    if not table:
        return {"source_table": None}
    try:
        columns = [col["name"] for col in inspector.get_columns(table)]
        team_id_col = _find_column(columns, ["team_id", "teamid", "mlb_team_id"])
        team_name_col = _find_column(columns, ["team_name", "team", "name"])
        era_col = _find_column(columns, ["era", "bullpen_era"])
        bb9_col = _find_column(columns, ["bb_per_9", "bb9", "bb_9", "bb_per_nine", "walks_per_9"])
        whip_col = _find_column(columns, ["whip", "bullpen_whip"])
        if not all([era_col, bb9_col, whip_col]):
            return {"source_table": table}
        where = None
        params: Dict[str, Any] = {}
        if team_id_col and team_id is not None:
            where = f"{team_id_col} = :team_id"
            params["team_id"] = int(team_id)
        elif team_name_col and team_name:
            where = f"lower({team_name_col}) = lower(:team_name)"
            params["team_name"] = team_name
        if not where:
            return {"source_table": table}
        row = session.execute(text(f"SELECT {era_col} AS era, {bb9_col} AS bb_per_9, {whip_col} AS whip FROM {table} WHERE {where} LIMIT 1"), params).mappings().first()
        return {"era": row.get("era"), "bb_per_9": row.get("bb_per_9"), "whip": row.get("whip"), "source_table": table} if row else {"source_table": table}
    except Exception as exc:
        return {"source_table": table, "error": str(exc)}


def _side_context(matchup: Dict[str, Any], side: str, session: Session, season: int) -> Dict[str, Any]:
    pitcher_id = matchup.get(f"{side}_pitcher_id")
    arsenal = matchup.get(f"{side}_pitch_arsenal") or {}
    arsenal_source = "matchup_generator" if arsenal else "missing_pitch_arsenal"
    if not arsenal and pitcher_id:
        records, arsenal_season = get_pitch_arsenal_with_fallback(session, int(pitcher_id), season)
        arsenal = _arsenal_records_to_dict(records)
        arsenal_source = f"pitch_arsenal_fallback_{arsenal_season}" if arsenal else "missing_pitch_arsenal"
    team_id = matchup.get(f"{side}_team_id")
    team_name = matchup.get(f"{side}_team_name")
    ctx = {
        "side": side,
        "team_id": team_id,
        "team_name": team_name,
        "pitcher_id": pitcher_id,
        "pitcher_name": matchup.get(f"{side}_pitcher_name"),
        "pitcher_features": matchup.get(f"{side}_pitcher_features") or {},
        "pitch_arsenal": arsenal,
        "pitch_arsenal_source": arsenal_source,
        "offense_inputs": _team_split_inputs(session, team_id, season),
        "bullpen_inputs": _bullpen_inputs(session, team_id, team_name),
    }
    ctx["models"] = [
        pitching_volatility_score(ctx["pitcher_features"], ctx["pitch_arsenal"]),
        offensive_firepower_score(ctx["offense_inputs"]),
        bullpen_collapse_index(ctx["bullpen_inputs"]),
        pitch_identity_disruption_score(ctx["pitch_arsenal"], hitter_pitch_rows=[]),
    ]
    return ctx


def build_model_projection_payload(session: Session, target_date: str) -> Dict[str, Any]:
    try:
        date_obj = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("date must be YYYY-MM-DD") from exc
    matchups = generate_matchups_for_date(session, target_date)
    games: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for matchup in matchups:
        try:
            away = _side_context(matchup, "away", session, date_obj.year)
            home = _side_context(matchup, "home", session, date_obj.year)
            games.append({
                "game_pk": matchup.get("game_pk"),
                "game_date": matchup.get("game_date") or target_date,
                "game_time": matchup.get("game_time"),
                "status": matchup.get("status"),
                "venue": matchup.get("venue"),
                "weather": matchup.get("weather"),
                "away_team": {"id": away.get("team_id"), "name": away.get("team_name")},
                "home_team": {"id": home.get("team_id"), "name": home.get("team_name")},
                "away_pitcher": {"id": away.get("pitcher_id"), "name": away.get("pitcher_name")},
                "home_pitcher": {"id": home.get("pitcher_id"), "name": home.get("pitcher_name")},
                "main_matchup_probabilities": {"away_win_prob": safe_float(matchup.get("away_win_prob")), "home_win_prob": safe_float(matchup.get("home_win_prob"))},
                "teams": {"away": away, "home": home},
            })
        except Exception as exc:
            errors.append({"game_pk": matchup.get("game_pk"), "error": str(exc)})
    return {"date": target_date, "count": len(games), "games": games, "errors": errors, "source_notes": ["Daily games are loaded through main generate_matchups_for_date.", "Scores use available real production inputs only.", "Missing inputs are returned explicitly and are not fabricated."]}
