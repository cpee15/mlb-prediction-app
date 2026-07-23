from __future__ import annotations

import copy
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import inspect, text

from . import model_projections as raw_model_projections
from .performance import record_span, timing_span
from .shared_artifacts import payload_input_hash, simulation_key
from .shared_payload_cache import env_ttl, get_cache, set_cache

_SCHEMA_CACHE: Dict[str, Dict[str, Any]] = {}
_RAW_BULLPEN_INPUTS = raw_model_projections._bullpen_inputs
_RAW_BUILD_PROJECTION_SIMULATION_CARDS = raw_model_projections._build_projection_simulation_cards
_PATCHED = False


def _engine_cache_key(session: Any) -> str:
    bind = getattr(session, "bind", None)
    url = getattr(bind, "url", None)
    return str(url or id(bind))


def clear_projection_performance_caches() -> None:
    _SCHEMA_CACHE.clear()


def _discover_bullpen_schema(session: Any) -> Dict[str, Any]:
    """Discover bullpen table/column mapping once per engine.

    The original `_bullpen_inputs` reflected table names and columns for every
    team/game. This cache preserves the same lookup semantics while avoiding
    repeated SQLAlchemy inspection work on hot projection builds.
    """
    cache_key = _engine_cache_key(session)
    if cache_key in _SCHEMA_CACHE:
        record_span("model_projection.bullpen_schema_cache", category="db", cache_status="HIT")
        return _SCHEMA_CACHE[cache_key]

    with timing_span("model_projection.bullpen_schema_discovery", category="db", cache_status="MISS"):
        try:
            inspector = inspect(session.bind)
            table_names = set(inspector.get_table_names())
        except Exception:
            schema = {"source_table": None, "error": "inspection_failed"}
            _SCHEMA_CACHE[cache_key] = schema
            return schema

        table = next(
            (
                name
                for name in [
                    "bullpen_stats",
                    "team_bullpen_stats",
                    "table_layerseven",
                    "layerseven",
                    "team_pitching_bullpen",
                    "team_pitching_stats",
                ]
                if name in table_names
            ),
            None,
        )
        if not table:
            schema = {"source_table": None}
            _SCHEMA_CACHE[cache_key] = schema
            return schema

        try:
            columns = [col["name"] for col in inspector.get_columns(table)]
        except Exception as exc:
            schema = {"source_table": table, "error": str(exc)}
            _SCHEMA_CACHE[cache_key] = schema
            return schema

        schema = {
            "source_table": table,
            "team_id_col": raw_model_projections._find_column(columns, ["team_id", "teamid", "mlb_team_id"]),
            "team_name_col": raw_model_projections._find_column(columns, ["team_name", "team", "name"]),
            "era_col": raw_model_projections._find_column(columns, ["era", "bullpen_era"]),
            "bb9_col": raw_model_projections._find_column(columns, ["bb_per_9", "bb9", "bb_9", "bb_per_nine", "walks_per_9"]),
            "whip_col": raw_model_projections._find_column(columns, ["whip", "bullpen_whip"]),
        }
        _SCHEMA_CACHE[cache_key] = schema
        record_span("model_projection.bullpen_schema_cache", category="db", cache_status="STORE")
        return schema


def cached_bullpen_inputs(session: Any, team_id: Optional[int], team_name: Optional[str]) -> Dict[str, Any]:
    schema = _discover_bullpen_schema(session)
    table = schema.get("source_table")
    if not table:
        return {"source_table": None}
    if schema.get("error"):
        return {"source_table": table, "error": schema.get("error")}

    era_col = schema.get("era_col")
    bb9_col = schema.get("bb9_col")
    whip_col = schema.get("whip_col")
    if not all([era_col, bb9_col, whip_col]):
        return {"source_table": table}

    where = None
    params: Dict[str, Any] = {}
    if schema.get("team_id_col") and team_id is not None:
        where = f"{schema['team_id_col']} = :team_id"
        params["team_id"] = int(team_id)
    elif schema.get("team_name_col") and team_name:
        where = f"lower({schema['team_name_col']}) = lower(:team_name)"
        params["team_name"] = team_name
    if not where:
        return {"source_table": table}

    try:
        with timing_span("model_projection.bullpen_inputs_query", category="db"):
            row = session.execute(
                text(f"SELECT {era_col} AS era, {bb9_col} AS bb_per_9, {whip_col} AS whip FROM {table} WHERE {where} LIMIT 1"),
                params,
            ).mappings().first()
        return {"era": row.get("era"), "bb_per_9": row.get("bb_per_9"), "whip": row.get("whip"), "source_table": table} if row else {"source_table": table}
    except Exception as exc:
        return {"source_table": table, "error": str(exc)}


def _simulation_cards_cache_key(matchup: Dict[str, Any], away: Dict[str, Any], home: Dict[str, Any]) -> str:
    date = matchup.get("game_date") or "unknown_date"
    game_pk = matchup.get("game_pk") or matchup.get("gamePk") or "unknown_game"
    input_hash = payload_input_hash({
        "matchup": {
            "game_pk": game_pk,
            "game_date": date,
            "venue": matchup.get("venue"),
            "weather": matchup.get("weather"),
            "park_factor": matchup.get("park_factor"),
        },
        "away": {
            "team_id": away.get("team_id"),
            "team_name": away.get("team_name"),
            "offense_inputs": away.get("offense_inputs"),
            "bullpen_inputs": away.get("bullpen_inputs"),
        },
        "home": {
            "team_id": home.get("team_id"),
            "team_name": home.get("team_name"),
            "offense_inputs": home.get("offense_inputs"),
            "bullpen_inputs": home.get("bullpen_inputs"),
        },
        "simulation_count": 3000,
        "seed": 42,
    })
    return simulation_key(date=date, game_pk=game_pk, simulation_count=3000, input_hash=input_hash)


def cached_projection_simulation_cards(matchup: Dict[str, Any], away: Dict[str, Any], home: Dict[str, Any]) -> Dict[str, Any]:
    cache_key = _simulation_cards_cache_key(matchup, away, home)
    ttl = env_ttl("MODEL_PROJECTION_SIMULATION_CACHE_TTL_SECONDS")
    cached = get_cache(cache_key, ttl)
    if isinstance(cached, dict):
        cached["cache_hit"] = True
        cached.setdefault("cache_key", cache_key)
        return cached

    with timing_span(
        "model_projection.build_projection_simulation_cards_uncached",
        category="simulation",
        game_pk=matchup.get("game_pk") or matchup.get("gamePk"),
        date=matchup.get("game_date"),
        cache_status="MISS",
    ):
        built = _RAW_BUILD_PROJECTION_SIMULATION_CARDS(matchup, away, home)
    if isinstance(built, dict):
        built.setdefault("cache_hit", False)
        built.setdefault("cache_key", cache_key)
    return set_cache(cache_key, built)


def install_model_projection_performance_cache() -> bool:
    """Patch targeted hot functions used by build_model_projection_payload.

    This is intentionally narrow: no formulas or model semantics change. It only
    caches bullpen schema discovery and repeats of deterministic diagnostic
    simulation-card builds.
    """
    global _PATCHED
    if _PATCHED:
        return True
    raw_model_projections._bullpen_inputs = cached_bullpen_inputs
    raw_model_projections._build_projection_simulation_cards = cached_projection_simulation_cards
    _PATCHED = True
    return True


def build_model_projection_payload(session: Any, target_date: str) -> Dict[str, Any]:
    install_model_projection_performance_cache()
    return raw_model_projections.build_model_projection_payload(session, target_date)


__all__ = [
    "build_model_projection_payload",
    "cached_bullpen_inputs",
    "cached_projection_simulation_cards",
    "clear_projection_performance_caches",
    "install_model_projection_performance_cache",
]
