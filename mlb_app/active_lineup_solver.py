from __future__ import annotations

import copy
import datetime as dt
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from .lineup_profile import fetch_boxscore_lineup
from .matchup_generator import generate_matchups_for_date
from .my_dashboard_solver import build_dashboard_solver_payload
from .shared_payload_cache import get_cache, make_cache_key, set_cache, stable_hash

HITTER_COMPONENTS = {"hitters", "overall_players"}
LIVE_LINEUP_POLL_SECONDS = 30
HISTORICAL_LINEUP_TTL_SECONDS = 300


def _normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _string_id(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return str(int(value))
    except Exception:
        return str(value).strip()


def _team_value(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value).strip().lower()


def _is_hitter_item(item: Dict[str, Any]) -> bool:
    return str(item.get("entity_type") or "").lower() == "hitter" or str(item.get("player_type") or "").lower() == "hitter"


def _lineup_match_keys(player: Dict[str, Any], team_id: Any, team_name: Any) -> Tuple[Set[str], Set[Tuple[str, str]]]:
    ids: Set[str] = set()
    names: Set[Tuple[str, str]] = set()
    player_id = _string_id(player.get("batter_id") or player.get("id") or player.get("person_id"))
    if player_id:
        ids.add(player_id)
    name = _normalize_name(player.get("name") or player.get("fullName") or player.get("player_name"))
    if name:
        for team_value in (_team_value(team_id), _team_value(team_name)):
            if team_value:
                names.add((name, team_value))
    return ids, names


def _serialize_lineup_index(lineup_index: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "confirmed_ids": sorted(lineup_index.get("confirmed_ids") or []),
        "confirmed_names": [list(item) for item in sorted(lineup_index.get("confirmed_names") or [])],
        "metadata": lineup_index.get("metadata") or {},
    }


def _deserialize_lineup_index(lineup_index: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "confirmed_ids": set(lineup_index.get("confirmed_ids") or []),
        "confirmed_names": set(tuple(item) for item in (lineup_index.get("confirmed_names") or [])),
        "metadata": lineup_index.get("metadata") or {},
    }


def _build_confirmed_lineup_index_uncached(session: Session, target_date: str) -> Dict[str, Any]:
    warnings: List[str] = []
    confirmed_ids: Set[str] = set()
    confirmed_names: Set[Tuple[str, str]] = set()
    games_checked = games_with_lineups = teams_with_lineups = 0
    try:
        matchups = generate_matchups_for_date(session, target_date)
    except Exception as exc:
        return {"confirmed_ids": confirmed_ids, "confirmed_names": confirmed_names, "metadata": {"enabled": True, "source": "matchups_boxscore_lineups", "lineup_status": "unavailable", "confirmed_lineup_date": target_date, "confirmed_batter_count": 0, "games_checked": 0, "games_with_lineups": 0, "teams_with_lineups": 0, "removed_unconfirmed_count": 0, "warnings": [f"Could not load matchup payload: {exc}"]}}

    for matchup in matchups or []:
        game_pk = matchup.get("game_pk")
        if not game_pk:
            warnings.append("Skipped matchup without game_pk while building active-lineup filter.")
            continue
        games_checked += 1
        try:
            lineups = fetch_boxscore_lineup(int(game_pk))
        except Exception as exc:
            warnings.append(f"Lineup fetch failed for game {game_pk}: {exc}")
            continue
        game_has_lineup = False
        for side in ("away", "home"):
            lineup = lineups.get(side) or []
            if not lineup:
                continue
            game_has_lineup = True
            teams_with_lineups += 1
            for player in lineup:
                ids, names = _lineup_match_keys(player, matchup.get(f"{side}_team_id"), matchup.get(f"{side}_team_name"))
                confirmed_ids.update(ids)
                confirmed_names.update(names)
        if game_has_lineup:
            games_with_lineups += 1

    if confirmed_ids or confirmed_names:
        lineup_status = "confirmed" if games_checked and games_with_lineups == games_checked else "partial"
    else:
        lineup_status = "unavailable"
        warnings.append("No confirmed starting lineups were found in the matchup boxscore payload for this date.")

    metadata = {
        "enabled": True,
        "source": "matchups_boxscore_lineups",
        "lineup_status": lineup_status,
        "confirmed_lineup_date": target_date,
        "confirmed_batter_count": len(confirmed_ids) or len(confirmed_names),
        "games_checked": games_checked,
        "games_with_lineups": games_with_lineups,
        "teams_with_lineups": teams_with_lineups,
        "removed_unconfirmed_count": 0,
        "warnings": warnings,
    }
    metadata["lineup_revision"] = stable_hash({"status": lineup_status, "ids": sorted(confirmed_ids), "names": sorted(confirmed_names)})
    metadata["model_state"] = "confirmed" if lineup_status == "confirmed" else "lineup_building" if lineup_status == "partial" else "projected"
    return {"confirmed_ids": confirmed_ids, "confirmed_names": confirmed_names, "metadata": metadata}


def _lineup_cache_ttl(target_date: str) -> int:
    return LIVE_LINEUP_POLL_SECONDS if target_date == dt.date.today().isoformat() else HISTORICAL_LINEUP_TTL_SECONDS


def build_confirmed_lineup_index(session: Session, target_date: str) -> Dict[str, Any]:
    cache_key = make_cache_key("dashboard_context", "active_lineup_index", target_date)
    ttl_seconds = _lineup_cache_ttl(target_date)
    cached = get_cache(cache_key, ttl_seconds)
    if cached is not None:
        return _deserialize_lineup_index(cached)
    built = _serialize_lineup_index(_build_confirmed_lineup_index_uncached(session, target_date))
    return _deserialize_lineup_index(set_cache(cache_key, built))


def lineup_revision(lineup_index: Dict[str, Any]) -> str:
    metadata = lineup_index.get("metadata") or {}
    return str(metadata.get("lineup_revision") or stable_hash(_serialize_lineup_index(lineup_index)))


def _item_confirmed_in_lineup(item: Dict[str, Any], confirmed_ids: Set[str], confirmed_names: Set[Tuple[str, str]]) -> bool:
    entity_id = _string_id(item.get("entity_id") or item.get("batter_id") or item.get("player_id"))
    if entity_id and entity_id in confirmed_ids:
        return True
    name = _normalize_name(item.get("entity_name") or item.get("name") or item.get("player_name"))
    team_values = [_team_value(item.get("team")), _team_value(item.get("team_id")), _team_value(item.get("batter_team_id"))]
    return bool(name) and any((name, team) in confirmed_names for team in team_values if team)


def _rerank_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{**item, "rank": idx} for idx, item in enumerate(items, start=1)]


def _apply_active_lineup_filter(response: Dict[str, Any], lineup_index: Dict[str, Any], component: str) -> Dict[str, Any]:
    updated = copy.deepcopy(response or {})
    metadata = copy.deepcopy(lineup_index.get("metadata") or {})
    confirmed_ids = lineup_index.get("confirmed_ids") or set()
    confirmed_names = lineup_index.get("confirmed_names") or set()
    if component not in HITTER_COMPONENTS:
        metadata.update({"enabled": False, "lineup_status": "not_applicable", "model_state": "not_applicable", "warnings": list(metadata.get("warnings") or []) + ["Active-lineup filtering only applies to hitter/player components."]})
        updated["lineup_filter"] = metadata
        return updated
    kept, removed = [], 0
    for item in updated.get("items") or []:
        if not _is_hitter_item(item):
            kept.append(item)
        elif metadata.get("lineup_status") == "unavailable":
            removed += 1
        elif _item_confirmed_in_lineup(item, confirmed_ids, confirmed_names):
            kept.append({**item, "lineup_verified": True, "lineup_source": metadata.get("source"), "confirmed_lineup_date": metadata.get("confirmed_lineup_date"), "lineup_revision": metadata.get("lineup_revision")})
        else:
            removed += 1
    metadata["removed_unconfirmed_count"] = removed
    if removed:
        metadata.setdefault("warnings", []).append(f"Removed {removed} hitter result(s) that were not confirmed in the starting lineup.")
    updated["items"] = _rerank_items(kept)
    updated["result_count_after_lineup_filter"] = len(updated["items"])
    updated["lineup_filter"] = metadata
    updated["model_state"] = metadata.get("model_state")
    updated["lineup_revision"] = metadata.get("lineup_revision")
    return updated


def build_active_lineup_solver_payload(session: Session, date: Optional[str], component: str, filters: Optional[Dict[str, Any]] = None, lineup_index: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    target_date = str(date or "")[:10]
    normalized_component = (component or "").strip().lower()
    base_payload = build_dashboard_solver_payload(session=session, date=target_date, component=normalized_component, filters=filters)
    active_index = lineup_index or build_confirmed_lineup_index(session, target_date)
    return _apply_active_lineup_filter(base_payload, active_index, normalized_component)
