from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, List

SITUATION_TERMS = [
    "scratched", "lineup", "injury", "bullpen", "closer", "opener", "delay", "postponed", "weather",
    "pitch count", "call up", "optioned", "activated", "IL", "day to day", "not in lineup", "rest day",
]
BETTING_TERMS = ["steam", "line move", "moneyline", "total", "run line", "prop", "sharp", "public", "handle", "ticket", "odds"]


def _today() -> str:
    return dt.date.today().isoformat()


def _load_matchups(date: str) -> List[Dict[str, Any]]:
    try:
        from .app import _get_session
        from .matchup_generator import generate_matchups_for_date
        Session = _get_session()
        with Session() as session:
            data = generate_matchups_for_date(session, date)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def build_keyword_groups(date: str | None = None) -> Dict[str, List[str]]:
    target_date = (date or _today())[:10]
    matchups = _load_matchups(target_date)
    team_terms: List[str] = []
    player_terms: List[str] = []
    for matchup in matchups:
        for key in ["away_team_name", "away_team", "away_name", "home_team_name", "home_team", "home_name"]:
            value = matchup.get(key)
            if value and value not in team_terms:
                team_terms.append(str(value))
        for key in ["away_pitcher_name", "home_pitcher_name", "away_probable_pitcher", "home_probable_pitcher"]:
            value = matchup.get(key)
            if value and value not in player_terms:
                player_terms.append(str(value))
        for side in ["away", "home"]:
            lineup = matchup.get(f"{side}_lineup") or matchup.get(f"{side}_players") or []
            if isinstance(lineup, list):
                for player in lineup[:9]:
                    name = player.get("name") or player.get("fullName") if isinstance(player, dict) else None
                    if name and name not in player_terms:
                        player_terms.append(str(name))
    default_query = os.getenv("NEWS_DEFAULT_QUERY", "MLB baseball injuries lineups probable pitchers odds")
    return {
        "date": [target_date],
        "teams": team_terms,
        "players": player_terms,
        "situations": list(SITUATION_TERMS),
        "betting": list(BETTING_TERMS),
        "default": [default_query],
        "all": list(dict.fromkeys(team_terms + player_terms + SITUATION_TERMS + BETTING_TERMS + [default_query])),
    }
