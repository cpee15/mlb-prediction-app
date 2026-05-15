from __future__ import annotations

from typing import Any, Dict, List, Optional

_TEAM_ROWS = [
    ("ARI", "Arizona Diamondbacks", ["Diamondbacks", "D-backs", "Arizona"], ["Chase Field"]),
    ("ATL", "Atlanta Braves", ["Braves", "Atlanta"], ["Truist Park"]),
    ("BAL", "Baltimore Orioles", ["Orioles", "O's", "Baltimore"], ["Camden Yards", "Oriole Park"]),
    ("BOS", "Boston Red Sox", ["Red Sox", "Boston"], ["Fenway Park"]),
    ("CHC", "Chicago Cubs", ["Cubs", "Chicago Cubs"], ["Wrigley Field", "Wrigley weather"]),
    ("CWS", "Chicago White Sox", ["White Sox", "Sox", "Chicago White Sox"], ["Rate Field", "Guaranteed Rate Field"]),
    ("CIN", "Cincinnati Reds", ["Reds", "Cincinnati"], ["Great American Ball Park"]),
    ("CLE", "Cleveland Guardians", ["Guardians", "Cleveland"], ["Progressive Field"]),
    ("COL", "Colorado Rockies", ["Rockies", "Colorado", "Denver"], ["Coors Field"]),
    ("DET", "Detroit Tigers", ["Tigers", "Detroit"], ["Comerica Park"]),
    ("HOU", "Houston Astros", ["Astros", "Houston"], ["Daikin Park", "Minute Maid Park"]),
    ("KC", "Kansas City Royals", ["Royals", "Kansas City"], ["Kauffman Stadium"]),
    ("LAA", "Los Angeles Angels", ["Angels", "LA Angels", "Anaheim"], ["Angel Stadium"]),
    ("LAD", "Los Angeles Dodgers", ["Dodgers", "LA Dodgers", "Los Angeles Dodgers"], ["Dodger Stadium"]),
    ("MIA", "Miami Marlins", ["Marlins", "Miami"], ["loanDepot park"]),
    ("MIL", "Milwaukee Brewers", ["Brewers", "Milwaukee"], ["American Family Field"]),
    ("MIN", "Minnesota Twins", ["Twins", "Minnesota", "Minneapolis"], ["Target Field"]),
    ("NYM", "New York Mets", ["Mets", "Queens", "New York Mets"], ["Citi Field"]),
    ("NYY", "New York Yankees", ["Yankees", "Bronx", "New York Yankees"], ["Yankee Stadium"]),
    ("ATH", "Athletics", ["A's", "Athletics", "Oakland Athletics", "Sacramento Athletics"], ["Sutter Health Park"]),
    ("PHI", "Philadelphia Phillies", ["Phillies", "Philadelphia"], ["Citizens Bank Park"]),
    ("PIT", "Pittsburgh Pirates", ["Pirates", "Pittsburgh"], ["PNC Park"]),
    ("SD", "San Diego Padres", ["Padres", "San Diego"], ["Petco Park"]),
    ("SEA", "Seattle Mariners", ["Mariners", "Seattle"], ["T-Mobile Park"]),
    ("SF", "San Francisco Giants", ["Giants", "San Francisco"], ["Oracle Park"]),
    ("STL", "St. Louis Cardinals", ["Cardinals", "Cards", "St. Louis", "Saint Louis"], ["Busch Stadium"]),
    ("TB", "Tampa Bay Rays", ["Rays", "Tampa Bay", "St. Petersburg"], ["Steinbrenner Field", "Tropicana Field"]),
    ("TEX", "Texas Rangers", ["Rangers", "Texas", "Arlington"], ["Globe Life Field"]),
    ("TOR", "Toronto Blue Jays", ["Blue Jays", "Jays", "Toronto"], ["Rogers Centre"]),
    ("WSH", "Washington Nationals", ["Nationals", "Nats", "Washington"], ["Nationals Park"]),
]

_SITUATION_TERMS = ["lineup", "injury", "probable pitcher", "bullpen", "weather", "roster move"]


def _queries(team_name: str, aliases: List[str], venues: List[str]) -> List[str]:
    base = list(dict.fromkeys([team_name, *aliases, *venues]))
    return base + [f"{team_name} {term}" for term in _SITUATION_TERMS]


def get_news_sources(team: Optional[str] = None) -> List[Dict[str, Any]]:
    wanted = str(team or "").upper()
    rows: List[Dict[str, Any]] = []
    for abbr, team_name, aliases, venues in _TEAM_ROWS:
        if wanted and wanted != abbr:
            continue
        rows.append({
            "team": abbr,
            "team_name": team_name,
            "aliases": aliases,
            "city_aliases": aliases,
            "venue_keywords": venues,
            "queries": _queries(team_name, aliases, venues),
            "beat_sources": [],
            "local_outlets": [],
            "source_registry_status": "needs_verification",
            "source_registry_note": "Curated beat/local handles and RSS URLs are intentionally empty until verified. Broad provider queries remain available.",
        })
    return rows


def team_lookup() -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in get_news_sources():
        for key in [row["team"], row["team_name"], *row.get("aliases", []), *row.get("city_aliases", [])]:
            lookup[str(key).strip().lower()] = row
    return lookup
