from __future__ import annotations

import urllib.parse
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

TEAM_SLUGS = {
    "ARI": "dbacks",
    "ATL": "braves",
    "BAL": "orioles",
    "BOS": "redsox",
    "CHC": "cubs",
    "CWS": "whitesox",
    "CIN": "reds",
    "CLE": "guardians",
    "COL": "rockies",
    "DET": "tigers",
    "HOU": "astros",
    "KC": "royals",
    "LAA": "angels",
    "LAD": "dodgers",
    "MIA": "marlins",
    "MIL": "brewers",
    "MIN": "twins",
    "NYM": "mets",
    "NYY": "yankees",
    "ATH": "athletics",
    "PHI": "phillies",
    "PIT": "pirates",
    "SD": "padres",
    "SEA": "mariners",
    "SF": "giants",
    "STL": "cardinals",
    "TB": "rays",
    "TEX": "rangers",
    "TOR": "bluejays",
    "WSH": "nationals",
}

GLOBAL_RSS_SOURCES = [
    {
        "name": "MLB.com News",
        "url": "https://www.mlb.com/feeds/news/rss.xml",
        "source_type": "official",
        "team": None,
        "reliability": "high",
        "requires_api_key": False,
    },
    {
        "name": "MLB Trade Rumors",
        "url": "https://www.mlbtraderumors.com/feed",
        "source_type": "transactions",
        "team": None,
        "reliability": "medium",
        "requires_api_key": False,
    },
    {
        "name": "Google News MLB",
        "url": "https://news.google.com/rss/search?q=MLB+injury+lineup+probable+pitcher+odds+roster+weather+when:1d&hl=en-US&gl=US&ceid=US:en",
        "source_type": "google_news",
        "team": None,
        "reliability": "medium",
        "requires_api_key": False,
    },
]

OPTIONAL_RSS_CANDIDATES = [
    {
        "name": "FOX Sports MLB",
        "url": None,
        "source_type": "national",
        "team": None,
        "reliability": "medium",
        "requires_api_key": False,
        "source_registry_status": "needs_verification",
        "source_registry_note": "FOX Sports MLB is intentionally disabled until a stable RSS URL is confirmed.",
    },
]

_SITUATION_TERMS = ["lineup", "injury", "probable pitcher", "bullpen", "weather", "roster move"]


def _queries(team_name: str, aliases: List[str], venues: List[str]) -> List[str]:
    base = list(dict.fromkeys([team_name, *aliases, *venues]))
    return base + [f"{team_name} {term}" for term in _SITUATION_TERMS]


def _google_news_url(query: str) -> str:
    return (
        "https://news.google.com/rss/search?q="
        f"{urllib.parse.quote_plus(query)}"
        "&hl=en-US&gl=US&ceid=US:en"
    )


def _team_rss_sources(abbr: str, team_name: str) -> List[Dict[str, Any]]:
    slug = TEAM_SLUGS.get(abbr)
    sources: List[Dict[str, Any]] = []
    if slug:
        sources.append(
            {
                "name": f"MLB.com {team_name.split()[-1]} News",
                "url": f"https://www.mlb.com/{slug}/feeds/news/rss.xml",
                "source_type": "official",
                "team": abbr,
                "reliability": "high",
                "requires_api_key": False,
            }
        )
    sources.append(
        {
            "name": f"Google News {team_name}",
            "url": _google_news_url(f"{team_name} injury lineup starter probable pitcher roster when:1d"),
            "source_type": "google_news",
            "team": abbr,
            "reliability": "medium",
            "requires_api_key": False,
        }
    )
    return sources


def get_global_rss_sources() -> List[Dict[str, Any]]:
    return [dict(source) for source in GLOBAL_RSS_SOURCES]


def get_rss_sources(team: Optional[str] = None, include_team_google: bool = True) -> List[Dict[str, Any]]:
    wanted = str(team or "").upper()
    sources = get_global_rss_sources()

    for abbr, team_name, _aliases, _venues in _TEAM_ROWS:
        if wanted and wanted != abbr:
            continue
        team_sources = _team_rss_sources(abbr, team_name)
        if not include_team_google:
            team_sources = [source for source in team_sources if source.get("source_type") != "google_news"]
        sources.extend(team_sources)

    seen = set()
    unique: List[Dict[str, Any]] = []
    for source in sources:
        url = source.get("url")
        key = (source.get("name"), url, source.get("team"))
        if not url or key in seen:
            continue
        seen.add(key)
        unique.append(source)
    return unique


def get_news_sources(team: Optional[str] = None) -> List[Dict[str, Any]]:
    wanted = str(team or "").upper()
    rows: List[Dict[str, Any]] = []
    for abbr, team_name, aliases, venues in _TEAM_ROWS:
        if wanted and wanted != abbr:
            continue
        rows.append(
            {
                "team": abbr,
                "team_name": team_name,
                "aliases": aliases,
                "city_aliases": aliases,
                "venue_keywords": venues,
                "queries": _queries(team_name, aliases, venues),
                "rss_sources": _team_rss_sources(abbr, team_name),
                "beat_sources": [],
                "local_outlets": [],
                "source_registry_status": "rss_active_manual_sources_pending",
                "source_registry_note": (
                    "Official MLB.com team RSS and Google News RSS discovery are active. "
                    "Curated beat/local handles and outlet feeds remain intentionally empty until verified."
                ),
            }
        )
    return rows


def team_lookup() -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in get_news_sources():
        for key in [row["team"], row["team_name"], *row.get("aliases", []), *row.get("city_aliases", [])]:
            value = str(key).strip().lower()
            if value:
                lookup[value] = row
    return lookup
