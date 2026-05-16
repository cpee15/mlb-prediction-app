from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import re
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional

from .news_classifier import classify_text, score_importance
from .news_keywords import BETTING_TERMS
from .news_provider import stable_id, utc_now
from .news_sources import get_news_sources, team_lookup

DEFAULT_BASEBALL_FEED_QUERIES = [
    "from:PitchingNinja",
    "from:CodifyBaseball",
    "from:baseballsavant",
    "from:MLBPipeline",
    "from:fangraphs",
    "from:enosarris",
    "from:BenLindbergh",
    "from:TheAthleticMLB",
    "from:MLBNetwork",
    "baseball biomechanics pitching mechanics",
    "MLB Statcast barrel rate xwOBA",
]

CLICKBAIT_TERMS = {
    "you won't believe",
    "shocking",
    "destroyed",
    "cooked",
    "exposed",
    "fraud",
    "ratio",
    "clown",
    "hot take",
    "insane parlay",
    "lock of the day",
}

QUALITY_TERMS = {
    "statcast",
    "xwoba",
    "barrel",
    "hard-hit",
    "hard hit",
    "savant",
    "release point",
    "extension",
    "ivb",
    "sweeper",
    "arsenal",
    "pitch design",
    "biomechanics",
    "mechanics",
    "swing decision",
    "run value",
    "stuff+",
    "location+",
    "lineup",
    "injury",
    "scratch",
    "bullpen",
    "weather",
    "odds",
    "line movement",
}


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_env(name: str, default: int, minimum: int = 1, maximum: int = 100) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


def _safe_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return max(0, int(value))
    text = str(value).strip().replace(",", "")
    if not text:
        return 0
    multiplier = 1
    lowered = text.lower()
    if lowered.endswith("k"):
        multiplier = 1_000
        text = text[:-1]
    elif lowered.endswith("m"):
        multiplier = 1_000_000
        text = text[:-1]
    try:
        return max(0, int(float(text) * multiplier))
    except Exception:
        return 0


def _nested_get(row: Any, *keys: str) -> Any:
    current = row
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            current = getattr(current, key, None)
        if current is None:
            return None
    return current


def _first_from_paths(row: Dict[str, Any], paths: Iterable[tuple[str, ...]]) -> Any:
    for path in paths:
        value = _nested_get(row, *path)
        if value is not None and value != "":
            return value
    return None


def _first_present(row: Dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = row.get(key)
        if value:
            return str(value)
    return ""


def _post_datetime(value: Any) -> str:
    if value is None:
        return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")
    text = str(value)
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return text


def _load_matchups_from_generator(date: str) -> List[Dict[str, Any]]:
    try:
        from .app import _get_session
        from .matchup_generator import generate_matchups_for_date

        Session = _get_session()
        with Session() as session:
            data = generate_matchups_for_date(session, date)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _load_matchups_from_mlb_schedule(date: str) -> List[Dict[str, Any]]:
    target_date = (date or dt.date.today().isoformat())[:10]
    params = urllib.parse.urlencode({
        "sportId": 1,
        "date": target_date,
        "hydrate": "probablePitcher,team,venue",
    })
    url = f"https://statsapi.mlb.com/api/v1/schedule?{params}"
    try:
        with urllib.request.urlopen(url, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []

    games: List[Dict[str, Any]] = []
    for day in payload.get("dates") or []:
        for game in day.get("games") or []:
            teams = game.get("teams") or {}
            away = teams.get("away") or {}
            home = teams.get("home") or {}
            away_team = away.get("team") or {}
            home_team = home.get("team") or {}
            away_pitcher = away.get("probablePitcher") or {}
            home_pitcher = home.get("probablePitcher") or {}
            games.append({
                "game_id": game.get("gamePk"),
                "gamePk": game.get("gamePk"),
                "game_pk": game.get("gamePk"),
                "game_date": game.get("gameDate") or target_date,
                "away_team_name": away_team.get("name"),
                "away_team": away_team.get("abbreviation") or away_team.get("name"),
                "away_name": away_team.get("name"),
                "home_team_name": home_team.get("name"),
                "home_team": home_team.get("abbreviation") or home_team.get("name"),
                "home_name": home_team.get("name"),
                "away_pitcher_name": away_pitcher.get("fullName"),
                "away_probable_pitcher": away_pitcher.get("fullName"),
                "home_pitcher_name": home_pitcher.get("fullName"),
                "home_probable_pitcher": home_pitcher.get("fullName"),
                "venue": (game.get("venue") or {}).get("name"),
                "source": "mlb_schedule_fallback",
            })
    return games


def _load_matchups(date: str) -> tuple[List[Dict[str, Any]], str]:
    generated = _load_matchups_from_generator(date)
    if generated:
        return generated, "matchup_generator"
    fallback = _load_matchups_from_mlb_schedule(date)
    if fallback:
        return fallback, "mlb_schedule_fallback"
    return [], "empty"


class ApifyXNewsProvider:
    name = "apify"
    requires_api_key = True

    def __init__(self) -> None:
        self.timeout_seconds = _int_env("TWITTER_X_TIMEOUT_SECONDS", _int_env("TWIKIT_TIMEOUT_SECONDS", 20, 3, 60), 3, 120)
        self.max_results = _int_env("TWITTER_X_MAX_RESULTS", _int_env("TWIKIT_MAX_RESULTS", 50, 1, 50), 1, 50)
        self.matchup_cap = _int_env("TWITTER_X_MATCHUP_TWEET_CAP", 20, 1, 50)
        self.general_cap = _int_env("TWITTER_X_GENERAL_TWEET_CAP", 50, 1, 100)
        self.max_queries_per_matchup = _int_env("TWITTER_X_MAX_QUERIES_PER_MATCHUP", _int_env("TWIKIT_MAX_QUERIES_PER_MATCHUP", 5, 1, 8), 1, 8)
        self.min_engagement = _int_env("TWITTER_X_MIN_ENGAGEMENT", 0, 0, 1000000)
        self.last_error: Optional[str] = None

    @staticmethod
    def supported() -> bool:
        return True

    @staticmethod
    def apify_installed() -> bool:
        try:
            import apify_client  # noqa: F401
            return True
        except Exception:
            return False

    def enabled(self) -> bool:
        return os.getenv("NEWS_X_PROVIDER", "").strip().lower() == "apify"

    def actor_configured(self) -> bool:
        return bool(os.getenv("TWITTER_X_ACTOR_ID", "").strip())

    def configured(self) -> bool:
        return self.enabled() and bool(os.getenv("APIFY_TOKEN", "").strip()) and self.actor_configured()

    def available(self) -> bool:
        return self.configured() and self.apify_installed()

    def health(self) -> Dict[str, Any]:
        return {
            "x_provider": "apify" if self.enabled() else os.getenv("NEWS_X_PROVIDER", "twikit").strip().lower() or "twikit",
            "apify_x_supported": True,
            "apify_x_installed": self.apify_installed(),
            "apify_x_enabled": self.enabled(),
            "apify_x_configured": self.configured(),
            "apify_x_actor_configured": self.actor_configured(),
            "apify_x_matchup_schedule_fallback": True,
            "apify_x_engagement_ranking": True,
            "apify_x_matchup_tweet_cap": self.matchup_cap,
            "apify_x_general_tweet_cap": self.general_cap,
            "apify_x_min_engagement": self.min_engagement,
        }

    def _not_configured(self, date: Optional[str] = None, items_key: str = "items") -> Dict[str, Any]:
        if self.enabled() and self.configured() and not self.apify_installed():
            status = "provider_missing_dependency"
            message = "Apify X provider is configured, but apify-client is not installed."
        else:
            status = "provider_not_configured"
            message = "Apify X provider is disabled or missing APIFY_TOKEN/TWITTER_X_ACTOR_ID. Configure NEWS_X_PROVIDER=apify, APIFY_TOKEN, and TWITTER_X_ACTOR_ID to enable matchup intel."
        payload = {
            "date": (date or dt.date.today().isoformat())[:10],
            "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "provider": self.name,
            "status": status,
            "message": message,
            "errors": [message] + ([self.last_error] if self.last_error else []),
        }
        payload[items_key] = []
        return payload

    def _actor_input(self, query: str, limit: int) -> Dict[str, Any]:
        mode = os.getenv("TWITTER_X_ACTOR_INPUT_MODE", "searchTerms").strip()
        max_key = os.getenv("TWITTER_X_ACTOR_MAX_ITEMS_KEY", "maxItems").strip() or "maxItems"
        if mode == "query":
            payload: Dict[str, Any] = {"query": query, max_key: limit}
        elif mode == "queries":
            payload = {"queries": [query], max_key: limit}
        else:
            payload = {"searchTerms": [query], max_key: limit}
        raw_sort = os.getenv("TWITTER_X_ACTOR_SORT", "Top").strip()
        if raw_sort:
            payload.setdefault("sort", raw_sort)
        return payload

    def _client(self) -> Any:
        try:
            from apify_client import ApifyClient
        except Exception as exc:
            raise RuntimeError("apify-client dependency is not installed") from exc
        return ApifyClient(os.getenv("APIFY_TOKEN", ""))

    def _raw_search(self, query: str, limit: int) -> List[Dict[str, Any]]:
        actor_id = os.getenv("TWITTER_X_ACTOR_ID", "").strip()
        if not actor_id:
            raise RuntimeError("TWITTER_X_ACTOR_ID is not configured")
        client = self._client()
        pull_limit = max(limit, min(100, limit * 3))
        run_input = self._actor_input(query, pull_limit)
        actor = client.actor(actor_id)
        run = actor.call(run_input=run_input, timeout_secs=self.timeout_seconds)
        dataset_id = (run or {}).get("defaultDatasetId") or (run or {}).get("default_dataset_id")
        if not dataset_id:
            return []
        dataset_client = client.dataset(dataset_id)
        rows = dataset_client.list_items(limit=pull_limit).items
        return [row for row in rows if isinstance(row, dict)]

    def _extract_author(self, row: Dict[str, Any]) -> tuple[str, str]:
        user = row.get("user") or row.get("author") or row.get("owner") or {}
        author = (
            row.get("authorName")
            or row.get("author_name")
            or row.get("name")
            or _nested_get(user, "name")
            or _nested_get(user, "displayName")
            or _nested_get(user, "display_name")
            or ""
        )
        handle = (
            row.get("handle")
            or row.get("screenName")
            or row.get("screen_name")
            or row.get("username")
            or _nested_get(user, "screenName")
            or _nested_get(user, "screen_name")
            or _nested_get(user, "username")
            or ""
        )
        author_text = _safe_text(author)
        handle_text = _safe_text(handle)
        if handle_text and not handle_text.startswith("@"):
            handle_text = f"@{handle_text}"
        return author_text, handle_text

    def _engagement_metrics(self, row: Dict[str, Any]) -> Dict[str, int]:
        likes = _to_int(_first_from_paths(row, [("likeCount",), ("likes",), ("favoriteCount",), ("favorite_count",), ("public_metrics", "like_count"), ("metrics", "likes")]))
        replies = _to_int(_first_from_paths(row, [("replyCount",), ("replies",), ("reply_count",), ("public_metrics", "reply_count"), ("metrics", "replies")]))
        reposts = _to_int(_first_from_paths(row, [("retweetCount",), ("retweets",), ("retweet_count",), ("repostCount",), ("public_metrics", "retweet_count"), ("metrics", "retweets")]))
        quotes = _to_int(_first_from_paths(row, [("quoteCount",), ("quotes",), ("quote_count",), ("public_metrics", "quote_count"), ("metrics", "quotes")]))
        views = _to_int(_first_from_paths(row, [("viewCount",), ("views",), ("view_count",), ("impressionCount",), ("public_metrics", "impression_count"), ("metrics", "views")]))
        bookmarks = _to_int(_first_from_paths(row, [("bookmarkCount",), ("bookmarks",), ("bookmark_count",), ("public_metrics", "bookmark_count"), ("metrics", "bookmarks")]))
        weighted = likes + (2 * replies) + (3 * reposts) + (2 * quotes) + (2 * bookmarks) + min(views // 100, 500)
        raw = likes + replies + reposts + quotes + bookmarks
        return {
            "likes": likes,
            "replies": replies,
            "reposts": reposts,
            "quotes": quotes,
            "views": views,
            "bookmarks": bookmarks,
            "engagement_total": raw,
            "weighted_engagement": weighted,
        }

    def _quality_score(self, text: str, tags: List[str], metrics: Dict[str, int], query: str) -> float:
        lower = text.lower()
        score = float(metrics.get("weighted_engagement") or 0)
        if metrics.get("engagement_total", 0) > 0:
            score += math.log1p(metrics["engagement_total"]) * 8
        if any(term in lower for term in QUALITY_TERMS):
            score += 25
        if any(term in lower for term in BETTING_TERMS):
            score += 12
        if set(tags).intersection({"injury", "lineup", "starter", "weather", "odds", "betting", "scratch"}):
            score += 20
        for token in query.lower().split():
            if len(token) >= 4 and token in lower:
                score += 2
        if any(term in lower for term in CLICKBAIT_TERMS):
            score -= 35
        if len(text) < 20:
            score -= 20
        return round(score, 3)

    def _detect_teams(self, text: str, preferred: Optional[List[str]] = None) -> List[str]:
        found: List[str] = []
        for team in preferred or []:
            if team and team not in found:
                found.append(team)
        lower = text.lower()
        for alias, row in team_lookup().items():
            if len(alias) < 3:
                continue
            if alias in lower and row["team"] not in found:
                found.append(row["team"])
        return found

    def _normalize(self, row: Dict[str, Any], query: str, bucket: str = "beat", teams: Optional[List[str]] = None, games: Optional[List[str]] = None) -> Dict[str, Any]:
        text = _safe_text(
            row.get("fullText")
            or row.get("full_text")
            or row.get("tweetText")
            or row.get("text")
            or row.get("content")
            or row.get("body")
            or row.get("description")
        )
        tweet_id = _safe_text(row.get("id") or row.get("id_str") or row.get("tweetId") or row.get("tweet_id") or row.get("rest_id"))
        author, handle = self._extract_author(row)
        url = _safe_text(row.get("url") or row.get("tweetUrl") or row.get("tweet_url") or row.get("link"))
        username = handle[1:] if handle.startswith("@") else handle
        if not url and tweet_id and username:
            url = f"https://x.com/{username}/status/{tweet_id}"
        published_at = _post_datetime(row.get("createdAt") or row.get("created_at") or row.get("date") or row.get("timestamp"))
        tags = classify_text(text[:140], text, "X/Twitter")
        metrics = self._engagement_metrics(row)
        is_betting = bool(set(tags).intersection({"odds", "betting"})) or any(term in text.lower() for term in BETTING_TERMS)
        quality_score = self._quality_score(text, tags, metrics, query)
        item = {
            "id": stable_id(tweet_id, url, text, query),
            "title": text[:140] or "X/Twitter post",
            "summary": text,
            "source": "X/Twitter",
            "source_type": "x",
            "author": author,
            "handle": handle,
            "url": url,
            "published_at": published_at,
            "bucket": "betting" if is_betting else bucket,
            "tags": tags,
            "teams": self._detect_teams(text, teams),
            "players": [],
            "games": games or [],
            "importance_score": 0.0,
            "twitter_quality_score": quality_score,
            "engagement": metrics,
            "is_breaking": False,
            "is_local": True,
            "is_beat_report": True,
            "is_betting_relevant": is_betting,
            "confidence_score": 0.55,
            "raw": {"query": query, "tweet_id": tweet_id, "provider": "apify"},
        }
        item["importance_score"] = score_importance(item, today_terms=[query, *BETTING_TERMS])
        item["is_breaking"] = item["importance_score"] >= 70 or bool(set(tags).intersection({"injury", "lineup", "starter", "weather", "scratch"}))
        return item

    def _dedupe(self, items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out: List[Dict[str, Any]] = []
        for item in items:
            key = item.get("url") or item.get("id") or hashlib.sha1(str(item.get("summary") or "").encode("utf-8")).hexdigest()[:16]
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    def _rank_items(self, items: Iterable[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        deduped = self._dedupe(items)
        filtered = [item for item in deduped if (item.get("engagement") or {}).get("engagement_total", 0) >= self.min_engagement]
        if not filtered and deduped:
            filtered = deduped
        return sorted(
            filtered,
            key=lambda row: (
                row.get("twitter_quality_score") or 0,
                (row.get("engagement") or {}).get("weighted_engagement") or 0,
                row.get("importance_score") or 0,
                row.get("published_at") or "",
            ),
            reverse=True,
        )[:limit]

    def search(self, query: str, limit: int = 50, date: Optional[str] = None, bucket: str = "beat", teams: Optional[List[str]] = None, games: Optional[List[str]] = None) -> Dict[str, Any]:
        target_date = (date or dt.date.today().isoformat())[:10]
        safe_limit = max(1, min(50, int(limit or self.max_results)))
        query = _safe_text(query)
        if not self.available():
            return self._not_configured(target_date)
        if not query:
            return {"date": target_date, "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"), "provider": self.name, "status": "empty", "query": query, "items": [], "errors": []}
        try:
            rows = self._raw_search(query, safe_limit)
            items = [self._normalize(row, query=query, bucket=bucket, teams=teams, games=games) for row in rows]
            items = self._rank_items(items, safe_limit)
            return {
                "date": target_date,
                "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "provider": self.name,
                "status": "ok" if items else "empty",
                "query": query,
                "items": items,
                "errors": [],
                "ranking": "twitter_quality_score_desc",
            }
        except Exception as exc:
            self.last_error = str(exc)
            return {
                "date": target_date,
                "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "provider": self.name,
                "status": "provider_error",
                "query": query,
                "items": [],
                "errors": [str(exc)],
            }

    def team_queries(self, team: str) -> List[str]:
        wanted = str(team or "").upper()
        row = next((src for src in get_news_sources(wanted) if src.get("team") == wanted), None)
        if not row:
            row = next((src for src in get_news_sources() if wanted in {src.get("team"), str(src.get("team_name", "")).upper()}), None)
        if not row:
            return [f"{team} lineup", f"{team} injury", f"{team} beat writer", f"{team} Statcast"]
        name = row.get("team_name") or wanted
        alias = (row.get("aliases") or [name])[0]
        return [
            f"{alias} lineup",
            f"{alias} injury",
            f"{alias} probable starter",
            f"{alias} beat writer",
            f"{alias} Statcast",
            f"{alias} pregame",
        ]

    def search_team(self, team: str, date: str, limit: int = 50) -> Dict[str, Any]:
        safe_limit = max(1, min(50, int(limit or self.max_results)))
        queries = self.team_queries(team)[: self.max_queries_per_matchup]
        items: List[Dict[str, Any]] = []
        errors: List[str] = []
        for query in queries:
            result = self.search(query, limit=safe_limit, date=date, teams=[str(team).upper()])
            items.extend(result.get("items") or [])
            errors.extend(result.get("errors") or [])
            if result.get("status") == "provider_not_configured":
                return result
        items = self._rank_items(items, safe_limit)
        return {"date": date[:10], "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"), "provider": self.name, "status": "ok" if items else "empty", "team": team, "queries": queries, "items": items, "errors": errors[:3], "ranking": "twitter_quality_score_desc"}

    def _matchup_queries(self, matchup: Dict[str, Any]) -> List[str]:
        away = _first_present(matchup, ["away_team_name", "away_team", "away_name", "away"]) or "Away"
        home = _first_present(matchup, ["home_team_name", "home_team", "home_name", "home"]) or "Home"
        away_pitcher = _first_present(matchup, ["away_pitcher_name", "away_probable_pitcher", "away_pitcher"])
        home_pitcher = _first_present(matchup, ["home_pitcher_name", "home_probable_pitcher", "home_pitcher"])
        short_away = away.split()[-1]
        short_home = home.split()[-1]
        queries = [
            f"{short_away} {short_home} lineup injury starter",
            f"{short_away} {short_home} bullpen weather odds",
            f"{short_away} {short_home} Statcast",
        ]
        if away_pitcher:
            queries.append(f"{away_pitcher} {short_home}")
        if home_pitcher:
            queries.append(f"{home_pitcher} {short_away}")
        return list(dict.fromkeys(q for q in queries if q and "Away Home" not in q))[: self.max_queries_per_matchup]

    def search_matchups(self, date: str, limit: int = 20) -> Dict[str, Any]:
        target_date = date[:10]
        cap = max(1, min(self.matchup_cap, int(limit or self.matchup_cap)))
        if not self.available():
            return self._not_configured(target_date, items_key="matchups")
        matchup_rows, matchup_source = _load_matchups(target_date)
        blocks: List[Dict[str, Any]] = []
        for matchup in matchup_rows:
            game_id = _first_present(matchup, ["game_id", "gamePk", "game_pk", "id"])
            away = _first_present(matchup, ["away_team_name", "away_team", "away_name", "away"])
            home = _first_present(matchup, ["home_team_name", "home_team", "home_name", "home"])
            away_pitcher = _first_present(matchup, ["away_pitcher_name", "away_probable_pitcher", "away_pitcher"])
            home_pitcher = _first_present(matchup, ["home_pitcher_name", "home_probable_pitcher", "home_pitcher"])
            queries = self._matchup_queries(matchup)
            items: List[Dict[str, Any]] = []
            errors: List[str] = []
            teams = [str(away).upper(), str(home).upper()]
            games = [game_id] if game_id else []
            for query in queries:
                result = self.search(query, limit=cap, date=target_date, teams=teams, games=games)
                items.extend(result.get("items") or [])
                errors.extend(result.get("errors") or [])
            items = self._rank_items(items, cap)
            blocks.append({
                "game_id": game_id or stable_id(target_date, away, home),
                "away_team": away,
                "home_team": home,
                "away_pitcher": away_pitcher,
                "home_pitcher": home_pitcher,
                "queries": queries,
                "items": items,
                "errors": errors[:3],
                "matchup_source": matchup_source,
                "tweet_cap": cap,
                "ranking": "twitter_quality_score_desc",
            })
        return {
            "date": target_date,
            "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "provider": self.name,
            "status": "ok" if blocks else "empty",
            "matchups": blocks,
            "errors": [],
            "matchup_count": len(blocks),
            "matchup_source": matchup_source,
            "tweet_cap_per_game": cap,
            "ranking": "twitter_quality_score_desc",
            "message": "No matchup rows loaded from generator or MLB schedule fallback." if not blocks else None,
        }

    def search_betting(self, date: str, limit: int = 50) -> Dict[str, Any]:
        target_date = date[:10]
        safe_limit = max(1, min(50, int(limit or self.max_results)))
        queries = [
            "MLB odds steam line movement",
            "MLB prop injury lineup scratch odds",
            "MLB sharp money baseball",
        ][: self.max_queries_per_matchup]
        items: List[Dict[str, Any]] = []
        errors: List[str] = []
        for query in queries:
            result = self.search(query, limit=safe_limit, date=target_date, bucket="betting")
            items.extend(result.get("items") or [])
            errors.extend(result.get("errors") or [])
            if result.get("status") == "provider_not_configured":
                return result
        items = self._rank_items(items, safe_limit)
        return {"date": target_date, "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"), "provider": self.name, "status": "ok" if items else "empty", "queries": queries, "items": items, "errors": errors[:3], "ranking": "twitter_quality_score_desc"}

    def baseball_feed_queries(self) -> List[str]:
        raw = os.getenv("TWITTER_X_BASEBALL_FEED_QUERIES", "").strip()
        if raw:
            queries = [part.strip() for part in raw.split(",") if part.strip()]
            if queries:
                return queries[:25]
        return DEFAULT_BASEBALL_FEED_QUERIES

    def search_baseball_feed(self, date: str, limit: int = 50) -> Dict[str, Any]:
        target_date = date[:10]
        safe_limit = max(1, min(self.general_cap, int(limit or self.general_cap)))
        if not self.available():
            return self._not_configured(target_date)
        queries = self.baseball_feed_queries()
        items: List[Dict[str, Any]] = []
        errors: List[str] = []
        per_query_limit = max(10, min(25, safe_limit))
        for query in queries:
            result = self.search(query, limit=per_query_limit, date=target_date, bucket="baseball_feed")
            items.extend(result.get("items") or [])
            errors.extend(result.get("errors") or [])
        items = self._rank_items(items, safe_limit)
        return {
            "date": target_date,
            "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "provider": self.name,
            "status": "ok" if items else "empty",
            "queries": queries,
            "items": items,
            "errors": errors[:5],
            "ranking": "twitter_quality_score_desc",
            "tweet_cap": safe_limit,
        }
