from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
from typing import Any, Dict, Iterable, List, Optional

from .news_classifier import classify_text, score_importance
from .news_keywords import BETTING_TERMS
from .news_provider import stable_id, utc_now
from .news_sources import get_news_sources, team_lookup


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


def _load_matchups(date: str) -> List[Dict[str, Any]]:
    # Same safe read-only path used by the existing Twikit provider and news keyword layer.
    try:
        from .app import _get_session
        from .matchup_generator import generate_matchups_for_date

        Session = _get_session()
        with Session() as session:
            data = generate_matchups_for_date(session, date)
            return data if isinstance(data, list) else []
    except Exception:
        return []


class ApifyXNewsProvider:
    name = "apify"
    requires_api_key = True

    def __init__(self) -> None:
        self.timeout_seconds = _int_env("TWITTER_X_TIMEOUT_SECONDS", _int_env("TWIKIT_TIMEOUT_SECONDS", 20, 3, 60), 3, 120)
        self.max_results = _int_env("TWITTER_X_MAX_RESULTS", _int_env("TWIKIT_MAX_RESULTS", 10, 1, 25), 1, 50)
        self.max_queries_per_matchup = _int_env("TWITTER_X_MAX_QUERIES_PER_MATCHUP", _int_env("TWIKIT_MAX_QUERIES_PER_MATCHUP", 5, 1, 8), 1, 8)
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
        raw_sort = os.getenv("TWITTER_X_ACTOR_SORT", "Latest").strip()
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
        run_input = self._actor_input(query, limit)
        timeout_secs = self.timeout_seconds
        actor = client.actor(actor_id)
        run = actor.call(run_input=run_input, timeout_secs=timeout_secs)
        dataset_id = (run or {}).get("defaultDatasetId") or (run or {}).get("default_dataset_id")
        if not dataset_id:
            return []
        dataset_client = client.dataset(dataset_id)
        rows = dataset_client.list_items(limit=limit).items
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
        is_betting = bool(set(tags).intersection({"odds", "betting"})) or any(term in text.lower() for term in BETTING_TERMS)
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

    def search(self, query: str, limit: int = 10, date: Optional[str] = None, bucket: str = "beat", teams: Optional[List[str]] = None, games: Optional[List[str]] = None) -> Dict[str, Any]:
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
            items = self._dedupe(items)[:safe_limit]
            return {
                "date": target_date,
                "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "provider": self.name,
                "status": "ok" if items else "empty",
                "query": query,
                "items": items,
                "errors": [],
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
            return [f"{team} lineup", f"{team} injury", f"{team} beat writer"]
        name = row.get("team_name") or wanted
        alias = (row.get("aliases") or [name])[0]
        return [
            f"{alias} lineup",
            f"{alias} injury",
            f"{alias} probable starter",
            f"{alias} beat writer",
            f"{alias} pregame",
        ]

    def search_team(self, team: str, date: str, limit: int = 10) -> Dict[str, Any]:
        queries = self.team_queries(team)[: self.max_queries_per_matchup]
        items: List[Dict[str, Any]] = []
        errors: List[str] = []
        for query in queries:
            result = self.search(query, limit=limit, date=date, teams=[str(team).upper()])
            items.extend(result.get("items") or [])
            errors.extend(result.get("errors") or [])
            if result.get("status") == "provider_not_configured":
                return result
        items = sorted(self._dedupe(items), key=lambda row: (row.get("importance_score") or 0, row.get("published_at") or ""), reverse=True)[:limit]
        return {"date": date[:10], "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"), "provider": self.name, "status": "ok" if items else "empty", "team": team, "queries": queries, "items": items, "errors": errors[:3]}

    def _matchup_queries(self, matchup: Dict[str, Any]) -> List[str]:
        away = _first_present(matchup, ["away_team_name", "away_team", "away_name", "away"]) or "Away"
        home = _first_present(matchup, ["home_team_name", "home_team", "home_name", "home"]) or "Home"
        away_pitcher = _first_present(matchup, ["away_pitcher_name", "away_probable_pitcher", "away_pitcher"])
        home_pitcher = _first_present(matchup, ["home_pitcher_name", "home_probable_pitcher", "home_pitcher"])
        short_away = away.split()[-1]
        short_home = home.split()[-1]
        queries = [
            f"{short_away} {short_home} lineup",
            f"{short_away} {short_home} injury",
            f"{short_away} {short_home} bullpen",
            f"{short_away} {short_home} weather",
            f"{short_away} {short_home} odds",
        ]
        if away_pitcher:
            queries.append(f"{away_pitcher} {short_home}")
        if home_pitcher:
            queries.append(f"{home_pitcher} {short_away}")
        return list(dict.fromkeys(q for q in queries if q and "Away Home" not in q))[: self.max_queries_per_matchup]

    def search_matchups(self, date: str, limit: int = 10) -> Dict[str, Any]:
        target_date = date[:10]
        if not self.available():
            return self._not_configured(target_date, items_key="matchups")
        matchup_rows = _load_matchups(target_date)
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
                result = self.search(query, limit=limit, date=target_date, teams=teams, games=games)
                items.extend(result.get("items") or [])
                errors.extend(result.get("errors") or [])
            items = sorted(self._dedupe(items), key=lambda row: (row.get("importance_score") or 0, row.get("published_at") or ""), reverse=True)[:limit]
            blocks.append({
                "game_id": game_id or stable_id(target_date, away, home),
                "away_team": away,
                "home_team": home,
                "away_pitcher": away_pitcher,
                "home_pitcher": home_pitcher,
                "queries": queries,
                "items": items,
                "errors": errors[:3],
            })
        return {"date": target_date, "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"), "provider": self.name, "status": "ok" if blocks else "empty", "matchups": blocks, "errors": []}

    def search_betting(self, date: str, limit: int = 10) -> Dict[str, Any]:
        target_date = date[:10]
        queries = [
            "MLB odds steam",
            "MLB line movement",
            "MLB prop injury",
            "MLB lineup scratch odds",
            "MLB sharp money",
        ][: self.max_queries_per_matchup]
        items: List[Dict[str, Any]] = []
        errors: List[str] = []
        for query in queries:
            result = self.search(query, limit=limit, date=target_date, bucket="betting")
            items.extend(result.get("items") or [])
            errors.extend(result.get("errors") or [])
            if result.get("status") == "provider_not_configured":
                return result
        items = sorted(self._dedupe(items), key=lambda row: (row.get("importance_score") or 0, row.get("published_at") or ""), reverse=True)[:limit]
        return {"date": target_date, "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"), "provider": self.name, "status": "ok" if items else "empty", "queries": queries, "items": items, "errors": errors[:3]}
