from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import inspect
import json
import os
import re
from pathlib import Path
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


def _run_maybe_await(value: Any, timeout_seconds: Optional[int] = None) -> Any:
    if not inspect.isawaitable(value):
        return value

    async def _await_with_timeout() -> Any:
        if timeout_seconds:
            return await asyncio.wait_for(value, timeout=timeout_seconds)
        return await value

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # FastAPI sync routes should not usually land here. Avoid nested loop crashes.
            return None
    except RuntimeError:
        loop = None
    return asyncio.run(_await_with_timeout())


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


def _first_present(row: Dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = row.get(key)
        if value:
            return str(value)
    return ""


def _load_matchups(date: str) -> List[Dict[str, Any]]:
    # Same safe access pattern as news_keywords.py. This reads the existing matchup generator without changing it.
    try:
        from .app import _get_session
        from .matchup_generator import generate_matchups_for_date

        Session = _get_session()
        with Session() as session:
            data = generate_matchups_for_date(session, date)
            return data if isinstance(data, list) else []
    except Exception:
        return []


class TwikitNewsProvider:
    name = "twikit"
    requires_api_key = True

    def __init__(self) -> None:
        self.language = os.getenv("TWIKIT_LANGUAGE", "en-US")
        self.timeout_seconds = _int_env("TWIKIT_TIMEOUT_SECONDS", 20, 3, 60)
        self.max_results = _int_env("TWIKIT_MAX_RESULTS", 10, 1, 25)
        self.max_queries_per_matchup = _int_env("TWIKIT_MAX_QUERIES_PER_MATCHUP", 5, 1, 8)
        self.last_error: Optional[str] = None

    @staticmethod
    def twikit_installed() -> bool:
        try:
            import twikit  # noqa: F401
            return True
        except Exception:
            return False

    def enabled(self) -> bool:
        selected = os.getenv("NEWS_PROVIDER", "").strip().lower()
        return _env_bool("NEWS_ENABLE_TWIKIT") or selected in {"twikit", "twitter"}

    def _cookie_file_from_json(self) -> str:
        cookies_json = os.getenv("TWIKIT_COOKIES_JSON", "").strip()
        if not cookies_json:
            return ""
        path = Path(os.getenv("TWIKIT_RUNTIME_COOKIES_FILE", "/tmp/twikit_cookies.json"))
        try:
            parsed = json.loads(cookies_json)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(parsed), encoding="utf-8")
            return str(path)
        except Exception as exc:
            self.last_error = f"Invalid TWIKIT_COOKIES_JSON: {exc}"
            return ""

    def _configured_cookie_file(self) -> str:
        cookies_file = os.getenv("TWIKIT_COOKIES_FILE", "").strip()
        if cookies_file and os.path.exists(cookies_file):
            return cookies_file
        return self._cookie_file_from_json()

    def configured(self) -> bool:
        if not self.enabled():
            return False
        has_cookies = bool(self._configured_cookie_file())
        has_login = bool(os.getenv("TWIKIT_USERNAME") and os.getenv("TWIKIT_PASSWORD"))
        return has_cookies or has_login

    def available(self) -> bool:
        return self.enabled() and self.configured() and self.twikit_installed()

    def health(self) -> Dict[str, Any]:
        return {
            "twikit_supported": True,
            "twikit_installed": self.twikit_installed(),
            "twikit_enabled": self.enabled(),
            "twikit_configured": self.configured(),
            "twikit_cookie_json_supported": True,
            "twikit_cookie_json_present": bool(os.getenv("TWIKIT_COOKIES_JSON", "").strip()),
        }

    def _not_configured(self, date: Optional[str] = None, items_key: str = "items") -> Dict[str, Any]:
        status = "provider_not_configured"
        if self.enabled() and self.configured() and not self.twikit_installed():
            status = "provider_missing_dependency"
        message = (
            "Twitter/X provider is disabled or missing credentials. Configure NEWS_ENABLE_TWIKIT and TWIKIT_COOKIES_JSON or TWIKIT_* login env vars to enable matchup intel."
            if status == "provider_not_configured"
            else "Twitter/X provider is configured, but the twikit dependency is not installed."
        )
        errors = [message]
        if self.last_error:
            errors.append(self.last_error)
        payload = {
            "date": (date or dt.date.today().isoformat())[:10],
            "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "provider": self.name,
            "status": status,
            "message": message,
            "errors": errors,
        }
        payload[items_key] = []
        return payload

    def _client(self) -> Any:
        try:
            from twikit import Client
        except Exception as exc:
            raise RuntimeError("twikit dependency is not installed") from exc

        client = Client(self.language)
        cookies_file = self._configured_cookie_file()
        if cookies_file and os.path.exists(cookies_file):
            loader = getattr(client, "load_cookies", None)
            if callable(loader):
                _run_maybe_await(loader(cookies_file), self.timeout_seconds)
                return client

        username = os.getenv("TWIKIT_USERNAME")
        email = os.getenv("TWIKIT_EMAIL")
        password = os.getenv("TWIKIT_PASSWORD")
        login = getattr(client, "login", None)
        if not callable(login) or not username or not password:
            return client

        try:
            _run_maybe_await(login(auth_info_1=username, auth_info_2=email or username, password=password), self.timeout_seconds)
        except TypeError:
            _run_maybe_await(login(username, email or username, password), self.timeout_seconds)

        cookie_save_path = os.getenv("TWIKIT_COOKIES_FILE") or os.getenv("TWIKIT_RUNTIME_COOKIES_FILE", "/tmp/twikit_cookies.json")
        saver = getattr(client, "save_cookies", None)
        if callable(saver) and cookie_save_path:
            try:
                _run_maybe_await(saver(cookie_save_path), self.timeout_seconds)
            except Exception:
                pass
        return client

    def _raw_search(self, query: str, limit: int) -> List[Any]:
        client = self._client()
        search = getattr(client, "search_tweet", None) or getattr(client, "search_tweets", None) or getattr(client, "search", None)
        if not callable(search):
            raise RuntimeError("Twikit client has no recognized search method")
        try:
            result = _run_maybe_await(search(query, "Latest", count=limit), self.timeout_seconds)
        except TypeError:
            try:
                result = _run_maybe_await(search(query, "Latest"), self.timeout_seconds)
            except TypeError:
                result = _run_maybe_await(search(query), self.timeout_seconds)
        if result is None:
            return []
        if isinstance(result, list):
            return result[:limit]
        if hasattr(result, "tweets"):
            tweets = getattr(result, "tweets") or []
            return list(tweets)[:limit]
        try:
            return list(result)[:limit]
        except Exception:
            return []

    def _tweet_attr(self, tweet: Any, *names: str) -> Any:
        if isinstance(tweet, dict):
            for name in names:
                if tweet.get(name) is not None:
                    return tweet.get(name)
            return None
        for name in names:
            if hasattr(tweet, name):
                value = getattr(tweet, name)
                if value is not None:
                    return value
        return None

    def _author_fields(self, tweet: Any) -> tuple[str, str]:
        user = self._tweet_attr(tweet, "user", "author")
        author = ""
        handle = ""
        if isinstance(user, dict):
            author = _safe_text(user.get("name") or user.get("display_name"))
            handle = _safe_text(user.get("screen_name") or user.get("username"))
        elif user is not None:
            author = _safe_text(getattr(user, "name", "") or getattr(user, "display_name", ""))
            handle = _safe_text(getattr(user, "screen_name", "") or getattr(user, "username", ""))
        if handle and not handle.startswith("@"):
            handle = f"@{handle}"
        return author, handle

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

    def _normalize(self, tweet: Any, query: str, bucket: str = "beat", teams: Optional[List[str]] = None, games: Optional[List[str]] = None) -> Dict[str, Any]:
        text = _safe_text(self._tweet_attr(tweet, "full_text", "text", "content"))
        tweet_id = _safe_text(self._tweet_attr(tweet, "id", "id_str", "rest_id"))
        author, handle = self._author_fields(tweet)
        username = handle[1:] if handle.startswith("@") else handle
        url = _safe_text(self._tweet_attr(tweet, "url", "tweet_url"))
        if not url and tweet_id and username:
            url = f"https://x.com/{username}/status/{tweet_id}"
        published_at = _post_datetime(self._tweet_attr(tweet, "created_at", "createdAt", "date"))
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
            "raw": {"query": query, "tweet_id": tweet_id},
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
        limit = max(1, min(25, int(limit or self.max_results)))
        query = _safe_text(query)
        if not self.available():
            return self._not_configured(target_date)
        if not query:
            return {"date": target_date, "generated_at": utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z"), "provider": self.name, "status": "empty", "query": query, "items": [], "errors": []}
        try:
            rows = self._raw_search(query, limit)
            items = [self._normalize(tweet, query=query, bucket=bucket, teams=teams, games=games) for tweet in rows]
            items = self._dedupe(items)[:limit]
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
