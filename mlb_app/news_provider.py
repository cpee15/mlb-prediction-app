from __future__ import annotations

import datetime as dt
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
import re
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional

import feedparser

from .news_classifier import classify_text, parse_datetime, score_importance
from .news_keywords import build_keyword_groups
from .news_sources import get_news_sources, get_rss_sources, team_lookup

BUCKETS = ["hourly", "daily", "weekly", "monthly", "beat", "betting"]


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_id(*parts: Any) -> str:
    raw = "|".join(str(p or "") for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def empty_response(date: str, provider: str, status: str, errors: Optional[List[str]] = None, cache_hit: bool = False) -> Dict[str, Any]:
    return {
        "date": date,
        "generated_at": iso_now(),
        "provider": provider,
        "status": status,
        "buckets": {bucket: [] for bucket in BUCKETS},
        "ticker": [],
        "team_intel": {},
        "sources": [],
        "errors": errors or [],
        "cache_hit": cache_hit,
    }


def normalize_item(raw: Dict[str, Any], bucket: str = "daily", source_type: str = "national") -> Dict[str, Any]:
    title = raw.get("title") or raw.get("headline") or "Untitled news item"
    summary = raw.get("description") or raw.get("summary") or raw.get("content") or ""
    source_obj = raw.get("source") if isinstance(raw.get("source"), dict) else {}
    source = raw.get("source_name") or source_obj.get("name") or raw.get("source") or "Unknown"
    url = raw.get("url") or raw.get("link") or ""
    published_at = raw.get("publishedAt") or raw.get("published_at") or raw.get("pubDate") or raw.get("updated_at")
    tags = classify_text(title, summary, str(source))
    is_betting = bool(set(tags).intersection({"odds", "betting"}))
    is_local = source_type in {"local", "beat", "radio", "newspaper", "official", "blog", "x", "rss", "google_news"}
    return {
        "id": stable_id(url, title, published_at),
        "title": title,
        "summary": summary,
        "source": source,
        "source_type": source_type,
        "author": raw.get("author"),
        "handle": raw.get("handle"),
        "url": url,
        "published_at": published_at,
        "bucket": bucket,
        "tags": tags,
        "teams": raw.get("teams") or [],
        "players": raw.get("players") or [],
        "games": raw.get("games") or [],
        "importance_score": 0.0,
        "is_breaking": False,
        "is_local": is_local,
        "is_beat_report": source_type in {"beat", "x"},
        "is_betting_relevant": is_betting,
        "confidence_score": 0.65 if url else 0.35,
        "raw": raw,
    }


def _normalized_title(title: str) -> str:
    value = re.sub(r"\s+", " ", str(title or "").strip().lower())
    value = re.sub(r"[^a-z0-9\s]", "", value)
    return value[:180]


def _canonical_url(url: str) -> str:
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    if "news.google." in parsed.netloc.lower():
        return ""
    clean = parsed._replace(query="", fragment="")
    return urllib.parse.urlunparse(clean).rstrip("/").lower()


def _dedupe(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for item in items:
        url_key = _canonical_url(str(item.get("url") or ""))
        title_key = _normalized_title(str(item.get("title") or ""))
        key = url_key or title_key
        if not key or key in seen:
            continue
        seen.add(key)
        if title_key:
            seen.add(title_key)
        out.append(item)
    return out


def _bucket_for(item: Dict[str, Any], now: dt.datetime) -> str:
    published = parse_datetime(item.get("published_at"))
    if not published:
        return item.get("bucket") or "daily"
    hours = max((now - published).total_seconds() / 3600.0, 0.0)
    if item.get("is_betting_relevant") and hours <= 24:
        return "betting"
    if item.get("is_local") and hours <= 72:
        return "beat"
    if hours <= 6:
        return "hourly"
    if hours <= 24:
        return "daily"
    if hours <= 168:
        return "weekly"
    return "monthly"


def assign_buckets(items: List[Dict[str, Any]], date: str) -> Dict[str, Any]:
    now = utc_now()
    terms = build_keyword_groups(date).get("all", [])
    max_items = int(os.getenv("NEWS_MAX_ITEMS_PER_BUCKET", "40"))
    buckets = {bucket: [] for bucket in BUCKETS}
    for item in _dedupe(items):
        bucket = _bucket_for(item, now)
        item["bucket"] = bucket if bucket in buckets else "daily"
        item["importance_score"] = score_importance(item, today_terms=terms)
        item["is_breaking"] = item["importance_score"] >= 70 or bool(set(item.get("tags", [])).intersection({"injury", "lineup", "starter", "weather"}))
        buckets[item["bucket"]].append(item)
    for bucket in buckets:
        buckets[bucket].sort(key=lambda row: (row.get("importance_score") or 0, row.get("published_at") or ""), reverse=True)
        buckets[bucket] = buckets[bucket][:max_items]
    return buckets


def build_ticker(buckets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rows = []
    for bucket in ["hourly", "beat", "betting", "daily"]:
        rows.extend(buckets.get(bucket, []))
    rows.sort(key=lambda row: row.get("importance_score") or 0, reverse=True)
    ticker = []
    for item in _dedupe(rows)[:25]:
        tag = (item.get("tags") or ["news"])[0]
        published = parse_datetime(item.get("published_at"))
        time_ago = "time pending"
        if published:
            minutes = int(max((utc_now() - published).total_seconds() / 60.0, 0))
            time_ago = f"{minutes}m ago" if minutes < 60 else f"{minutes // 60}h ago"
        ticker.append({
            "id": item.get("id"),
            "headline": item.get("title", "")[:140],
            "source": item.get("source"),
            "source_type": item.get("source_type"),
            "published_at": item.get("published_at"),
            "time_ago": time_ago,
            "tag": tag,
            "team": (item.get("teams") or [None])[0],
            "player": (item.get("players") or [None])[0],
            "url": item.get("url"),
            "importance_score": item.get("importance_score", 0.0),
        })
    return ticker


class BaseNewsProvider:
    name = "base"
    requires_api_key = False

    def configured(self) -> bool:
        return False

    def fetch(self, date: str, bucket: str = "all", team: Optional[str] = None) -> List[Dict[str, Any]]:
        return []


class RSSProvider(BaseNewsProvider):
    name = "rss"
    requires_api_key = False

    def __init__(self) -> None:
        self.last_errors: List[str] = []

    def configured(self) -> bool:
        selected = os.getenv("NEWS_PROVIDER", "").strip().lower()
        return selected in {"rss", "rss_network", "rss_feed_network"} or _env_bool("NEWS_ENABLE_RSS_PROVIDER")

    def _sources_for(self, team: Optional[str]) -> List[Dict[str, Any]]:
        if team:
            return get_rss_sources(team, include_team_google=True)
        return get_rss_sources(None, include_team_google=False)

    def _entry_datetime(self, entry: Any) -> Optional[str]:
        raw = (
            getattr(entry, "published", None)
            or getattr(entry, "updated", None)
            or entry.get("published")
            or entry.get("updated")
        )
        if raw:
            parsed = parse_datetime(raw)
            if parsed:
                return parsed.isoformat().replace("+00:00", "Z")
            try:
                email_parsed = parsedate_to_datetime(str(raw))
                if email_parsed.tzinfo is None:
                    email_parsed = email_parsed.replace(tzinfo=dt.timezone.utc)
                return email_parsed.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                return str(raw)
        return None

    def _in_window(self, published_at: Optional[str], date: str) -> bool:
        parsed = parse_datetime(published_at)
        if not parsed:
            return True
        lookback_hours = int(os.getenv("NEWS_DEFAULT_LOOKBACK_HOURS", "48"))
        try:
            target = dt.datetime.fromisoformat(str(date)[:10]).replace(tzinfo=dt.timezone.utc)
        except Exception:
            target = utc_now()
        start = target - dt.timedelta(hours=lookback_hours)
        end = target + dt.timedelta(hours=lookback_hours)
        return start <= parsed <= end

    def _detect_teams(self, title: str, summary: str, source_team: Optional[str]) -> List[str]:
        teams: List[str] = []
        if source_team:
            teams.append(str(source_team).upper())
        text = f"{title} {summary}".lower()
        for key, row in team_lookup().items():
            if len(key) < 3:
                continue
            if key in text and row["team"] not in teams:
                teams.append(row["team"])
        return teams

    def _confidence_for(self, source_type: str, published_at: Optional[str], url: str) -> float:
        if source_type == "official":
            base = 0.75
        elif source_type == "google_news":
            base = 0.6
        else:
            base = 0.65
        if not parse_datetime(published_at):
            base -= 0.1
        if not url:
            base -= 0.15
        return round(max(base, 0.25), 2)

    def _normalize_entry(self, entry: Any, source: Dict[str, Any], date: str) -> Optional[Dict[str, Any]]:
        title = str(entry.get("title") or "").strip()
        summary = str(entry.get("summary") or entry.get("description") or "").strip()
        url = str(entry.get("link") or "").strip()
        if not title and not url:
            return None

        source_name = source.get("name") or "RSS Feed"
        source_type = source.get("source_type") or "rss"
        published_at = self._entry_datetime(entry)
        if not self._in_window(published_at, date):
            return None

        teams = self._detect_teams(title, summary, source.get("team"))
        tags = classify_text(title, summary, source_name)
        is_betting = bool(set(tags).intersection({"odds", "betting"}))
        raw = {
            "feed": source_name,
            "source_type": source_type,
            "team": source.get("team"),
            "id": entry.get("id"),
        }
        return {
            "id": stable_id(url, title, published_at),
            "title": title or "Untitled RSS item",
            "summary": summary,
            "source": source_name,
            "source_type": source_type,
            "author": entry.get("author"),
            "handle": None,
            "url": url,
            "published_at": published_at,
            "bucket": "daily",
            "tags": tags,
            "teams": teams,
            "players": [],
            "games": [],
            "importance_score": 0.0,
            "is_breaking": False,
            "is_local": source_type in {"official", "local", "beat", "blog", "google_news"},
            "is_beat_report": source_type in {"beat", "x"},
            "is_betting_relevant": is_betting,
            "confidence_score": self._confidence_for(source_type, published_at, url),
            "raw": raw,
        }

    def fetch(self, date: str, bucket: str = "all", team: Optional[str] = None) -> List[Dict[str, Any]]:
        self.last_errors = []
        items: List[Dict[str, Any]] = []
        for source in self._sources_for(team):
            url = source.get("url")
            if not url:
                continue
            try:
                parsed = feedparser.parse(url)
                if getattr(parsed, "bozo", False):
                    self.last_errors.append(f"{source.get('name', url)} feed warning: {getattr(parsed, 'bozo_exception', 'parse warning')}")
                for entry in parsed.entries or []:
                    normalized = self._normalize_entry(entry, source, date)
                    if normalized:
                        items.append(normalized)
            except Exception as exc:
                self.last_errors.append(f"{source.get('name', url)} failed: {exc}")
                continue
        return _dedupe(items)


class NewsApiProvider(BaseNewsProvider):
    name = "newsapi"
    requires_api_key = True

    def configured(self) -> bool:
        return bool(os.getenv("NEWS_API_KEY"))

    def fetch(self, date: str, bucket: str = "all", team: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.configured():
            return []
        groups = build_keyword_groups(date)
        query = os.getenv("NEWS_DEFAULT_QUERY") or " OR ".join(groups.get("teams", [])[:8]) or "MLB baseball"
        if team:
            source = next(iter(get_news_sources(team)), None)
            if source:
                query = " OR ".join(source.get("queries", [])[:6])
        params = urllib.parse.urlencode({
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": os.getenv("NEWS_MAX_ITEMS_PER_BUCKET", "40"),
            "apiKey": os.getenv("NEWS_API_KEY"),
        })
        with urllib.request.urlopen(f"https://newsapi.org/v2/everything?{params}", timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return [normalize_item(row, source_type="national") for row in payload.get("articles", []) if isinstance(row, dict)]


class RapidApiNewsProvider(BaseNewsProvider):
    name = "rapidapi"
    requires_api_key = True

    def configured(self) -> bool:
        return bool(os.getenv("RAPIDAPI_KEY") and os.getenv("RAPIDAPI_NEWS_HOST"))


class XProvider(BaseNewsProvider):
    name = "x_official_api"
    requires_api_key = True

    def configured(self) -> bool:
        return os.getenv("NEWS_ENABLE_X_PROVIDER", "false").lower() == "true" and bool(os.getenv("X_BEARER_TOKEN"))


class ProviderNotConfigured(BaseNewsProvider):
    name = "provider_not_configured"
    requires_api_key = False


def get_provider() -> BaseNewsProvider:
    selected = os.getenv("NEWS_PROVIDER", "").strip().lower()
    if selected in {"rss", "rss_network", "rss_feed_network"}:
        return RSSProvider()
    if selected in {"newsapi", "news_api"}:
        return NewsApiProvider()
    if selected == "rapidapi":
        return RapidApiNewsProvider()
    if selected in {"x", "twitter"}:
        return XProvider()
    if _env_bool("NEWS_ENABLE_RSS_PROVIDER") and not os.getenv("NEWS_API_KEY"):
        return RSSProvider()
    if os.getenv("NEWS_API_KEY"):
        return NewsApiProvider()
    return ProviderNotConfigured()


def build_news_payload(date: str, bucket: str = "all", team: Optional[str] = None) -> Dict[str, Any]:
    provider = get_provider()
    if not provider.configured():
        payload = empty_response(
            date,
            provider.name,
            "provider_not_configured",
            ["Missing news provider config: set NEWS_PROVIDER=rss or NEWS_ENABLE_RSS_PROVIDER=true for free RSS, or configure a paid provider key."],
        )
        payload["sources"] = get_news_sources(team)
        return payload
    try:
        items = provider.fetch(date, bucket=bucket, team=team)
        buckets = assign_buckets(items, date)
        provider_errors = list(getattr(provider, "last_errors", []) or [])
        payload = empty_response(date, provider.name, "ok" if items else "empty", provider_errors)
        payload["buckets"] = buckets
        payload["ticker"] = build_ticker(buckets)
        payload["sources"] = get_news_sources(team)
        return payload
    except Exception as exc:
        payload = empty_response(date, provider.name, "provider_error", [str(exc)])
        payload["sources"] = get_news_sources(team)
        return payload


def build_team_intel(date: str, team: Optional[str] = None, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    sources = get_news_sources(team)
    payload = payload or build_news_payload(date, team=team)
    all_items = []
    for rows in (payload.get("buckets") or {}).values():
        all_items.extend(rows)
    boards: Dict[str, Any] = {}
    for source in sources:
        abbr = source["team"]
        aliases = [abbr, source["team_name"], *source.get("aliases", [])]
        team_items = []
        for item in all_items:
            text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
            if abbr in (item.get("teams") or []) or any(alias.lower() in text for alias in aliases):
                team_items.append(item)
        team_items.sort(key=lambda row: row.get("importance_score") or 0, reverse=True)
        tags = []
        for item in team_items:
            for tag in item.get("tags", []):
                if tag not in tags:
                    tags.append(tag)
        boards[abbr] = {
            "team": abbr,
            "team_name": source["team_name"],
            "latest_local_item": next((i for i in team_items if i.get("is_local")), None),
            "latest_injury_lineup_starter_item": next((i for i in team_items if set(i.get("tags", [])).intersection({"injury", "lineup", "starter"})), None),
            "latest_betting_relevant_item": next((i for i in team_items if i.get("is_betting_relevant")), None),
            "beat_report_count": sum(1 for i in team_items if i.get("is_beat_report") or i.get("is_local")),
            "national_headline_count": sum(1 for i in team_items if i.get("source_type") == "national"),
            "top_tags": tags[:8],
            "last_updated": payload.get("generated_at"),
            "local_source_count": len(source.get("beat_sources", [])) + len(source.get("local_outlets", [])),
            "confidence_provider_status": payload.get("status"),
        }
    return boards if team is None else boards.get(str(team).upper(), {})
