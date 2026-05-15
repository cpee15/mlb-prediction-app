from __future__ import annotations

import datetime as dt
import os
from typing import Optional

from fastapi import APIRouter, Query

from .news_cache import get_cache, set_cache, ttl_for
from .news_provider import BUCKETS, build_news_payload, build_team_intel, empty_response, get_provider
from .news_sources import get_global_rss_sources, get_news_sources
from .news_twitter_provider import TwikitNewsProvider

router = APIRouter()


def _date(value: Optional[str]) -> str:
    return (value or dt.date.today().isoformat())[:10]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _cached(key: str, ttl_key: str, builder, ttl_override: Optional[int] = None):
    cached = get_cache(key)
    if cached is not None:
        if isinstance(cached, dict):
            out = dict(cached)
            out["cache_hit"] = True
            return out
        return cached
    data = builder()
    ttl = ttl_override if ttl_override is not None else int(os.getenv("NEWS_CACHE_TTL_SECONDS", str(ttl_for(ttl_key))))
    set_cache(key, data, ttl)
    return data


def _twitter_provider() -> TwikitNewsProvider:
    return TwikitNewsProvider()


def _twitter_ttl() -> int:
    try:
        return int(os.getenv("NEWS_TWITTER_CACHE_TTL_SECONDS", "120"))
    except Exception:
        return 120


def _limit(value: int = 10) -> int:
    try:
        return max(1, min(25, int(value)))
    except Exception:
        return 10


@router.get("/news/health")
def news_health():
    provider = get_provider()
    rss_enabled = os.getenv("NEWS_PROVIDER", "").strip().lower() in {"rss", "rss_network", "rss_feed_network"} or _env_bool("NEWS_ENABLE_RSS_PROVIDER")
    twikit = _twitter_provider()
    return {
        "component": "news_terminal",
        "status": "ok",
        "provider_configured": provider.configured(),
        "active_provider": provider.name,
        "rss_enabled": rss_enabled,
        "rss_supported": True,
        **twikit.health(),
        "requires_api_key": bool(getattr(provider, "requires_api_key", False)),
        "env_vars_supported": [
            "NEWS_PROVIDER",
            "NEWS_ENABLE_RSS_PROVIDER",
            "NEWS_CACHE_TTL_SECONDS",
            "NEWS_MAX_ITEMS_PER_BUCKET",
            "NEWS_DEFAULT_LOOKBACK_HOURS",
            "NEWS_API_KEY",
            "NEWS_ENABLE_X_PROVIDER",
            "X_BEARER_TOKEN",
            "NEWS_DEFAULT_QUERY",
            "RAPIDAPI_KEY",
            "RAPIDAPI_NEWS_HOST",
            "X_API_KEY",
            "X_API_SECRET",
            "X_ACCESS_TOKEN",
            "X_ACCESS_TOKEN_SECRET",
            "NEWS_ENABLE_BETTING_WIRE",
            "NEWS_ENABLE_LOCAL_SOURCES",
            "NEWS_ENABLE_TWIKIT",
            "TWIKIT_USERNAME",
            "TWIKIT_EMAIL",
            "TWIKIT_PASSWORD",
            "TWIKIT_COOKIES_FILE",
            "TWIKIT_LANGUAGE",
            "TWIKIT_MAX_RESULTS",
            "TWIKIT_TIMEOUT_SECONDS",
            "TWIKIT_RATE_LIMIT_SLEEP_SECONDS",
            "NEWS_TWITTER_CACHE_TTL_SECONDS",
        ],
    }


@router.get("/news")
def news_bucket(bucket: str = Query("daily"), date: Optional[str] = None, team: Optional[str] = None):
    target_date = _date(date)
    if bucket not in BUCKETS:
        return empty_response(target_date, "news_terminal", "provider_error", [f"Unsupported bucket: {bucket}"])
    payload = _cached(f"news:{bucket}:{target_date}:{team or 'all'}", bucket, lambda: build_news_payload(target_date, bucket=bucket, team=team))
    slim = dict(payload)
    slim["buckets"] = {name: (payload.get("buckets") or {}).get(name, []) if name == bucket else [] for name in BUCKETS}
    return slim


@router.get("/news/all")
def news_all(date: Optional[str] = None):
    target_date = _date(date)
    return _cached(f"news:all:{target_date}", "all", lambda: build_news_payload(target_date))


@router.get("/news/ticker")
def news_ticker(date: Optional[str] = None):
    target_date = _date(date)
    payload = _cached(f"news:ticker:{target_date}", "ticker", lambda: build_news_payload(target_date))
    return {
        "date": target_date,
        "generated_at": payload.get("generated_at"),
        "provider": payload.get("provider"),
        "status": payload.get("status"),
        "ticker": payload.get("ticker") or [],
        "errors": payload.get("errors") or [],
        "cache_hit": payload.get("cache_hit", False),
    }


@router.get("/news/beat")
def news_beat(team: Optional[str] = None, date: Optional[str] = None):
    target_date = _date(date)
    payload = _cached(f"news:beat:{target_date}:{team or 'all'}", "beat", lambda: build_news_payload(target_date, bucket="beat", team=team))
    return {
        "date": target_date,
        "generated_at": payload.get("generated_at"),
        "provider": payload.get("provider"),
        "status": payload.get("status"),
        "items": (payload.get("buckets") or {}).get("beat", []),
        "errors": payload.get("errors") or [],
        "cache_hit": payload.get("cache_hit", False),
    }


@router.get("/news/betting")
def news_betting(date: Optional[str] = None):
    target_date = _date(date)
    payload = _cached(f"news:betting:{target_date}", "betting", lambda: build_news_payload(target_date, bucket="betting"))
    return {
        "date": target_date,
        "generated_at": payload.get("generated_at"),
        "provider": payload.get("provider"),
        "status": payload.get("status"),
        "items": (payload.get("buckets") or {}).get("betting", []),
        "errors": payload.get("errors") or [],
        "cache_hit": payload.get("cache_hit", False),
    }


@router.get("/news/team-intel")
def news_team_intel(team: Optional[str] = None, date: Optional[str] = None):
    target_date = _date(date)
    payload = _cached(f"news:team-intel:{target_date}:{team or 'all'}", "team_intel", lambda: build_news_payload(target_date, team=team))
    return {
        "date": target_date,
        "generated_at": payload.get("generated_at"),
        "provider": payload.get("provider"),
        "status": payload.get("status"),
        "team_intel": build_team_intel(target_date, team=team, payload=payload),
        "errors": payload.get("errors") or [],
        "cache_hit": payload.get("cache_hit", False),
    }


@router.get("/news/twitter/search")
def news_twitter_search(query: str = Query("MLB lineup"), date: Optional[str] = None, limit: int = Query(10)):
    target_date = _date(date)
    safe_limit = _limit(limit)
    safe_query = str(query or "").strip()
    return _cached(
        f"twitter:search:{safe_query}:{target_date}:{safe_limit}",
        "twitter",
        lambda: _twitter_provider().search(safe_query, limit=safe_limit, date=target_date),
        ttl_override=_twitter_ttl(),
    )


@router.get("/news/twitter/team")
def news_twitter_team(team: str = Query(...), date: Optional[str] = None, limit: int = Query(10)):
    target_date = _date(date)
    safe_limit = _limit(limit)
    safe_team = str(team or "").strip().upper()
    return _cached(
        f"twitter:team:{safe_team}:{target_date}:{safe_limit}",
        "twitter",
        lambda: _twitter_provider().search_team(safe_team, target_date, limit=safe_limit),
        ttl_override=_twitter_ttl(),
    )


@router.get("/news/twitter/matchups")
def news_twitter_matchups(date: Optional[str] = None, limit: int = Query(10)):
    target_date = _date(date)
    safe_limit = _limit(limit)
    return _cached(
        f"twitter:matchups:{target_date}:{safe_limit}",
        "twitter",
        lambda: _twitter_provider().search_matchups(target_date, limit=safe_limit),
        ttl_override=_twitter_ttl(),
    )


@router.get("/news/twitter/betting")
def news_twitter_betting(date: Optional[str] = None, limit: int = Query(10)):
    target_date = _date(date)
    safe_limit = _limit(limit)
    return _cached(
        f"twitter:betting:{target_date}:{safe_limit}",
        "twitter",
        lambda: _twitter_provider().search_betting(target_date, limit=safe_limit),
        ttl_override=_twitter_ttl(),
    )


@router.get("/news/sources")
def news_sources(team: Optional[str] = None):
    return {
        "status": "ok",
        "team": team,
        "sources": get_news_sources(team),
        "rss_sources": get_global_rss_sources(),
    }
